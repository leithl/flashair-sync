#!/usr/bin/env python3
"""flashair_rescue.py — re-download any FlashAir files whose local copy is
incomplete (size mismatch) or missing.

Context
-------
The normal flashair_sync flow uses a filename-based watermark (LAST_SYNCED).
Once a file has been downloaded once, the watermark advances past it and
subsequent polls skip it. That's wrong for two scenarios:

  1. The avionics is still writing the file when we poll, and we snapshot
     a partial. Later the same file on the SD card grows to its real size
     but we never re-pull it.
  2. The plane is away from home WiFi for days. On return, the watermark
     points at a now-stale "morning departure" CSV from the start of the
     trip. The sync pulls files NEWER than the watermark, but the
     departure file (== watermark) is skipped — even though it's now
     megabytes larger than the partial we have locally.

This rescue tool is independent of the watermark. It walks every CSV on
the FlashAir, compares the remote size to the local file, and re-downloads
anything that differs. It then SCPs changed/new files to the remote host
(same target as flashair_sync uses).

Designed to be safe to run repeatedly — it's a no-op when sizes already
match. Intended use is from the systemd service hook: run it once every
time FlashAir becomes reachable, as part of the same poll loop.

Usage
-----
    python3 flashair_rescue.py                  # rescue + SCP
    python3 flashair_rescue.py --no-scp         # just download locally
    python3 flashair_rescue.py --dry-run        # report only

Reads the same .env as flashair_sync.py.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
import urllib.request
from pathlib import Path

# Reuse all the existing primitives so this stays in lock-step with the
# main daemon (config, lock, FlashAir auth + reachability, SCP).
from flashair_sync import (
    Config,
    DOWNLOAD_TIMEOUT,
    FLASHAIR_HTTP_TIMEOUT,
    acquire_lock,
    connect_to_flashair,
    download_file,
    get_current_ssid,
    load_config,
    pending_scp_files,
    reconnect_home,
    release_lock,
    save_last_scpd,
    scp_files,
    wait_for_flashair,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("flashair_rescue")


def list_flashair_files_with_sizes(ip: str, directory: str) -> dict[str, int]:
    """Return {filename: size_bytes} for every log_*.csv on the SD card.

    The op=100 directory listing already returns sizes, but the existing
    list_flashair_files() throws them away. We need them here.
    """
    url = f"http://{ip}/command.cgi?op=100&DIR={directory}"
    with urllib.request.urlopen(url, timeout=FLASHAIR_HTTP_TIMEOUT) as resp:
        content = resp.read().decode("utf-8")
    out: dict[str, int] = {}
    for line in content.splitlines():
        if line.startswith("WLANSD_FILELIST"):
            continue
        parts = line.split(",")
        # Format: DIR,FILENAME,SIZE,ATTR,DATE,TIME
        if len(parts) < 3:
            continue
        fname = parts[1].strip()
        try:
            size = int(parts[2].strip())
        except ValueError:
            continue
        if fname.startswith("log_") and fname.lower().endswith(".csv"):
            out[fname] = size
    return out


def find_stale_or_missing(remote: dict[str, int], local_dir: Path) -> list[tuple[str, int, int]]:
    """For each remote file, decide if local needs an update.

    Returns list of (filename, local_size_or_-1, remote_size).
    Includes files where local is missing OR local size != remote size.
    Local size of -1 means 'no local copy yet'.
    """
    needs = []
    for fname, remote_size in remote.items():
        local_path = local_dir / fname
        if not local_path.exists():
            needs.append((fname, -1, remote_size))
            continue
        local_size = local_path.stat().st_size
        if local_size != remote_size:
            needs.append((fname, local_size, remote_size))
    return needs


def run_rescue(do_scp: bool, dry_run: bool) -> int:
    """Returns exit code (0 = success, non-zero = something went wrong)."""
    cfg = load_config()
    lock = acquire_lock()
    if lock is None:
        log.error("Could not acquire lock (another instance running). Exiting.")
        return 1
    home_ssid = get_current_ssid(cfg.interface)
    flashair_net_id: int | None = None
    try:
        flashair_net_id = connect_to_flashair(cfg)
        if not wait_for_flashair(cfg):
            log.error("FlashAir not reachable after connect. Bailing.")
            return 2

        remote = list_flashair_files_with_sizes(cfg.flashair_ip, cfg.flashair_directory)
        log.info(f"FlashAir reports {len(remote)} CSV file(s).")

        local_dir = Path(cfg.local_dir)
        local_dir.mkdir(parents=True, exist_ok=True)
        stale = find_stale_or_missing(remote, local_dir)

        if not stale:
            log.info("Everything in sync — no rescue needed.")
            return 0

        log.info(f"Found {len(stale)} file(s) needing rescue:")
        for fname, lsize, rsize in stale:
            tag = "MISSING" if lsize < 0 else f"size {lsize:,} -> {rsize:,}"
            log.info(f"  {fname}  ({tag})")
        if dry_run:
            log.info("--dry-run: not downloading.")
            return 0

        rescued: list[str] = []
        for fname, _, _ in stale:
            try:
                path = download_file(cfg.flashair_ip, cfg.flashair_directory,
                                     fname, cfg.local_dir)
                rescued.append(path)
            except Exception as e:
                log.warning(f"  download failed for {fname}: {e}")

        if not rescued:
            log.warning("No files actually rescued (all downloads failed).")
            return 3

        log.info(f"Rescued {len(rescued)} file(s) locally.")
    finally:
        # Always try to put the Pi back on home WiFi
        try:
            reconnect_home(cfg, flashair_net_id)
        except Exception as e:
            log.warning(f"reconnect_home failed: {e}")
        release_lock(lock)

    if do_scp:
        # SCP everything that's new since the SCP watermark, plus the
        # explicitly rescued files (which may be older than the watermark).
        from flashair_sync import load_last_scpd
        scp_watermark = load_last_scpd()
        candidates = pending_scp_files(cfg.local_dir, scp_watermark)
        # Also force-include any rescued file regardless of watermark
        candidate_set = {Path(p).name for p in candidates}
        for path in rescued:
            if Path(path).name not in candidate_set:
                candidates.append(path)
        if candidates:
            log.info(f"SCPing {len(candidates)} file(s) to remote...")
            n = scp_files(cfg, candidates)
            log.info(f"  SCP'd {n} file(s).")
            # Advance the SCP watermark to the newest local file
            newest = max(Path(p).name for p in candidates)
            if newest:
                save_last_scpd(newest)
        else:
            log.info("Nothing to SCP.")
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--no-scp", action="store_true",
                   help="Skip SCP step; only refresh local copies.")
    p.add_argument("--dry-run", action="store_true",
                   help="Report what would be done; don't download.")
    args = p.parse_args()
    return run_rescue(do_scp=not args.no_scp, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
