#!/usr/bin/env python3
"""flashair_sync.py — Sync engine monitor CSVs from a Toshiba FlashAir SD card.

Polls for the FlashAir WiFi network, connects when detected, downloads new
CSV files, reconnects to the home network, and SCPs files to a remote server.

Designed for Raspberry Pi (Linux / wpa_supplicant). For macOS testing, see
flashair_sync_macos.py.

Configuration (.env file in the same directory as this script):

    # Required
    FLASHAIR_SSID=FlashAir              WiFi network name of the FlashAir card
    FLASHAIR_PASSWORD=12345678          WiFi password for the FlashAir card
    FLASHAIR_DIR=/                      Directory on the card containing CSVs
    HOME_SSID=MyHomeWiFi                Home WiFi SSID to reconnect to after sync
    HOME_PASSWORD=secret                Home WiFi password (omit for open networks)
    LOCAL_CSV_DIR=/home/pi/csvs         Local directory to store downloaded CSVs
    REMOTE_HOST=192.168.1.100           Remote server hostname or IP for SCP
    REMOTE_USER=leith                   SSH user on the remote server
    REMOTE_DIR=/home/leith/savvy/csvs   Destination directory on the remote server

    # Optional
    FLASHAIR_IP=192.168.0.1             FlashAir IP (default: 192.168.0.1)
    SSH_KEY_PATH=~/.ssh/id_ed25519      SSH private key (default: ~/.ssh/id_ed25519)
    WIFI_INTERFACE=wlan0                WiFi interface (default: wlan0)
    COOLDOWN_MINUTES=30                 Minutes to wait before re-checking FlashAir (default: 30)

    # Managed by the script (do not edit)
    LAST_SYNCED=                        Watermark — filename of last downloaded CSV
    LAST_SCPD=                          Watermark — filename of last SCP'd CSV

Usage:
    python3 flashair_sync.py            # Normal sync (run via cron every minute)
    python3 flashair_sync.py --resync   # Re-download all files
    python3 flashair_sync.py -v         # Verbose logging
"""

import argparse
import fcntl
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

log = logging.getLogger("flashair_sync")

FLASHAIR_DEFAULT_IP = "192.168.0.1"
SCAN_WAIT_SECONDS = 5
CONNECT_TIMEOUT = 30
FLASHAIR_HTTP_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 120
COOLDOWN_MINUTES_DEFAULT = 30


# ---------------------------------------------------------------------------
# .env helpers
# ---------------------------------------------------------------------------

def _env_path() -> Path:
    return Path(__file__).resolve().parent / ".env"


def _read_env() -> dict[str, str]:
    """Read .env file as key=value pairs (no shell expansion)."""
    result: dict[str, str] = {}
    p = _env_path()
    if not p.exists():
        return result
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            result[k.strip()] = v.strip()
    return result


def _write_env(data: dict[str, str]) -> None:
    """Write the full .env dict back to disk, preserving comments and order."""
    lines: list[str] = []
    p = _env_path()
    written: set[str] = set()
    if p.exists():
        for line in p.read_text().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                k = stripped.split("=", 1)[0].strip()
                if k in data:
                    lines.append(f"{k}={data[k]}")
                    written.add(k)
                else:
                    lines.append(line)
            else:
                lines.append(line)
    for k, v in data.items():
        if k not in written:
            lines.append(f"{k}={v}")
    p.write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class Config:
    flashair_ssid: str
    flashair_password: str
    flashair_ip: str
    flashair_dir: str
    home_ssid: str
    home_password: str
    local_csv_dir: str
    remote_host: str
    remote_user: str
    remote_dir: str
    ssh_key_path: str = ""
    wifi_interface: str = ""

    def __post_init__(self):
        if not self.ssh_key_path:
            self.ssh_key_path = str(Path.home() / ".ssh" / "id_ed25519")
        if not self.wifi_interface:
            self.wifi_interface = "wlan0"


def load_config() -> Config:
    """Load configuration from .env file and environment variables."""
    env = _read_env()

    def _get(key: str, default: str = "") -> str:
        return os.environ.get(key, "") or env.get(key, "") or default

    cfg = Config(
        flashair_ssid=_get("FLASHAIR_SSID"),
        flashair_password=_get("FLASHAIR_PASSWORD"),
        flashair_ip=_get("FLASHAIR_IP", FLASHAIR_DEFAULT_IP),
        flashair_dir=_get("FLASHAIR_DIR"),
        home_ssid=_get("HOME_SSID"),
        home_password=_get("HOME_PASSWORD"),
        local_csv_dir=_get("LOCAL_CSV_DIR"),
        remote_host=_get("REMOTE_HOST"),
        remote_user=_get("REMOTE_USER"),
        remote_dir=_get("REMOTE_DIR"),
        ssh_key_path=_get("SSH_KEY_PATH"),
        wifi_interface=_get("WIFI_INTERFACE"),
    )

    missing = []
    for field_name in [
        "flashair_ssid", "flashair_password", "flashair_dir",
        "home_ssid", "local_csv_dir", "remote_host",
        "remote_user", "remote_dir",
    ]:
        if not getattr(cfg, field_name):
            missing.append(field_name.upper())
    if missing:
        log.error(f"Missing required config: {', '.join(missing)}")
        sys.exit(1)

    # Ensure local CSV directory exists
    Path(cfg.local_csv_dir).mkdir(parents=True, exist_ok=True)

    return cfg


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

def load_last_synced() -> str:
    return _read_env().get("LAST_SYNCED", "")


def save_last_synced(filename: str) -> None:
    env = _read_env()
    env["LAST_SYNCED"] = filename
    _write_env(env)
    log.info(f"Updated LAST_SYNCED={filename}")


# ---------------------------------------------------------------------------
# Cooldown (skip FlashAir scan after a recent successful download)
# ---------------------------------------------------------------------------

_COOLDOWN_PATH = Path(__file__).resolve().parent / ".last_sync"


def _cooldown_minutes() -> int:
    """Return the configured cooldown period in minutes."""
    val = os.environ.get("COOLDOWN_MINUTES", "") or _read_env().get("COOLDOWN_MINUTES", "")
    try:
        return int(val) if val else COOLDOWN_MINUTES_DEFAULT
    except ValueError:
        return COOLDOWN_MINUTES_DEFAULT


def _in_cooldown() -> bool:
    """Return True if a successful download happened within the cooldown period."""
    if not _COOLDOWN_PATH.exists():
        return False
    minutes = _cooldown_minutes()
    age_minutes = (time.time() - _COOLDOWN_PATH.stat().st_mtime) / 60
    return age_minutes < minutes


def _touch_cooldown() -> None:
    """Record that a successful download just happened."""
    _COOLDOWN_PATH.touch()


# ---------------------------------------------------------------------------
# Lock file (prevents concurrent runs from cron overlap)
# ---------------------------------------------------------------------------

def acquire_lock() -> Optional[object]:
    """Acquire an exclusive lock. Returns the file object, or None if locked."""
    lock_path = Path(__file__).resolve().parent / ".lock"
    f = open(lock_path, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write(str(os.getpid()))
        f.flush()
        return f
    except OSError:
        f.close()
        return None


def release_lock(f) -> None:
    """Release the exclusive lock."""
    if f:
        try:
            fcntl.flock(f, fcntl.LOCK_UN)
            f.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# WiFi management (wpa_cli)
# ---------------------------------------------------------------------------

def _wpa_cli(interface: str, *args: str) -> subprocess.CompletedProcess:
    """Run a wpa_cli command and return the result."""
    cmd = ["wpa_cli", "-i", interface] + list(args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=10)


def get_current_ssid(interface: str) -> str:
    """Return the SSID of the currently connected WiFi network, or ''."""
    result = _wpa_cli(interface, "status")
    for line in result.stdout.splitlines():
        if line.startswith("ssid="):
            return line.split("=", 1)[1]
    return ""


def scan_for_ssid(interface: str, ssid: str) -> bool:
    """Trigger a WiFi scan and check if the given SSID is visible."""
    result = _wpa_cli(interface, "scan")
    if "FAIL" in result.stdout:
        log.warning("WiFi scan trigger failed")
        return False

    time.sleep(SCAN_WAIT_SECONDS)

    result = _wpa_cli(interface, "scan_results")
    for line in result.stdout.splitlines()[1:]:  # Skip header row
        parts = line.split("\t")
        if len(parts) >= 5 and parts[4] == ssid:
            log.info(f"FlashAir '{ssid}' detected (signal: {parts[2]} dBm)")
            return True
    return False


def connect_to_flashair(cfg: Config) -> int:
    """Connect to the FlashAir WiFi network. Returns a wpa_cli network ID."""
    iface = cfg.wifi_interface
    result = _wpa_cli(iface, "add_network")
    net_id = int(result.stdout.strip().splitlines()[-1])
    _wpa_cli(iface, "set_network", str(net_id), "ssid", f'"{cfg.flashair_ssid}"')
    _wpa_cli(iface, "set_network", str(net_id), "psk", f'"{cfg.flashair_password}"')
    _wpa_cli(iface, "select_network", str(net_id))
    log.info(f"Connecting to {cfg.flashair_ssid}...")
    return net_id


def reconnect_home(cfg: Config, net_id: Optional[int] = None) -> None:
    """Disconnect from FlashAir and reconnect to home WiFi."""
    iface = cfg.wifi_interface
    if net_id is not None:
        _wpa_cli(iface, "remove_network", str(net_id))
    _wpa_cli(iface, "reconfigure")

    log.info(f"Reconnecting to {cfg.home_ssid}...")
    deadline = time.time() + CONNECT_TIMEOUT
    while time.time() < deadline:
        if get_current_ssid(iface) == cfg.home_ssid:
            log.info(f"Connected to {cfg.home_ssid}")
            time.sleep(3)  # Allow DHCP to settle
            return
        time.sleep(1)

    log.warning(f"Could not reconnect to {cfg.home_ssid} within {CONNECT_TIMEOUT}s")


def wait_for_flashair(cfg: Config) -> bool:
    """Wait until the FlashAir HTTP server is reachable after connecting."""
    url = f"http://{cfg.flashair_ip}/command.cgi?op=100&DIR=/"
    deadline = time.time() + CONNECT_TIMEOUT
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    log.info(f"FlashAir reachable at {cfg.flashair_ip}")
                    return True
        except Exception:
            pass
        time.sleep(1)
    log.error(f"FlashAir HTTP not reachable within {CONNECT_TIMEOUT}s")
    return False


# ---------------------------------------------------------------------------
# FlashAir HTTP API
# ---------------------------------------------------------------------------

def list_flashair_files(ip: str, directory: str) -> list[str]:
    """List CSV files on the FlashAir matching the engine monitor pattern.

    Uses the FlashAir command.cgi op=100 directory listing API.
    Returns a sorted list of filenames.
    """
    url = f"http://{ip}/command.cgi?op=100&DIR={directory}"
    with urllib.request.urlopen(url, timeout=FLASHAIR_HTTP_TIMEOUT) as resp:
        content = resp.read().decode("utf-8")

    files = []
    for line in content.splitlines():
        if line.startswith("WLANSD_FILELIST"):
            continue
        parts = line.split(",")
        if len(parts) >= 2:
            fname = parts[1].strip()
            if fname.startswith("log_") and fname.lower().endswith(".csv"):
                files.append(fname)
    return sorted(files)


def download_file(ip: str, directory: str, filename: str, local_dir: str) -> str:
    """Download a single file from FlashAir via HTTP. Returns the local path."""
    dir_part = directory.strip("/")
    if dir_part:
        url = f"http://{ip}/{dir_part}/{filename}"
    else:
        url = f"http://{ip}/{filename}"

    local_path = Path(local_dir) / filename

    log.info(f"Downloading {filename}...")
    with urllib.request.urlopen(url, timeout=DOWNLOAD_TIMEOUT) as resp:
        local_path.write_bytes(resp.read())

    size_kb = local_path.stat().st_size / 1024
    log.info(f"  Saved {filename} ({size_kb:.0f} KB)")
    return str(local_path)


# ---------------------------------------------------------------------------
# File management
# ---------------------------------------------------------------------------

def collect_new_files(
    remote_files: list[str], watermark: str
) -> tuple[list[str], list[str]]:
    """Split files into (new, skipped) based on the watermark.

    Any file whose name sorts <= watermark is considered already synced.
    """
    new = []
    skipped = []
    for f in remote_files:
        if watermark and f <= watermark:
            skipped.append(f)
        else:
            new.append(f)
    return new, skipped


def cleanup_local(
    directory: str, watermark: str, keep_recent: int = 10
) -> int:
    """Delete already-synced local CSVs, keeping the most recent *keep_recent*."""
    p = Path(directory).resolve()
    if not p.is_dir():
        return 0

    all_csvs = sorted(p.glob("log_*_*.csv"), key=lambda f: f.name)
    protected = {f.name for f in all_csvs[-keep_recent:]}

    deleted = 0
    for f in all_csvs:
        if f.name <= watermark and f.name not in protected:
            f.unlink()
            log.info(f"Cleaned up: {f.name}")
            deleted += 1

    if deleted:
        log.info(
            f"Deleted {deleted} local CSV(s), "
            f"kept {min(len(all_csvs), keep_recent)} most recent."
        )
    return deleted


# ---------------------------------------------------------------------------
# SCP transfer
# ---------------------------------------------------------------------------

def load_last_scpd() -> str:
    """Return the LAST_SCPD watermark (last file successfully SCP'd)."""
    return _read_env().get("LAST_SCPD", "")


def save_last_scpd(filename: str) -> None:
    env = _read_env()
    env["LAST_SCPD"] = filename
    _write_env(env)
    log.info(f"Updated LAST_SCPD={filename}")


def pending_scp_files(local_dir: str, scp_watermark: str) -> list[str]:
    """Return local CSV paths that haven't been SCP'd yet (after the SCP watermark)."""
    p = Path(local_dir).resolve()
    if not p.is_dir():
        return []
    all_csvs = sorted(p.glob("log_*_*.csv"), key=lambda f: f.name)
    return [str(f) for f in all_csvs if not scp_watermark or f.name > scp_watermark]


def scp_files(cfg: Config, local_paths: list[str]) -> int:
    """SCP files to the remote server. Returns the number successfully transferred."""
    transferred = 0
    last_ok = ""
    for path in local_paths:
        fname = Path(path).name
        dest = f"{cfg.remote_user}@{cfg.remote_host}:{cfg.remote_dir}/{fname}"
        cmd = [
            "scp",
            "-i", cfg.ssh_key_path,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10",
            path, dest,
        ]
        log.info(f"SCP {fname} → {cfg.remote_host}:{cfg.remote_dir}/")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            transferred += 1
            last_ok = fname
        else:
            log.error(f"SCP failed for {fname}: {result.stderr.strip()}")
            break  # Stop on first failure (network likely down)

    if last_ok:
        save_last_scpd(last_ok)

    log.info(f"Transferred {transferred}/{len(local_paths)} file(s) to {cfg.remote_host}")
    return transferred


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(resync: bool = False) -> None:
    cfg = load_config()

    # Acquire exclusive lock (prevents overlapping cron runs)
    lock = acquire_lock()
    if not lock:
        log.debug("Another instance is running, exiting.")
        return

    try:
        iface = cfg.wifi_interface
        current = get_current_ssid(iface)
        net_id = None
        already_on_flashair = (current == cfg.flashair_ssid)

        # --- Phase 1: Download from FlashAir (if available) ---
        if already_on_flashair:
            log.info("Already on FlashAir (recovery from previous run).")
        elif _in_cooldown() and not resync:
            log.debug(f"In cooldown (last sync < {_cooldown_minutes()}m ago), skipping FlashAir.")
        elif not current:
            log.warning("WiFi disconnected. Attempting to reconnect home...")
            reconnect_home(cfg)
            # Fall through to Phase 2 (SCP retry)
        else:
            # On home WiFi — scan for FlashAir
            if scan_for_ssid(iface, cfg.flashair_ssid):
                net_id = connect_to_flashair(cfg)
                already_on_flashair = True
            else:
                log.debug(f"FlashAir '{cfg.flashair_ssid}' not in range.")

        if already_on_flashair or net_id is not None:
            try:
                if not wait_for_flashair(cfg):
                    log.error("Cannot reach FlashAir HTTP server.")
                else:
                    remote_files = list_flashair_files(cfg.flashair_ip, cfg.flashair_dir)
                    log.info(f"Found {len(remote_files)} CSV(s) on FlashAir.")

                    watermark = "" if resync else load_last_synced()
                    new_files, skipped = collect_new_files(remote_files, watermark)

                    if skipped:
                        log.info(f"Skipped {len(skipped)} already-synced file(s).")

                    if not new_files:
                        log.info("No new files to download.")
                        _touch_cooldown()
                    else:
                        log.info(f"Downloading {len(new_files)} new file(s)...")
                        downloaded = []
                        for fname in new_files:
                            try:
                                path = download_file(
                                    cfg.flashair_ip, cfg.flashair_dir,
                                    fname, cfg.local_csv_dir,
                                )
                                downloaded.append(path)
                            except Exception as e:
                                log.error(f"Failed to download {fname}: {e}")

                        if downloaded:
                            newest = max(Path(p).name for p in downloaded)
                            save_last_synced(newest)
                            _touch_cooldown()
                            log.info(f"Cooldown set ({_cooldown_minutes()}m).")
            finally:
                reconnect_home(cfg, net_id)

        # --- Phase 2: SCP any un-transferred local files ---
        to_scp = pending_scp_files(cfg.local_csv_dir, load_last_scpd())
        if to_scp:
            log.info(f"{len(to_scp)} file(s) pending SCP.")
            scp_files(cfg, to_scp)
        else:
            log.debug("No files pending SCP.")

        # --- Phase 3: Cleanup old local files ---
        wm = load_last_synced()
        if wm:
            cleanup_local(cfg.local_csv_dir, wm)

        log.info("Sync complete.")

    finally:
        release_lock(lock)


def main():
    parser = argparse.ArgumentParser(
        description="Sync engine monitor CSVs from a FlashAir SD card"
    )
    parser.add_argument(
        "--resync", action="store_true",
        help="Ignore the watermark and re-download all files",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    run(resync=args.resync)


if __name__ == "__main__":
    main()
