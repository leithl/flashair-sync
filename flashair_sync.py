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
    REMOTE_USER=youruser                SSH user on the remote server
    REMOTE_DIR=/path/to/csvs            Destination directory on the remote server

    # Optional
    FLASHAIR_IP=192.168.0.1             FlashAir IP (default: 192.168.0.1)
    SSH_KEY_PATH=~/.ssh/id_ed25519      SSH private key (default: ~/.ssh/id_ed25519)
    WIFI_INTERFACE=wlan0                WiFi interface (default: wlan0)
    COOLDOWN_MINUTES=30                 Minutes to wait before re-checking FlashAir (default: 30)
    POLL_SECONDS=60                     Daemon poll interval in seconds (default: 60)

    # Optional screenshot path (BMP from card's /Screenshot/ dir).
    # All three must be set together or all left unset.
    FLASHAIR_SHOT_DIR=/Screenshot       Directory on the card containing BMPs
    LOCAL_SHOT_DIR=/run/flashair-shots  Tmpfs staging dir (avoids SD-card wear)
    REMOTE_SHOT_DIR=/path/to/shots      Destination on the remote server

    # Managed by the script (do not edit)
    LAST_SYNCED=                        Watermark — filename of last downloaded CSV
    LAST_SCPD=                          Watermark — filename of last SCP'd CSV
    LAST_SHOT_SCPD=                     Watermark — filename of last SCP'd BMP

Usage:
    python3 flashair_sync.py            # One-shot sync (for cron)
    python3 flashair_sync.py --daemon   # Long-running daemon (for systemd)
    python3 flashair_sync.py --resync   # Re-download all files
    python3 flashair_sync.py -v         # Verbose logging
"""

import argparse
import fcntl
import json
import logging
import os
import signal
import subprocess
import sys
import threading
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
POLL_SECONDS_DEFAULT = 60

# Status file path. /run is tmpfs on the Pi — no SD-card wear from
# per-state-change writes, and the file naturally vanishes on reboot.
# The hangar controller (remote-switch) WSGI app reads this directly to
# render "FlashAir: N files, X ago" on the chart-card header.
# Status file lives inside the service's RuntimeDirectory (/run/flashair-shots/)
# so the daemon (User=leith) can write to it. /run/ itself is root:root 0755
# — a non-root daemon can't create files there.
FLASHAIR_STATUS_FILE = Path("/run/flashair-shots/heater-flashair.json")


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
    # Screenshot path (opt-in; all three must be set together)
    flashair_shot_dir: str = ""
    local_shot_dir: str = ""
    remote_shot_dir: str = ""

    def __post_init__(self):
        if not self.ssh_key_path:
            self.ssh_key_path = str(Path.home() / ".ssh" / "id_ed25519")
        if not self.wifi_interface:
            self.wifi_interface = "wlan0"

    @property
    def screenshots_enabled(self) -> bool:
        return bool(self.flashair_shot_dir and self.local_shot_dir and self.remote_shot_dir)


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
        flashair_shot_dir=_get("FLASHAIR_SHOT_DIR"),
        local_shot_dir=_get("LOCAL_SHOT_DIR"),
        remote_shot_dir=_get("REMOTE_SHOT_DIR"),
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

    # Screenshot fields are opt-in but must be set together
    shot_fields = [cfg.flashair_shot_dir, cfg.local_shot_dir, cfg.remote_shot_dir]
    if any(shot_fields) and not all(shot_fields):
        log.error(
            "Screenshot config requires ALL of FLASHAIR_SHOT_DIR, "
            "LOCAL_SHOT_DIR, REMOTE_SHOT_DIR (or none).",
        )
        sys.exit(1)

    # Ensure local CSV directory exists
    Path(cfg.local_csv_dir).mkdir(parents=True, exist_ok=True)
    if cfg.screenshots_enabled:
        Path(cfg.local_shot_dir).mkdir(parents=True, exist_ok=True)

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
# Daemon helpers
# ---------------------------------------------------------------------------

_shutdown = False


def _signal_handler(signum, _frame):
    global _shutdown
    log.info("Received signal %s, shutting down...", signum)
    _shutdown = True


def _poll_seconds() -> int:
    """Return the configured daemon poll interval in seconds."""
    val = os.environ.get("POLL_SECONDS", "") or _read_env().get("POLL_SECONDS", "")
    try:
        return int(val) if val else POLL_SECONDS_DEFAULT
    except ValueError:
        return POLL_SECONDS_DEFAULT


def _remaining_cooldown_seconds() -> float:
    """Return seconds left in the cooldown period (0 if not in cooldown)."""
    if not _COOLDOWN_PATH.exists():
        return 0
    age = time.time() - _COOLDOWN_PATH.stat().st_mtime
    remaining = (_cooldown_minutes() * 60) - age
    return max(0, remaining)


def _interruptible_sleep(seconds: float) -> None:
    """Sleep for *seconds*, waking early if a shutdown signal arrives."""
    deadline = time.time() + seconds
    while not _shutdown and time.time() < deadline:
        time.sleep(min(1.0, deadline - time.time()))


# ---------------------------------------------------------------------------
# Status state (written to FLASHAIR_STATUS_FILE on every change, daemon mode)
# ---------------------------------------------------------------------------
# Lets a peer on the same host ("is it safe to power down the plane?") answer
# without scraping the journal. The Pi running flashair-sync also runs the
# remote-switch web UI as the Apache mod_wsgi process — both processes share
# the filesystem, so a single tmpfs file is the cheapest "API" available.
# Lock is kept as cheap insurance against re-entry from a future caller (only
# one writer thread today).

_status_lock = threading.Lock()
_status: dict = {
    # Pipeline stage (added 2026-05-21). Lets a glance-only consumer surface
    # *what* is happening, not just whether something is. Linear progression:
    #   idle → scanning → downloading_logs → downloading_shots
    #        → uploading_logs → uploading_shots → idle
    # Stages may be skipped when their phase has no new work.
    "stage": "idle",

    # Per-stage progress — current stage's "X of Y files".
    "files_done": 0,
    "files_total": 0,

    # Session counts — set once per cycle from the FlashAir directory listings.
    # Lets the consumer show "N shots queued" during the logs phase and
    # "N logs done" during the shots phase without re-enumerating itself.
    "session_csv_n": 0,
    "session_shots_n": 0,

    # CSV watermark info
    "last_sync_epoch": None,    # epoch seconds of most recent reach-and-process cycle
    "last_sync_files_n": 0,     # number of files downloaded in that cycle

    # Screenshot watermark info — parallels last_sync_* for the BMP pipeline.
    # None until at least one screenshot cycle has completed since daemon start.
    "last_shot_sync_epoch": None,
    "last_shot_sync_files_n": 0,

    # WiFi SSID the daemon is currently associated to. Empty string from
    # wpa_cli (no association) is normalised to None. The remote-switch OLED
    # uses this to surface "on FlashAir-Card" (amber) vs "on Hangar-WiFi"
    # (grey) vs "no wifi" (red), letting the user see at a glance whether
    # the radio actually hopped to the expected network.
    "current_ssid": None,

    # Back-compat with the v0 contract — older consumers (pre-stage) read these.
    # `transferring=True` whenever stage is any of the four active transfer stages.
    "transferring": False,
    "current_file": None,
}


def _status_snapshot() -> dict:
    """Return a JSON-serialisable snapshot. `epoch` is when this snapshot was taken."""
    with _status_lock:
        snap = dict(_status)
    snap["epoch"] = int(time.time())
    return snap


def _write_status() -> None:
    """Atomically write the current status snapshot to FLASHAIR_STATUS_FILE.

    Temp + rename keeps a concurrent reader (remote-switch's WSGI handler)
    from ever seeing a half-written file. 0664 perms so www-data can read
    it; we run as `pi` so chown isn't possible (and unnecessary — other-read
    covers the WSGI process). Never raises — a tmpfs write failure should
    not crash the sync loop.
    """
    snap = _status_snapshot()
    try:
        FLASHAIR_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = FLASHAIR_STATUS_FILE.with_suffix(FLASHAIR_STATUS_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(snap))
        os.chmod(str(tmp), 0o664)
        os.replace(str(tmp), str(FLASHAIR_STATUS_FILE))
    except OSError as e:
        log.debug(f"Status file write failed: {e}")


def _status_set_transferring(filename: str) -> None:
    with _status_lock:
        _status["transferring"] = True
        _status["current_file"] = filename
    _write_status()


def _status_clear_transferring() -> None:
    with _status_lock:
        _status["transferring"] = False
        _status["current_file"] = None
    _write_status()


def _status_record_sync(files_n: int) -> None:
    """Mark a CSV sync session as complete: we reached the card and processed N files."""
    with _status_lock:
        _status["last_sync_epoch"] = int(time.time())
        _status["last_sync_files_n"] = int(files_n)
    _write_status()


def _status_record_shot_sync(files_n: int) -> None:
    """Mark a screenshot sync session as complete. Parallels _status_record_sync."""
    with _status_lock:
        _status["last_shot_sync_epoch"] = int(time.time())
        _status["last_shot_sync_files_n"] = int(files_n)
    _write_status()


def _status_set_stage(stage: str, *, files_total: int = 0) -> None:
    """Transition to a new pipeline stage. Resets per-stage progress to 0 of files_total."""
    with _status_lock:
        _status["stage"] = stage
        _status["files_done"] = 0
        _status["files_total"] = int(files_total)
    _write_status()


def _status_inc_files_done() -> None:
    """Increment files_done by one (call after a successful per-file transfer)."""
    with _status_lock:
        _status["files_done"] += 1
    _write_status()


def _status_set_session_counts(*, csv_n: int = 0, shots_n: int = 0) -> None:
    """Set the once-per-cycle session totals. Called after enumerating both
    directories on the FlashAir, before any downloads begin."""
    with _status_lock:
        _status["session_csv_n"] = int(csv_n)
        _status["session_shots_n"] = int(shots_n)
    _write_status()


def _status_set_ssid(ssid: str) -> None:
    """Update current_ssid. Empty string (wpa_cli's "not associated" response)
    is normalised to None so consumers can distinguish "wifi up but on no
    network" from "we just haven't checked yet"."""
    with _status_lock:
        _status["current_ssid"] = ssid if ssid else None
    _write_status()


def _status_init_from_disk() -> None:
    """Prime last_sync_epoch from .last_sync mtime so a daemon restart doesn't
    erase the 'last sync at X' signal. The file count from that pre-restart
    session isn't recoverable — left at 0 until the next sync completes."""
    if _COOLDOWN_PATH.exists():
        try:
            mtime = int(_COOLDOWN_PATH.stat().st_mtime)
            with _status_lock:
                _status["last_sync_epoch"] = mtime
        except OSError:
            pass


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
    return sorted(list_flashair_files_with_sizes(ip, directory).keys())


def _parse_flashair_listing(content: str) -> dict[str, int]:
    """Parse op=100 directory listing -> {filename: size_bytes}.

    Output format per line: DIR,FILENAME,SIZE,ATTR,DATE,TIME
    Caller filters by name.
    """
    out: dict[str, int] = {}
    for line in content.splitlines():
        if line.startswith("WLANSD_FILELIST"):
            continue
        parts = line.split(",")
        if len(parts) < 3:
            continue
        fname = parts[1].strip()
        try:
            out[fname] = int(parts[2].strip())
        except ValueError:
            continue
    return out


def list_flashair_files_with_sizes(ip: str, directory: str) -> dict[str, int]:
    """List CSV files on FlashAir matching the engine-monitor pattern."""
    url = f"http://{ip}/command.cgi?op=100&DIR={directory}"
    with urllib.request.urlopen(url, timeout=FLASHAIR_HTTP_TIMEOUT) as resp:
        content = resp.read().decode("utf-8")
    return {
        f: sz for f, sz in _parse_flashair_listing(content).items()
        if f.startswith("log_") and f.lower().endswith(".csv")
    }


# How long to wait between size polls to confirm a file has stopped growing.
# The avionics logs at 1 Hz but may buffer writes and flush in chunks (we
# don't have firm timing — could be every second or every minute). Default
# of 90 sec is conservative enough to span any reasonable flush interval.
# Configurable for tests.
FLASHAIR_STABILITY_DELAY_SEC = int(os.environ.get("FLASHAIR_STABILITY_DELAY_SEC", "90"))

# How many recently-synced local files to re-check on every cycle. Belt &
# braces against the stability check returning a false "stable" during a
# flush gap — if any of the last N files grew on FlashAir since we pulled
# them, re-download (and rename, to avoid colliding with the partial that
# the downstream pipeline may have already accepted).
FLASHAIR_LOOKBACK_FILES = int(os.environ.get("FLASHAIR_LOOKBACK_FILES", "5"))


def filter_stable_files(
    ip: str, directory: str, candidates: list[str],
    delay_sec: int = FLASHAIR_STABILITY_DELAY_SEC,
) -> tuple[list[str], list[str]]:
    """Return (stable, unstable) split of *candidates* based on whether the
    file's size on FlashAir is unchanged across two polls *delay_sec* apart.

    The avionics keeps writing to the active CSV throughout a flight. If
    we download it while it's still growing, we capture only the bytes
    written so far — a partial that Savvy then accepts as "Success (0
    flights)" because the truncated chunk doesn't contain a complete
    takeoff+landing cycle.

    Returns:
        stable:   files whose size was identical in both polls — safe to
                  download.
        unstable: files that grew between polls — skip; the next sync
                  cycle will re-check once the engine is off.
    """
    if not candidates:
        return [], []
    first = list_flashair_files_with_sizes(ip, directory)
    time.sleep(delay_sec)
    second = list_flashair_files_with_sizes(ip, directory)
    stable, unstable = [], []
    for f in candidates:
        s1, s2 = first.get(f), second.get(f)
        if s1 is None or s2 is None:
            # File appeared/disappeared mid-check — treat as unstable.
            unstable.append(f)
        elif s1 == s2:
            stable.append(f)
        else:
            unstable.append(f)
    return stable, unstable


def find_grown_recent_files(
    ip: str, directory: str,
    local_dir: str, previous_watermark: str,
    lookback: int = FLASHAIR_LOOKBACK_FILES,
) -> list[tuple[str, int, int]]:
    """Belt & braces: re-check the previous-watermark file and N files
    before it for size drift.

    The relevant window is anchored at the PREVIOUS watermark — the most
    recent file flashair-sync had synced before this run. That's where the
    last partial would live: if the avionics was actively writing it when
    we previously polled, the partial is what got SCP'd downstream, and
    by now (next sync, post engine-off) the FlashAir version will be
    bigger. Looking at "the last 5 local files" instead would miss this
    case after a multi-day trip — by the time the plane returns, the
    most recent local files are the brand-new trip files, and the
    partial-departure file is N+ back from the head of the list.

    Layer 2 runs BEFORE cleanup_local, so the partial's local copy is
    still present and the size compare is direct (no need to SSH the
    downstream host).

    Returns [(filename, local_size, remote_size), ...] for files whose
    FlashAir version is larger than what we have locally.
    """
    if not previous_watermark:
        return []
    p = Path(local_dir)
    if not p.is_dir():
        return []
    try:
        flashair = list_flashair_files_with_sizes(ip, directory)
    except Exception as e:
        log.warning(f"lookback: could not list FlashAir: {e}")
        return []
    fa_sorted = sorted(flashair.keys())
    try:
        idx = fa_sorted.index(previous_watermark)
    except ValueError:
        log.info(f"lookback: prev watermark {previous_watermark!r} no longer on FlashAir; skipping")
        return []
    # Take the watermark + lookback files before it
    start = max(0, idx - lookback)
    candidates = fa_sorted[start:idx + 1]
    grown = []
    for fname in candidates:
        local_path = p / fname
        if not local_path.exists():
            # Cleanup already removed it (shouldn't happen — we run before
            # cleanup_local — but be defensive).
            continue
        local_size = local_path.stat().st_size
        remote_size = flashair[fname]
        if remote_size > local_size:
            grown.append((fname, local_size, remote_size))
    return grown


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
# Screenshot path (BMPs from FLASHAIR_SHOT_DIR, staged in tmpfs LOCAL_SHOT_DIR)
# ---------------------------------------------------------------------------
# Differences from the CSV path:
#  - LOCAL_SHOT_DIR is typically /run/flashair-shots (tmpfs) — avoids SD-card
#    wear, since BMPs are 2.81 MB each vs CSVs at <500 KB
#  - Single watermark (LAST_SHOT_SCPD). The downstream host keeps originals,
#    so the Pi doesn't need a "10 most recent" safety buffer.
#  - No stability check / lookback: screenshots are single-frame writes,
#    not streamed like the active flight CSV
#  - BMP is deleted from /run as soon as SCP confirms delivery

def list_flashair_screenshots(ip: str, directory: str) -> list[str]:
    """List BMP screenshot files on the FlashAir."""
    url = f"http://{ip}/command.cgi?op=100&DIR={directory}"
    with urllib.request.urlopen(url, timeout=FLASHAIR_HTTP_TIMEOUT) as resp:
        content = resp.read().decode("utf-8")
    return sorted(
        f for f in _parse_flashair_listing(content)
        if f.lower().endswith(".bmp")
    )


def load_last_shot_scpd() -> str:
    return _read_env().get("LAST_SHOT_SCPD", "")


def save_last_shot_scpd(filename: str) -> None:
    env = _read_env()
    env["LAST_SHOT_SCPD"] = filename
    _write_env(env)
    log.info(f"Updated LAST_SHOT_SCPD={filename}")


def pending_scp_shots(local_dir: str) -> list[Path]:
    """All BMPs currently staged in local_dir. /run wipes on reboot, so
    presence in this dir means 'downloaded but not yet SCP-confirmed'."""
    p = Path(local_dir)
    if not p.is_dir():
        return []
    return sorted(p.glob("*.bmp"))


def download_screenshots(
    cfg: Config, new_files: list[str], resync: bool = False,
) -> int:
    """Phase-1b BMP download: pull new BMPs from FlashAir into LOCAL_SHOT_DIR.

    `new_files` is a pre-enumerated list (post-watermark) from the caller's
    earlier `list_flashair_screenshots()` call — passed in so the dashboard's
    session totals are set before any download begins, and so this function
    doesn't repeat the listing HTTP call.

    `resync` is accepted for symmetry with the CSV path but currently has
    no effect (carry-over filtering still uses the local staging dir).

    Returns the number of files successfully downloaded.
    """
    del resync  # reserved for symmetry; no resync-specific behavior here
    Path(cfg.local_shot_dir).mkdir(parents=True, exist_ok=True)

    # Carry-over from previous cycle's failed SCP: don't re-download
    already_local = {p.name for p in pending_scp_shots(cfg.local_shot_dir)}
    to_download = [f for f in new_files if f not in already_local]
    carryover = len(new_files) - len(to_download)
    if carryover:
        log.info(
            f"Screenshot: {carryover} already in {cfg.local_shot_dir}, "
            f"skipping re-download."
        )

    if not to_download:
        return 0

    log.info(f"Screenshot: downloading {len(to_download)} new BMP(s)...")
    downloaded = 0
    for fname in to_download:
        _status_set_transferring(fname)
        try:
            download_file(
                cfg.flashair_ip, cfg.flashair_shot_dir,
                fname, cfg.local_shot_dir,
            )
            downloaded += 1
            _status_inc_files_done()
        except Exception as e:
            log.error(f"Failed to download screenshot {fname}: {e}")
        finally:
            _status_clear_transferring()
    return downloaded


def scp_screenshots(cfg: Config, local_paths: list[Path]) -> int:
    """SCP staged BMPs to REMOTE_SHOT_DIR. On each success, advance
    LAST_SHOT_SCPD and delete the local file.

    BMPs are ~2.81 MB each — bigger SCP timeout than the CSV path because
    the link to the downstream host is residential internet, not LAN.
    """
    transferred = 0
    last_ok = ""
    for path in local_paths:
        fname = path.name
        dest = f"{cfg.remote_user}@{cfg.remote_host}:{cfg.remote_shot_dir}/{fname}"
        cmd = [
            "scp",
            "-i", cfg.ssh_key_path,
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10",
            str(path), dest,
        ]
        log.info(f"SCP {fname} → {cfg.remote_host}:{cfg.remote_shot_dir}/")
        _status_set_transferring(fname)
        try:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            except subprocess.TimeoutExpired:
                log.error(f"SCP timed out for {fname}")
                break
            if result.returncode == 0:
                transferred += 1
                last_ok = fname
                _status_inc_files_done()
                try:
                    path.unlink()
                except OSError as e:
                    log.warning(f"Could not delete staged {path}: {e}")
            else:
                log.error(f"SCP failed for {fname}: {result.stderr.strip()}")
                break
        finally:
            _status_clear_transferring()

    if last_ok:
        save_last_shot_scpd(last_ok)

    log.info(f"Screenshot: transferred {transferred}/{len(local_paths)}.")
    return transferred


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
        _status_set_transferring(fname)
        try:
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            except subprocess.TimeoutExpired:
                log.error(f"SCP timed out for {fname}")
                break
            if result.returncode == 0:
                transferred += 1
                last_ok = fname
                _status_inc_files_done()
            else:
                log.error(f"SCP failed for {fname}: {result.stderr.strip()}")
                break  # Stop on first failure (network likely down)
        finally:
            _status_clear_transferring()

    if last_ok:
        save_last_scpd(last_ok)

    log.info(f"Transferred {transferred}/{len(local_paths)} file(s) to {cfg.remote_host}")
    return transferred


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(resync: bool = False, _lock=None) -> bool:
    cfg = load_config()

    # Acquire exclusive lock (prevents overlapping cron runs).
    # In daemon mode the caller holds the lock for us (_lock is set).
    own_lock = _lock is None
    if own_lock:
        lock = acquire_lock()
        if not lock:
            log.debug("Another instance is running, exiting.")
            return False
    else:
        lock = _lock

    try:
        # Fresh cycle — reset stage + session counters so a stale dashboard
        # doesn't carry over "5 shots queued" from a previous run.
        _status_set_stage("scanning")
        _status_set_session_counts(csv_n=0, shots_n=0)

        iface = cfg.wifi_interface
        current = get_current_ssid(iface)
        _status_set_ssid(current)
        net_id = None
        already_on_flashair = (current == cfg.flashair_ssid)

        # Status: count successful FlashAir HTTP downloads this run; used by
        # /status to answer "did all the CSVs come off the card?".
        session_downloads = 0
        session_shot_downloads = 0
        reached_card = False

        # --- Phase 1: Download from FlashAir (if available) ---
        if already_on_flashair:
            log.info("Already on FlashAir (recovery from previous run).")
        elif _in_cooldown() and not resync:
            log.debug(f"In cooldown (last sync < {_cooldown_minutes()}m ago), skipping FlashAir.")
        elif not current:
            log.warning("WiFi disconnected. Attempting to reconnect home...")
            reconnect_home(cfg)
            _status_set_ssid(get_current_ssid(iface))
            # Fall through to Phase 2 (SCP retry)
        else:
            # On home WiFi — scan for FlashAir
            if scan_for_ssid(iface, cfg.flashair_ssid):
                net_id = connect_to_flashair(cfg)
                already_on_flashair = True
                _status_set_ssid(get_current_ssid(iface))
            else:
                log.debug(f"FlashAir '{cfg.flashair_ssid}' not in range.")

        if already_on_flashair or net_id is not None:
            try:
                if not wait_for_flashair(cfg):
                    log.error("Cannot reach FlashAir HTTP server.")
                else:
                    reached_card = True
                    remote_files = list_flashair_files(cfg.flashair_ip, cfg.flashair_dir)
                    log.info(f"Found {len(remote_files)} CSV(s) on FlashAir.")

                    watermark = "" if resync else load_last_synced()
                    previous_watermark = watermark  # captured before any save_last_synced
                    new_files, skipped = collect_new_files(remote_files, watermark)

                    if skipped:
                        log.info(f"Skipped {len(skipped)} already-synced file(s).")

                    # Skip the file that's still being written by the
                    # avionics. Only the most recent CSV on the SD card can
                    # be the active one (avionics writes serially: it
                    # closes file N before opening file N+1). So the
                    # stability check only needs to run on the newest
                    # candidate; everything older is already closed.
                    #
                    # Without this guard we'd grab a partial snapshot
                    # (e.g. 34 sec of pre-flight engine warmup captured as
                    # the start of a 2-hour flight), the watermark would
                    # advance past it, and we'd never re-pull the
                    # complete file. Next sync re-checks any file we
                    # defer here.
                    if new_files:
                        newest = new_files[-1]  # new_files is sorted
                        stable, unstable = filter_stable_files(
                            cfg.flashair_ip, cfg.flashair_dir, [newest],
                        )
                        if unstable:
                            log.info(
                                f"Deferring actively-written file: {unstable[0]} "
                                f"(will retry next cycle)"
                            )
                            new_files = new_files[:-1]  # all but the unstable one

                    # Pre-enumerate screenshots so the dashboard can show
                    # "N shots queued" alongside CSV progress. Done before
                    # the CSV download loop so the consumer sees both totals
                    # from the start of the cycle.
                    new_shots: list[str] = []
                    if cfg.screenshots_enabled:
                        try:
                            remote_shots = list_flashair_screenshots(
                                cfg.flashair_ip, cfg.flashair_shot_dir,
                            )
                            shot_watermark = "" if resync else load_last_shot_scpd()
                            new_shots, _shot_skipped = collect_new_files(
                                remote_shots, shot_watermark,
                            )
                            if _shot_skipped:
                                log.info(
                                    f"Screenshot: skipped "
                                    f"{len(_shot_skipped)} already-synced."
                                )
                        except Exception as e:
                            log.warning(
                                f"Screenshot listing failed "
                                f"(will retry next cycle): {e}"
                            )

                    _status_set_session_counts(
                        csv_n=len(new_files), shots_n=len(new_shots),
                    )

                    if not new_files:
                        log.info("No new files to download.")
                        _touch_cooldown()
                    else:
                        _status_set_stage(
                            "downloading_logs", files_total=len(new_files),
                        )
                        log.info(f"Downloading {len(new_files)} new file(s)...")
                        downloaded = []
                        for fname in new_files:
                            _status_set_transferring(fname)
                            try:
                                path = download_file(
                                    cfg.flashair_ip, cfg.flashair_dir,
                                    fname, cfg.local_csv_dir,
                                )
                                downloaded.append(path)
                                session_downloads += 1
                                _status_inc_files_done()
                            except Exception as e:
                                log.error(f"Failed to download {fname}: {e}")
                            finally:
                                _status_clear_transferring()

                        if downloaded:
                            newest = max(Path(p).name for p in downloaded)
                            save_last_synced(newest)
                            if len(downloaded) == len(new_files):
                                _touch_cooldown()
                                log.info(f"Cooldown set ({_cooldown_minutes()}m).")
                            else:
                                log.warning(
                                    "Downloaded %d/%d file(s); will retry remaining on next cycle.",
                                    len(downloaded), len(new_files),
                                )

                    # Lookback pass: catch any partials the stability check
                    # may have let slip through (e.g. flush-gap false stable).
                    # Anchor at previous_watermark (the file we synced just
                    # before THIS sync) and re-check it + N files before it.
                    # If FlashAir's version is larger than our local copy,
                    # re-download under a `_rescue` name so it doesn't
                    # collide downstream.
                    grown = find_grown_recent_files(
                        cfg.flashair_ip, cfg.flashair_dir, cfg.local_csv_dir,
                        previous_watermark,
                    )
                    for fname, lsize, rsize in grown:
                        log.warning(
                            f"Lookback: {fname} grew on FlashAir "
                            f"({lsize:,} -> {rsize:,}). Re-downloading."
                        )
                        # Run stability check on the grown file too — no
                        # point re-pulling if it's STILL being written.
                        stable, _ = filter_stable_files(
                            cfg.flashair_ip, cfg.flashair_dir, [fname],
                        )
                        if not stable:
                            log.info(f"  still growing, deferring: {fname}")
                            continue
                        _status_set_transferring(fname)
                        try:
                            path = download_file(
                                cfg.flashair_ip, cfg.flashair_dir,
                                fname, cfg.local_csv_dir,
                            )
                            p = Path(path)
                            renamed = p.with_name(p.stem + "_rescue" + p.suffix)
                            p.rename(renamed)
                            log.info(f"  renamed {p.name} -> {renamed.name}")
                            session_downloads += 1
                        except Exception as e:
                            log.warning(f"  lookback re-download failed for {fname}: {e}")
                        finally:
                            _status_clear_transferring()

                    # --- Phase 1b: Screenshot download (opt-in) ---
                    # Runs while still on FlashAir, after the CSV path.
                    # Wrapped so a screenshot failure doesn't tank the CSV sync.
                    # `new_shots` was pre-enumerated above so the dashboard's
                    # session totals were known from the start of the cycle.
                    if cfg.screenshots_enabled and new_shots:
                        _status_set_stage(
                            "downloading_shots", files_total=len(new_shots),
                        )
                        try:
                            shot_n = download_screenshots(
                                cfg, new_shots, resync=resync,
                            )
                            session_downloads += shot_n
                            session_shot_downloads = shot_n
                        except Exception as e:
                            log.error(f"Screenshot download phase failed: {e}")
            finally:
                reconnect_home(cfg, net_id)
                _status_set_ssid(get_current_ssid(iface))
                _status_clear_transferring()
                if reached_card:
                    _status_record_sync(session_downloads)

        # --- Phase 2: SCP any un-transferred local files ---
        did_work = False
        scp_ok = True
        transferred = 0
        to_scp = pending_scp_files(cfg.local_csv_dir, load_last_scpd())
        if to_scp:
            did_work = True
            log.info(f"{len(to_scp)} file(s) pending SCP.")
            _status_set_stage("uploading_logs", files_total=len(to_scp))
            transferred = scp_files(cfg, to_scp)
            scp_ok = (transferred == len(to_scp))
        else:
            log.debug("No files pending SCP.")

        # --- Phase 2b: SCP pending screenshots (opt-in) ---
        shots_ok = True
        shots_transferred = 0
        pending_shots: list[Path] = []
        if cfg.screenshots_enabled:
            pending_shots = pending_scp_shots(cfg.local_shot_dir)
            if pending_shots:
                did_work = True
                log.info(f"{len(pending_shots)} BMP(s) pending SCP.")
                _status_set_stage("uploading_shots", files_total=len(pending_shots))
                shots_transferred = scp_screenshots(cfg, pending_shots)
                shots_ok = (shots_transferred == len(pending_shots))
            # Even when nothing was pending right now, a download cycle that
            # processed shots in Phase 1b counts as a completed shot sync.
            if shots_transferred or session_shot_downloads:
                _status_record_shot_sync(
                    shots_transferred or session_shot_downloads,
                )

        # --- Phase 3: Cleanup old local files ---
        wm = load_last_synced()
        if wm:
            cleanup_local(cfg.local_csv_dir, wm)

        all_ok = scp_ok and shots_ok
        if not all_ok:
            log.warning(
                "Sync complete (incomplete — CSV %d/%d, BMP %d/%d).",
                transferred, len(to_scp),
                shots_transferred, len(pending_shots),
            )
        elif did_work or (already_on_flashair or net_id is not None):
            log.info("Sync complete.")
        else:
            log.debug("Sync complete (no-op).")

        return not all_ok

    finally:
        # Always return to idle so the dashboard reflects "between cycles".
        # On the next cycle's first call, _status_set_stage("scanning")
        # immediately takes over.
        _status_set_stage("idle")
        if own_lock:
            release_lock(lock)


def run_daemon(resync_first: bool = False) -> None:
    """Run as a long-lived daemon, polling on a dynamic interval."""
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    # Hold the lock for the daemon's entire lifetime — avoids a lock-file
    # write every cycle and prevents cron from running simultaneously.
    lock = acquire_lock()
    if not lock:
        log.error("Another instance is already running, exiting.")
        return

    _status_init_from_disk()
    _write_status()

    poll = _poll_seconds()
    cooldown = _cooldown_minutes()
    log.info("Starting flashair-sync daemon (poll every %ds, cooldown %dm)", poll, cooldown)

    try:
        first = True
        while not _shutdown:
            needs_retry = False
            try:
                needs_retry = run(resync=(resync_first and first), _lock=lock)
                first = False
            except Exception:
                log.exception("Unexpected error during sync cycle")

            if needs_retry:
                log.info("SCP incomplete, retrying in %ds", poll)
                _interruptible_sleep(poll)
            else:
                remaining = _remaining_cooldown_seconds()
                if remaining > 0:
                    log.debug("In cooldown, sleeping %.0fs", remaining)
                    _interruptible_sleep(remaining)
                else:
                    _interruptible_sleep(poll)

        log.info("Daemon stopped.")
    finally:
        release_lock(lock)


def main():
    parser = argparse.ArgumentParser(
        description="Sync engine monitor CSVs from a FlashAir SD card"
    )
    parser.add_argument(
        "--daemon", action="store_true",
        help="Run as a long-lived daemon instead of one-shot",
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

    if args.daemon:
        run_daemon(resync_first=args.resync)
    else:
        run(resync=args.resync)


if __name__ == "__main__":
    main()
