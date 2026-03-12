# CLAUDE.md — flashair-sync

## What this project does

Syncs engine monitor CSV files from a Toshiba FlashAir WiFi SD card to a remote server, running on a Raspberry Pi. Detects the card's WiFi, connects, downloads new CSVs via the FlashAir HTTP API, reconnects to home WiFi, and SCPs files to a remote server — all unattended.

Downstream, [savvy-uploader](https://github.com/leithl/savvy-uploader) watches the SCP destination directory and uploads CSVs to SavvyAviation.com. The two projects share a file naming contract (`log_YYYYMMDD_HHMMSS_KXXX.csv`) and watermark pattern.

## Architecture

Three-phase sync cycle, run either as a systemd daemon (recommended) or via cron:

1. **Download** — Connect to FlashAir WiFi, list/download new CSVs via HTTP API
2. **Transfer** — SCP files to remote server (destination must match savvy-uploader's `CSV_DIR`)
3. **Cleanup** — Delete old local CSVs, keeping 10 most recent

Key mechanisms:
- **Watermarks** (`LAST_SYNCED`, `LAST_SCPD`) in `.env` track progress across restarts. Files sort lexicographically by name = chronologically.
- **Cooldown** (`.last_sync` file mtime) prevents re-scanning FlashAir for 30 min after a successful download. Only set on *complete* downloads — partial failures retry promptly.
- **Lock file** (`.lock` with `fcntl.flock`) prevents concurrent runs. Daemon holds lock for its entire lifetime.
- **Interruptible sleep** — daemon sleeps in 1-second increments so SIGTERM/SIGINT are handled promptly.

## Files

```
flashair_sync.py          Main script (Linux/RPi, tracked in git)
flashair_sync_macos.py    macOS testing variant (NOT in git, see .gitignore)
flashair_cron.sh          Shell wrapper for cron
flashair-sync.service     systemd unit file
.env.example              Configuration template
.env                      Actual config (not in git, has watermark state)
```

## Key conventions

- **stdlib only** — no pip dependencies. Uses urllib, subprocess, fcntl, pathlib, dataclasses.
- **WiFi management** — Linux uses `wpa_cli`; macOS variant uses `networksetup`. These are the only platform-specific parts.
- **macOS variant** — `flashair_sync_macos.py` is for local testing, not deployed. Keep it in sync with the main script when making changes. It is NOT tracked in git.
- **Config pattern** — env vars override `.env` file values. Required fields validated at startup.
- **.env is both config and state** — watermarks (`LAST_SYNCED`, `LAST_SCPD`) are written back to `.env` by the script. The `_write_env()` helper preserves comments and ordering.
- **Error recovery** — `try/finally` ensures WiFi reconnects after FlashAir operations. SCP failures trigger prompt retry (poll interval) instead of waiting for cooldown. SCP timeouts preserve the watermark for already-transferred files.

## Integration with savvy-uploader

The SCP destination in this project's `.env` (`REMOTE_DIR`) must be the same path as savvy-uploader's `CSV_DIR`. savvy-uploader uses `inotifywait` with a 60-second debounce to batch files before uploading. Both projects use the same watermark-based skip pattern and keep-10-recent cleanup policy.

## Development workflow

- Test locally with `flashair_sync_macos.py` on macOS, then sync changes to `flashair_sync.py`
- Deploy to Pi: `git pull` on the Pi, then `sudo systemctl restart flashair-sync`
- View daemon logs: `journalctl -u flashair-sync -f`
- The Pi runs Raspberry Pi OS with `wpa_supplicant`; the user is `pi`
- Editor preference: `vi`

## Common tasks

- **Re-download everything**: `python3 flashair_sync.py --resync`
- **Debug a cycle**: `python3 flashair_sync.py -v` (one-shot verbose mode)
- **Reset watermarks**: edit `LAST_SYNCED=` and `LAST_SCPD=` in `.env`
- **Change poll/cooldown**: edit `POLL_SECONDS` / `COOLDOWN_MINUTES` in `.env`, restart daemon
