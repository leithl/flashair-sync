# FlashAir Sync

Automatically syncs engine monitor CSV files from a **Toshiba FlashAir** SD card to a remote server via a Raspberry Pi (or anything that can run python and has wifi.)

The Pi polls for the FlashAir WiFi network every minute. When the nearby card powers on, the Pi detects it, connects, downloads new CSV files, reconnects to the original WiFi, and SCPs the files to a remote server — all hands-free.

Designed to feed into [savvy-upload](https://github.com/leithl/savvy-uploader), which then uploads the CSVs to SavvyAviation.com.

## How It Works

1. **Poll** — Cron runs the script every minute. It triggers a WiFi scan via `wpa_cli`.
2. **Detect** — If the FlashAir SSID is not visible, the script exits immediately (takes ~5 seconds).
3. **Connect** — FlashAir detected: the script adds a temporary WiFi network and connects.
4. **Download** — Lists files on the card via the FlashAir HTTP API (`command.cgi`). Downloads any CSVs newer than the watermark (`LAST_SYNCED`).
5. **Reconnect** — Removes the temporary network and reloads `wpa_supplicant.conf`. Home WiFi reconnects automatically.
6. **Transfer** — SCPs the new files to the remote server.
7. **Cleanup** — Deletes old local CSVs, keeping the 10 most recent.

A file lock prevents overlapping cron runs. If a sync takes 3 minutes, the next cron invocations exit silently until it finishes.

If the script crashes mid-sync while on FlashAir WiFi, a `try/finally` ensures it always attempts to reconnect home. On the next run, if the Pi is disconnected, it reconnects before doing anything else.

## Requirements

- Raspberry Pi with WiFi (tested on Raspberry Pi Zero W)
- Raspberry Pi OS (Bullseye or later) with `wpa_supplicant`
- Python 3.9+
- Toshiba FlashAir SD card with WiFi enabled
- SSH access to the remote server (for SCP)

## Setup

### 1. Copy files to the Pi

```bash
# On the Pi
git clone https://github.com/leithl/flashair-sync.git
```

### 2. Create Python virtual environment

If you haven't installed it yet
```bash
apt-get install python3-venv
```

```bash
cd ~/flashair-sync
python3 -m venv venv
```

No additional packages are needed — the script uses only the Python standard library.

### 3. Find the FlashAir directory

Connect to the FlashAir WiFi manually and browse to the card to find where your engine monitor stores CSV files:

```bash
# Connect to FlashAir WiFi manually first, then:
curl "http://192.168.0.1/command.cgi?op=100&DIR=/"
```

Look for the directory containing `log_YYYYMMDD_HHMMSS_KXXX.csv` files. Common locations: `/`, `/data_log/`, `/DATA/`, `/GARMIN/`. That directory path goes into `FLASHAIR_DIR` in your `.env`.

### 4. Set up SSH keys (Pi → remote server)

The Pi needs passwordless SSH access to the remote server for SCP.

```bash
# On the Pi — generate a key pair (press Enter for no passphrase):
ssh-keygen -t ed25519 -C "flashair-sync" -f ~/.ssh/id_ed25519

# Copy the public key to the remote server:
ssh-copy-id -i ~/.ssh/id_ed25519.pub USER@REMOTE_HOST

# Test the connection (should not prompt for a password):
ssh -i ~/.ssh/id_ed25519 USER@REMOTE_HOST "echo ok"
```

Replace `USER` and `REMOTE_HOST` with your actual values.

### 5. Configure .env

```bash
cd ~/flashair-sync
cp .env.example .env
nano .env
```

Fill in all required values:

```
FLASHAIR_SSID=flashair            # Your FlashAir's WiFi network name
FLASHAIR_PASSWORD=12345678        # Your FlashAir's WiFi password
FLASHAIR_DIR=/                    # Directory on the card containing CSVs
HOME_SSID=MyHomeWiFi              # Your home WiFi SSID
HOME_PASSWORD=secret              # Your home WiFi password (omit for open networks)
LOCAL_CSV_DIR=/home/pi/flashair-sync/csvs
REMOTE_HOST=192.168.1.100         # IP or hostname of the remote server
REMOTE_USER=user                  # SSH user on the remote server
REMOTE_DIR=/path/to/the/csvs      # Where to put the CSVs on the remote server
```

### 6. Test manually

```bash
cd ~/flashair-sync

# Verbose mode — shows debug output including scan results:
./flashair_cron.sh -v
```

If the FlashAir is not nearby, you should see `FlashAir 'xxx' not in range.` and the script exits. If it is nearby, you should see the full sync cycle.

### 7. Set up cron

```bash
crontab -e
```

Add this line to poll every minute:

```
* * * * * /home/pi/flashair-sync/flashair_cron.sh >> /home/pi/flashair-sync/sync.log 2>&1
```

The lock file prevents overlapping runs, so polling every minute is safe even if a sync takes several minutes.

### 8. (Optional) Verify wpa_cli permissions

The script uses `wpa_cli` to scan and switch WiFi networks. On most Raspberry Pi OS installs, the default user can run `wpa_cli` without `sudo`. If you get permission errors:

```bash
# Option A: Add your user to the netdev group
sudo usermod -aG netdev $USER
# Log out and back in

# Option B: Run cron as root
sudo crontab -e
# Add: * * * * * /home/pi/flashair-sync/flashair_cron.sh >> /home/pi/flashair-sync/sync.log 2>&1
```

## Configuration Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `FLASHAIR_SSID` | Yes | — | FlashAir WiFi network name |
| `FLASHAIR_PASSWORD` | Yes | — | FlashAir WiFi password |
| `FLASHAIR_DIR` | Yes | — | Directory on the card containing CSVs |
| `HOME_SSID` | Yes | — | Home WiFi SSID to reconnect to |
| `HOME_PASSWORD` | No | — | Home WiFi password (omit for open networks) |
| `LOCAL_CSV_DIR` | Yes | — | Local directory for downloaded CSVs |
| `REMOTE_HOST` | Yes | — | Remote server hostname or IP |
| `REMOTE_USER` | Yes | — | SSH username on remote server |
| `REMOTE_DIR` | Yes | — | Destination directory on remote server |
| `FLASHAIR_IP` | No | `192.168.0.1` | FlashAir card IP address |
| `SSH_KEY_PATH` | No | `~/.ssh/id_ed25519` | Path to SSH private key |
| `WIFI_INTERFACE` | No | `wlan0` | WiFi interface name |
| `COOLDOWN_MINUTES` | No | `30` | Minutes to wait before re-checking FlashAir |
| `LAST_SYNCED` | — | — | Managed by script. Last downloaded filename. |
| `LAST_SCPD` | — | — | Managed by script. Last SCP'd filename. |

## Troubleshooting

**Script never detects FlashAir:**
- Make sure the FlashAir card is powered on (engine monitor running or external power)
- Check that the SSID and password are correct
- Try `wpa_cli -i wlan0 scan && sleep 5 && wpa_cli -i wlan0 scan_results` manually
- The Pi's WiFi might not reach the SD card — try moving it closer

**Permission denied on wpa_cli:**
- See step 8 above (add user to `netdev` group or run as root)

**SCP fails:**
- Verify SSH key setup: `ssh -i ~/.ssh/id_ed25519 USER@HOST "echo ok"`
- Check that `REMOTE_DIR` exists on the remote server
- Check firewall rules between the Pi and the remote server

**Pi stuck on FlashAir WiFi:**
- This shouldn't happen — the script always reconnects in a `finally` block
- If it does (e.g. power loss mid-sync), either reboot or run: `wpa_cli -i wlan0 reconfigure`

**Downloads are slow or fail:**
- FlashAir WiFi is not fast — large files may take time
- The `DOWNLOAD_TIMEOUT` is 120 seconds per file; adjust in the script if needed

## Files

```
flashair_sync.py    Main script
flashair_cron.sh    Shell wrapper for cron
.env                Configuration (not in git)
.env.example        Example configuration
.gitignore          Git exclusions
.lock               Lock file (auto-created, not in git)
csvs/               Downloaded CSVs (auto-created, not in git)
venv/               Python virtual environment (not in git)
sync.log            Cron output log (not in git)
```
