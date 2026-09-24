#!/usr/bin/env python3
"""Smoke test for LINK_MODE=ap|sta in flashair_sync.py. Stdlib only.

Runs the real script one-shot against a mock FlashAir HTTP server on
localhost, with a stub wpa_cli on PATH so no WiFi hardware is touched.
Everything happens in a temp dir; SCP intentionally targets an
unroutable TEST-NET address to exercise the graceful-failure path.

Usage:
    python3 tests/smoke_link_modes.py

Exits 0 if all checks pass. Total runtime ~30 s (dominated by the
shortened stability check and the SCP connect timeouts).
"""
import http.server
import os
import shutil
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "flashair_sync.py"

CARD_FILES = {
    "/data_log": {
        "log_20260101_120000_KXXX.csv": b"A" * 2048,
        "log_20260102_130000_KXXX.csv": b"B" * 4096,
    },
    "/": {},
}

STA_ENV = """\
LINK_MODE=sta
FLASHAIR_IP=127.0.0.1:{port}
FLASHAIR_DIR=/data_log
LOCAL_CSV_DIR={d}/csvs
REMOTE_HOST=203.0.113.1
REMOTE_USER=nobody
REMOTE_DIR=/tmp/nowhere
LAST_SYNCED=
LAST_SCPD=
"""

AP_ENV = """\
FLASHAIR_SSID=flashair_test
FLASHAIR_PASSWORD=12345678
FLASHAIR_DIR=/data_log
HOME_SSID=TestHome
LOCAL_CSV_DIR={d}/csvs
REMOTE_HOST=203.0.113.1
REMOTE_USER=nobody
REMOTE_DIR=/tmp/nowhere
LAST_SYNCED=
LAST_SCPD=
"""

WPA_CLI_STUB = """\
#!/bin/sh
shift 2
case "$1" in
  status) echo "ssid=TestHome";;
  scan) echo "OK";;
  scan_results) printf "bssid / frequency / signal level / flags / ssid\\n";;
  *) echo "OK";;
esac
"""

# Variant: wlan0 not associated to anything (no ssid= line in status).
WPA_CLI_STUB_NOSSID = """\
#!/bin/sh
shift 2
case "$1" in
  status) echo "wpa_state=DISCONNECTED";;
  scan) echo "OK";;
  scan_results) printf "bssid / frequency / signal level / flags / ssid\\n";;
  *) echo "OK";;
esac
"""


class CardHandler(http.server.BaseHTTPRequestHandler):
    """Mock FlashAir: command.cgi?op=100 listings + plain-GET downloads."""

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/command.cgi":
            q = urllib.parse.parse_qs(parsed.query)
            d = q.get("DIR", ["/"])[0]
            files = CARD_FILES.get(d)
            if files is None:
                self.send_response(404)
                self.end_headers()
                return
            lines = ["WLANSD_FILELIST"]
            for name, data in sorted(files.items()):
                lines.append(f"{d},{name},{len(data)},32,22000,30000")
            body = "\r\n".join(lines).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        for d, files in CARD_FILES.items():
            prefix = d.rstrip("/") + "/"
            if self.path.startswith(prefix):
                name = self.path[len(prefix):]
                if name in files:
                    data = files[name]
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
        self.send_response(404)
        self.end_headers()

    def log_message(self, *args):
        pass


failures = []


def check(label, cond):
    print(f"  {'PASS' if cond else 'FAIL'}  {label}")
    if not cond:
        failures.append(label)


def setup_case(root, name, env_text, port):
    d = root / name
    (d / "csvs").mkdir(parents=True)
    shutil.copy(SRC, d / "flashair_sync.py")
    (d / ".env").write_text(env_text.format(d=d, port=port))
    return d


def run_sync(d, bin_dir, extra_env=None):
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["FLASHAIR_STABILITY_DELAY_SEC"] = "2"
    if extra_env:
        env.update(extra_env)
    p = subprocess.run(
        [sys.executable, "flashair_sync.py", "-v"],
        cwd=d, env=env, capture_output=True, text=True, timeout=180,
    )
    return p.returncode, p.stdout + p.stderr


def main():
    root = Path(tempfile.mkdtemp(prefix="flashair-smoke-"))
    bin_dir = root / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "wpa_cli"
    stub.write_text(WPA_CLI_STUB)
    stub.chmod(0o755)
    bin_nossid = root / "bin-nossid"
    bin_nossid.mkdir()
    stub2 = bin_nossid / "wpa_cli"
    stub2.write_text(WPA_CLI_STUB_NOSSID)
    stub2.chmod(0o755)

    socketserver.TCPServer.allow_reuse_address = True
    server = socketserver.TCPServer(("127.0.0.1", 0), CardHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        print("TEST 1: sta happy path (probe, download, watermark; scp fails gracefully)")
        d = setup_case(root, "t1", STA_ENV, port)
        rc, out = run_sync(d, bin_dir)
        check("exit 0", rc == 0)
        check("probe log", f"FlashAir reachable at 127.0.0.1:{port} (sta mode)." in out)
        check("found 2 CSVs", "Found 2 CSV(s) on FlashAir." in out)
        check("downloaded old", "Saved log_20260101_120000_KXXX.csv" in out)
        check("downloaded new", "Saved log_20260102_130000_KXXX.csv" in out)
        check("no radio hop", "Reconnecting to" not in out)
        check("no wifi scan", "not in range" not in out)
        envf = (d / ".env").read_text()
        check("LAST_SYNCED advanced", "LAST_SYNCED=log_20260102_130000_KXXX.csv" in envf)
        check("scp attempted + failed", ("SCP failed" in out or "SCP timed out" in out))
        check("both files local", len(list((d / "csvs").glob("*.csv"))) == 2)
        check("cooldown set (download side complete)", (d / ".last_sync").exists())

        print("TEST 2: second sta run lands in cooldown, still retries SCP")
        rc2, out2 = run_sync(d, bin_dir)
        check("exit 0", rc2 == 0)
        check("cooldown skip", "In cooldown" in out2)
        check("no re-listing", "Found 2 CSV(s)" not in out2)
        check("scp retried", ("SCP failed" in out2 or "SCP timed out" in out2))

        print("TEST 3: sta without FLASHAIR_IP fails validation")
        d3 = setup_case(root, "t3", STA_ENV.replace("FLASHAIR_IP=127.0.0.1:{port}\n", ""), port)
        rc3, out3 = run_sync(d3, bin_dir)
        check("exit 1", rc3 == 1)
        check("names FLASHAIR_IP", "FLASHAIR_IP" in out3)

        print("TEST 4: default ap mode unchanged (scan, not in range, clean exit)")
        d4 = setup_case(root, "t4", AP_ENV, port)
        rc4, out4 = run_sync(d4, bin_dir)
        check("exit 0", rc4 == 0)
        check("scanned + not in range", "'flashair_test' not in range" in out4)
        check("no sta probe", "sta mode" not in out4)

        print("TEST 5: bad LINK_MODE rejected")
        d5 = setup_case(root, "t5", STA_ENV.replace("LINK_MODE=sta", "LINK_MODE=bogus"), port)
        rc5, out5 = run_sync(d5, bin_dir)
        check("exit 1", rc5 == 1)
        check("names LINK_MODE", "LINK_MODE" in out5)

        print("TEST 6: sta with no HOME_SSID + no wlan0 association skips self-heal")
        d6 = setup_case(root, "t6", STA_ENV, port)
        rc6, out6 = run_sync(d6, bin_nossid)
        check("exit 0", rc6 == 0)
        check("no reconnect attempt", "Attempting to reconnect home" not in out6)
        check("no bogus 'Connected to'", "Connected to" not in out6)
        check("still probes + lists", "Found 2 CSV(s) on FlashAir." in out6)

        print("TEST 7: whitespace-only FLASHAIR_IP env var rejected in sta mode")
        d7 = setup_case(root, "t7", STA_ENV.replace("FLASHAIR_IP=127.0.0.1:{port}\n", ""), port)
        rc7, out7 = run_sync(d7, bin_dir, extra_env={"FLASHAIR_IP": "   "})
        check("exit 1", rc7 == 1)
        check("names FLASHAIR_IP", "FLASHAIR_IP" in out7)
    finally:
        server.shutdown()
        shutil.rmtree(root, ignore_errors=True)

    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("ALL PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
