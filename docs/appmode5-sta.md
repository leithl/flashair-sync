# Station-mode link (`LINK_MODE=sta`): design + card runbook

**Status: CUT OVER on the reference install, 2026-09-24** — observed results in
§12. The code side ships behind the `LINK_MODE` flag (default `ap` — zero behaviour
change until flipped). The card side requires a one-time `CONFIG` edit at a laptop,
documented in §5. Do the card edit and the `.env` flip together, per the cutover
order in §8.

> Placeholders: `<card-mac>` is the card's WiFi MAC (the hex tail of its factory
> default SSID, `flashair_XXXXXXXXXXXX`), `<ap-ssid>` / `<ap-passphrase>` are the
> local AP's credentials. IPs shown (`192.168.50.x`) are examples — substitute your
> AP's subnet.

## 1. Summary

Today the FlashAir card runs as its own WiFi access point (factory default), and the
sync host must *leave its network* to reach it: scan → join the card's AP → download
→ reconnect home. The card instead supports **station mode**: a `CONFIG` change makes
the card *join an existing AP* whenever it powers up.

If the sync host already hosts (or can reach) an AP — e.g. a Raspberry Pi running
hostapd on a `uap0` interface for other devices, as in the
[remote-switch](https://github.com/leithl/remote-switch) co-tenant setup — the card
becomes just another LAN client. The host's radio never moves. Detection collapses
from a ~60 s scan/join cycle to one HTTP probe, and the host's internet/SSH sessions
stay up through every sync.

Everything downstream of detection is unchanged: same `command.cgi?op=100` listings,
same plain-GET downloads, same watermarks, stability check, lookback rescue, SCP
phase, and cooldown.

## 2. What the AP hop costs (why bother)

With a single radio (`wlan0`) doing the hop:

- **Internet and SSH die during every sync.** Anything the host serves or tunnels
  (VPN links, monitoring, other daemons' uplinks) drops for the duration.
- **Detection is slow**: a scan pass per poll cycle, ~5 s when the card is absent,
  and a full join/DHCP dance when present.
- **Failure modes multiply**: a crash mid-hop strands the host on the card's AP
  (mitigated by `try/finally` + recovery-on-next-cycle code that exists *only*
  because of the hop).
- **The hop machinery can't be shared.** A second device that also wants the
  hop-to-device pattern would need an interface interlock with this daemon; a
  device that joins the host's AP instead needs nothing.

In station mode all of this disappears: the card is reachable at a fixed LAN IP
whenever it has power, and "is the card there?" is a 5-second-timeout HTTP GET.

## 3. Topology

Before (`LINK_MODE=ap`):

```
[home/hangar AP] ←──wlan0──  Pi  ── (hops wlan0 to) ──→ [card's own AP 192.168.0.1]
                              │
                            uap0 AP (other clients)
```

After (`LINK_MODE=sta`):

```
[home/hangar AP] ←──wlan0──  Pi ──uap0 AP (192.168.50.1)──← card as station
                                       ├─← other client(s)      (192.168.50.20,
                                       │                         static lease)
```

The card is powered only while its host device (the panel display it sits in) is
on — so "card associated to the AP" ≡ "device powered", same reachability semantics
as its own AP appearing today.

## 4. Card facts

The official flashair-developers.com documentation went offline in 2019; claims
below were verified against the community mirror
([flashair-developers.github.io](https://flashair-developers.github.io/website/)),
web.archive.org snapshots of the English docs, and the linked primary sources.

### 4.1 Which model is the card? (W-03 vs W-04)

The factory-default SSID format (`flashair_<MAC>`) is generation-generic, so the
model can't be read off the SSID. Two read-only checks during cutover:

- `curl "http://<card-ip>/command.cgi?op=108"` — firmware version string
  ([op=108 = "get firmware version"](https://flashair-developers.github.io/website/docs/api/command.cgi.html)).
  Prefix `FA9CAW3AW3.xx.xx` ⇒ **W-03**; `F15DBW3BW4.xx.xx` ⇒ **W-04**
  (prefixes community-verified; the official doc documents the scheme for W-01/W-02).
- The `VERSION=` line the card maintains in `/SD_WLAN/CONFIG` — visible during the
  §5 edit with zero extra steps.

The answer doesn't change the plan: **both W-03 and W-04 support station mode with
WPA2-PSK/AES**
([W-03 spec §5.6](https://media.digikey.com/pdf/Data%20Sheets/Toshiba%20PDFs/THNSWzzzGAA-C_Series_Spec_V3.2_2015-09-28.pdf),
[W-04 spec](https://asia.dynabook.com/storage/memory-cards/wireless-cards/flash-air-w04-sd-card/specification.php)).
One W-04-specific note: firmware before 4.00.02 has a known WPA2 client-side
(KRACK-class) vulnerability specific to STA/bridge modes
([advisory](https://apac.kioxia.com/en-apac/personal/news/2017/20171218-1.html)),
and the official updater has been discontinued. If op=108 reports an old W-04
firmware, weigh it — for a card joining a private WPA2 AP at a quiet site the
practical exposure is minimal, but record the version during cutover regardless.

### 4.2 APPMODE

From the [CONFIG documentation](https://flashair-developers.github.io/website/docs/api/config.html)
(matches the [archived English page](https://web.archive.org/web/20190916221054/https://www.flashair-developers.com/en/documents/api/config/)):

| Value | Meaning |
|---|---|
| 0 | Manual startup, AP mode |
| 2 | Manual startup, **station** mode |
| 3 | Manual startup, internet pass-thru (bridge) mode (FW 2.00.02+) |
| 4 | Auto startup, AP mode — **factory default** |
| **5** | **Auto startup, station mode** — this design |
| 6 | Auto startup, internet pass-thru mode (FW 2.00.02+) |

"Auto startup" = the WLAN comes up whenever the host device powers the card —
exactly the semantics the sync relies on today, just as a station instead of an AP.
("Manual startup" gates WLAN on a write-protect toggle of a boot image; not used
here.)

### 4.3 CONFIG parameters this design sets

All in the `[Vendor]` section of `/SD_WLAN/CONFIG`
([CONFIG doc](https://flashair-developers.github.io/website/docs/api/config.html)):

| Line | Why |
|---|---|
| `APPMODE=5` | Auto-startup station mode (§4.2). |
| `APPSSID=<ap-ssid>` | The AP to join. 1–32 chars, no spaces. |
| `APPNETWORKKEY=<ap-passphrase>` | WPA2 passphrase, 8–63 chars, no spaces. **The card rewrites this line to `********` on its next boot** and stores the real key internally — expected, not corruption. A later CONFIG rewrite that keeps the asterisks preserves the stored key; writing a new plaintext value re-sets it. |
| `STA_RETRY_CT=0` | Join-retry count, `0` = retry indefinitely (FW 3.00.00+). The default is undocumented, so set it explicitly — a briefly-down AP at card power-on then costs minutes, not the whole session. |
| `MASTERCODE=<12-hex-digits>` | Pre-claims the card. With no MASTERCODE set, the **first** `config.cgi` caller to offer one claims the card (§5.1) — and in station mode every client on the AP LAN can reach `config.cgi`. Pick any 12 hex digits, set it during the offline edit, record it privately. |

Facts that shape the rest of the design:

- **No idle timeout in station mode.** `APPAUTOTIME` (the WLAN auto-off timer)
  applies **only to AP mode** — official: "There will be no timeout … when APPMODE
  is STA or BRG". The card stays associated as long as it has power; no keep-alive
  needed.
- **IP addressing**: DHCP client by default (`DHCP_Enabled=YES`). The card sends
  **no DHCP hostname**, so discovery must be by IP — hence the static lease on the
  AP side (§6.1). (CONFIG-side static IP exists — `[WLANSD]` `IP_Address` etc.
  with `DHCP_Enabled=NO` — but the dnsmasq lease keeps address policy on the host
  and the card edit minimal.)
- **Channels 1–11 only**, 2.4 GHz 802.11b/g/n. Keep the AP inside 1–11; never
  12/13.
- **The HTTP API is mode-independent**: `command.cgi?op=100` listings and plain
  GETs behave identically in station mode (the only AP-mode-specific calls,
  op=105/op=111, aren't used here). Existing quirks carry over unchanged, e.g.
  `DIR` must not have a trailing slash — the current config (`/data_log` style
  values) already complies.
- **Host-write concurrency is also mode-independent** — the avionics writing the
  active CSV while it's fetched over HTTP is the same situation as today, so the
  stability check and lookback rescue remain exactly as necessary, no more.

## 5. Provisioning the card

### 5.1 Remote options — evaluated, rejected

Two remote paths exist; neither should be used for this cutover:

- **`config.cgi`** is the official remote-reconfiguration path
  ([doc](https://flashair-developers.github.io/website/docs/api/config.cgi.html),
  [station tutorial](https://flashair-developers.github.io/website/docs/tutorials/advanced/1.html))
  and could do the whole flip in one call —
  `config.cgi?MASTERCODE=<12hex>&APPMODE=5&APPSSID=…&APPNETWORKKEY=…` — after
  which **the card reboots itself** into station mode. On a never-configured card
  (factory SSID and default password) the first MASTERCODE offered claims the
  card. Rejected as a *remote* action because the failure mode is
  one-way: the call succeeds, the card reboots as a station, and if it then fails
  to join the AP there is no card AP left to reach it by — recovery is a physical
  visit anyway. It is also a card-side write to CONFIG while the host device has
  the FAT mounted (hazard below). If ever used, use it on site with the panel
  about to be power-cycled and a hand free to pull the card.
- **`upload.cgi`** (rewriting `/SD_WLAN/CONFIG` as a file upload) requires
  `UPLOAD=1` to already be present in CONFIG — not the case on a factory-default
  card, so the path isn't even open. Card-side CGI writes also carry a documented
  FAT-corruption hazard while a host has the filesystem mounted
  ([upload.cgi doc](https://flashair-developers.github.io/website/docs/api/upload.cgi.html):
  hosts cache the FAT; "always remove and re-insert the card" after CGI writes).

The physical edit below is offline (host device unpowered), backed up, and
trivially reversible — and since cutover verification has to happen on site
anyway (§8), it adds no extra trips.

### 5.2 Laptop runbook (host device POWERED OFF throughout)

1. Pull the card from the panel; insert it in a laptop's SD reader.
2. **Back up the entire card first**, hidden files included:
   `rsync -a /Volumes/<card-label>/ ~/flashair-backup-$(date +%Y%m%d)/` (macOS).
   Never reformat the card — a normal OS reformat deletes the hidden
   firmware/web-app files and permanently kills the WiFi features.
3. Enable hidden-file display and open `/SD_WLAN/CONFIG` (plain text, INI-style;
   the `[Vendor]` section holds the `APP*` parameters).
4. In `[Vendor]`, set/add the five lines from §4.3. Leave every other line —
   `VERSION=`, `CID=`, `LOCK=`, etc. — untouched, and preserve the file's
   existing line-ending style.
5. While in there, note the `VERSION=` value (model + firmware, §4.1).
6. Save, cleanly eject, confirm the card's side lock tab is still in the
   unlocked position (reader insertion/removal can nudge it — a locked card
   means the panel can't write new logs at all), and reinsert in the panel.
7. Verify per §8 step 5 — association, ping, `op=100` listing, and at least two
   panel power cycles — before flipping `.env`.

After the card's first station-mode boot, `APPNETWORKKEY` in CONFIG will read
`********` (§4.3). That's the success path, not damage — leave it alone.

## 6. Host-side changes

### 6.1 dnsmasq static lease

Give the card a fixed IP **below the dynamic pool** so `FLASHAIR_IP` never moves.
With a pool of `192.168.50.50–150`, add to the AP's dnsmasq config (e.g.
`/etc/dnsmasq.d/uap0.conf`):

```
dhcp-host=<card-mac>,192.168.50.20,flashair
```

`<card-mac>` is the hex tail of the card's factory SSID, colon-separated
(`flashair_a1b2c3d4e5f6` → `a1:b2:c3:d4:e5:f6`). Note that SSID-tail-equals-WiFi-MAC
is a community-observed convention, not a documented guarantee — treat it as the
starting value, and confirm the authoritative MAC from the AP side on the card's
first association (`iw dev <ap-if> station dump`, or the new `dnsmasq.leases`
entry), correcting the `dhcp-host` line if they differ (§8 step 5 covers this).
Then `sudo systemctl restart dnsmasq`. The lease name `flashair` is cosmetic but
makes `dnsmasq.leases` / `arp` output self-describing.

### 6.2 hostapd

No changes needed for a typical WPA2 AP; checks worth making:

- **Band/mode**: the card is 2.4 GHz 802.11b/g/n only — `hw_mode=g` (+ optional
  `ieee80211n=1`) is compatible. A 5 GHz-only AP will not work.
- **Security**: WPA2-PSK with CCMP (`wpa=2`, `wpa_key_mgmt=WPA-PSK`,
  `rsn_pairwise=CCMP`) is supported by the card. WPA3/SAE-only is not.
- **Client count**: the card is just one more station; hostapd's default limits are
  far above small-AP client counts.
- **Single-radio caveat**: if the AP interface is a virtual AP sharing one physical
  radio with a station-mode `wlan0` (the usual Pi `uap0` arrangement), the AP is
  pinned to whatever channel `wlan0`'s upstream network uses. If that upstream AP
  changes channel, the virtual AP — and every client on it, now including the card —
  drops until they re-agree. In particular, if the upstream association is 5 GHz,
  the virtual AP is 5 GHz too and the card can never join (it's 2.4 GHz ch 1–11
  only, §4.3). This constraint predates this design (it applies to all `uap0`
  clients); it just gains one more rider.

### 6.3 `.env`

```
LINK_MODE=sta
FLASHAIR_IP=192.168.50.20
```

`FLASHAIR_IP` is **required** in sta mode (the card is no longer at the well-known
`192.168.0.1`) — and it is the one variable a rollback must revert, back to
`192.168.0.1` (§9). `FLASHAIR_SSID`, `FLASHAIR_PASSWORD`, `HOME_PASSWORD` become
unused; `HOME_SSID` is still consulted by the uplink self-heal (rejoin `wlan0` to
the home network if it drops), so keep it set on a host whose uplink is that WiFi.
Leave them all in place for easy rollback. Watermarks (`LAST_SYNCED`,
`LAST_SCPD`, `LAST_SHOT_SCPD`) carry over untouched: same card, same filenames, so
nothing re-downloads after the switch.

Then `sudo systemctl restart flashair-sync`.

## 7. What `flashair_sync.py` does in sta mode

Implemented in this repo behind the `LINK_MODE` flag:

- **Detection** (`probe_flashair()`): one `command.cgi?op=100` GET at `FLASHAIR_IP`
  with a 5 s timeout, once per poll cycle (outside cooldown). Success ⇒ proceed
  exactly as if the AP-mode join had completed; failure ⇒ "card powered off",
  retry next cycle. No scan, no `wpa_cli`, no `reconnect_home()` in the
  post-download `finally`.
- **Self-heal kept** (when `HOME_SSID` is set): if `wlan0` has dropped off the
  home network the cycle still attempts `reconnect_home()` first — the SCP phase
  needs the uplink; the card probe is unaffected either way (different
  interface). With `HOME_SSID` unset (allowed in sta mode — e.g. an ethernet
  uplink) the self-heal is skipped: there's nothing to rejoin by name.
- **Unchanged**: watermarks, the 90 s stability check on the newest CSV, the
  lookback rescue, screenshot path, SCP retry semantics, cooldown. Cooldown remains
  worthwhile in sta mode — not for radio cost (now ~zero) but to avoid re-listing
  and stability-polling the card every 60 s while the panel stays on after a
  completed sync.
- **Status file**: gains an additive `link_mode` field. Note for consumers: in sta
  mode `current_ssid` stays parked on the home network the whole time — an
  "on FlashAir" indicator keyed off `current_ssid` should key off `stage != "idle"`
  instead.
- **Validation**: `LINK_MODE` must be `ap` or `sta`; sta requires `FLASHAIR_IP`;
  the WiFi-credential vars stop being required in sta.

A side benefit: sta mode never performs the WiFi hop, so the hop machinery —
scan, temporary network, reconnect-home in the download `finally`, the
platform-specific part of the script — goes unexercised. Note the script still
shells out to `wpa_cli` each cycle for the status-file SSID sample and (with
`HOME_SSID` set) the uplink self-heal, so a `wpa_supplicant`-managed host remains
a requirement of the main script even in sta mode.

## 8. Cutover plan

Ordered so every intermediate state is safe:

1. **Merge + deploy this repo with `LINK_MODE` unset.** Inert — the default is `ap`.
2. **Add the dnsmasq static lease** (§6.1) and restart dnsmasq. Inert — the card
   hasn't joined yet.
3. **Pre-visit, from anywhere with SSH to the host: check the AP against the
   card's envelope** (§6.2 / §4.3). `iw dev <ap-if> info` must show 2.4 GHz,
   channel 1–11 (on a shared single radio the AP is pinned to the upstream
   network's channel — a 5 GHz upstream means an AP the card can never join);
   hostapd must be WPA2-PSK/CCMP, not WPA3-only; and the AP SSID (1–32 chars) and
   passphrase (8–63 chars) must contain no spaces, or the card's CONFIG can't
   express them. Fix any mismatch before travelling.
4. **At the site, with the panel OFF: pull the card and run the §5 runbook at a
   laptop** (backup card contents, edit CONFIG, reinsert).
5. **Power the panel on. Verify from the host** before touching `.env`:
   `ping 192.168.50.20`, then
   `curl "http://192.168.50.20/command.cgi?op=100&DIR=/"`, and record the
   firmware for the log: `curl "http://192.168.50.20/command.cgi?op=108"` (§4.1).
   Power-cycle the panel at least twice and re-verify — re-association after
   power cycles is the property the whole design rests on.

   **If the ping fails**, triage from the AP side before touching the card:
   `iw dev <ap-if> station dump` (or `hostapd_cli all_sta`). A station present
   plus a `dnsmasq.leases` entry at a *different* IP means the §6.1 `dhcp-host`
   MAC was wrong (the SSID-tail heuristic missed) — fix the lease to the MAC
   shown, restart dnsmasq, power-cycle the panel, re-verify. No station at all
   means the card never associated — recheck the CONFIG lines (§4.3) and the
   step 3 AP checks. Roll back (§9) only if neither branch resolves it.
6. **Flip `.env`** (§6.3), restart the daemon, watch one full sync in the
   journal. Know what failure looks like here: **a failing probe is silent at
   the default INFO log level** (probe misses log at DEBUG), so a journal that
   shows the daemon start and then nothing within a poll interval or two means
   the probe is failing — recheck `FLASHAIR_IP` against the lease. To debug
   interactively, stop the daemon first (`sudo systemctl stop flashair-sync`)
   and run a one-shot `python3 flashair_sync.py -v` — a one-shot exits silently
   while the daemon holds the lock. To force a re-check after a completed sync,
   restart the daemon (the first cycle after start bypasses the cooldown); don't
   use `--resync` for that — it re-downloads everything.
7. **Leave the site only after** step 5's power-cycle test and step 6's full sync
   have both passed. If anything is off, run the rollback (§9) before leaving —
   a half-cut-over card strands logs.

## 9. Rollback

Station mode means the card no longer broadcasts its own AP — so if sta ever
misbehaves, **flipping `LINK_MODE=ap` back is not enough**; the card itself must be
reverted:

1. Pull the card (panel off), mount at a laptop.
2. Re-edit `/SD_WLAN/CONFIG`: `APPMODE=4`, remove `APPSSID` (the default SSID
   `flashair_<MAC>` returns), and set `APPNETWORKKEY=<old-card-password>` **in
   plaintext**. Do **not** just restore a backup whose `APPNETWORKKEY` line is
   masked (`********`): asterisks mean "keep the internally stored key", and after
   station-mode use the internally stored key is the *AP passphrase*, not the old
   card password — the card AP would come back with the wrong key.
   (`<old-card-password>` is the card's original AP password — still recorded on
   the sync host as `FLASHAIR_PASSWORD` in `.env`, which §6.3 leaves in place for
   exactly this; or the card's factory default if it was never changed.)
3. Reinsert (re-checking the lock tab, §5.2 step 6). In `.env`, set
   `LINK_MODE=ap` **and** `FLASHAIR_IP=192.168.0.1` — or delete both lines;
   those are the ap-mode defaults. Leaving `FLASHAIR_IP` at the sta-mode lease
   address would leave ap mode probing the wrong IP after every hop. Restart
   the daemon.

This is why the runbook's first step is a full-card backup, and why cutover
verification (§8 step 7) happens before leaving the site.

## 10. Risks

| Risk | Exposure | Mitigation |
|---|---|---|
| Card fails to (re)join the AP in the field | Logs stranded on card until a physical visit | §8 step 5's repeated power-cycle test before leaving; `STA_RETRY_CT=0` (§4.3); logs are never lost, only delayed — the card keeps them |
| AP briefly down when the card powers up (host rebooting) | Session delayed until the AP returns | `STA_RETRY_CT=0` makes the card retry joining indefinitely while powered, and station mode has no WLAN idle timeout (§4.3) — the loss window is only as long as the AP outage itself |
| Host writes to the card while we read over HTTP | **Unchanged from AP mode** — same firmware, same HTTP server, same concurrent-write behaviour regardless of WLAN mode | Existing stability check + lookback rescue stay in place |
| Upstream AP channel change breaks the virtual AP (single-radio) | All virtual-AP clients drop, card included | Pre-existing `uap0` constraint (§6.2); card adds no new failure mode |
| CONFIG edit typo / card doesn't come back | Card unreachable by any mode | Full-card backup first (§5); CONFIG is plain text on FAT — re-edit at any laptop; §9 |
| Card's HTTP server now exposed to all AP clients | Anyone on the AP LAN can browse/download card contents — and a card with **no** MASTERCODE could be claimed via `config.cgi` and remotely reconfigured (§5.1) | AP is WPA2 with a private passphrase and only trusted clients; `MASTERCODE` set during the §5 edit pre-claims the card (§4.3) |
| Host AP stack fails (hostapd/dnsmasq down, AP interface broken by a co-tenant config change) | New in sta mode: probe timeouts look exactly like "panel off" (DEBUG-only log), so stranded logs surface only when expected data never arrives | Before concluding "device not powered": `systemctl status hostapd dnsmasq`; `iw dev <ap-if> station dump` (other AP clients also missing ⇒ AP problem, not card); check `dnsmasq.leases` for the card's entry |
| `.env` flipped before the card CONFIG (or vice versa) | Probe times out every cycle / card joins but nobody polls | Both states are safe (no data loss, just no sync); cutover order in §8 |

## 11. Open questions

- ~~Rejoin latency after power-on is unmeasured~~ — **resolved at cutover**: the
  card was answering HTTP within seconds of associating (§12).
- ~~Card model/firmware unknown until cutover~~ — **resolved**: W-03, firmware
  3.00.01 on the reference card (§12). The W-04 advisory in §4.1 doesn't apply to it.
- Whether to shorten `COOLDOWN_MINUTES` / `POLL_SECONDS` in sta mode — probing is
  nearly free now, so fresher detection is affordable. The reference install runs
  a 3-minute cooldown without issue.
- The status-file consumer's "on FlashAir" indicator should key off
  `stage != "idle"`, not `current_ssid` (which never hops in sta mode). Consumer-side;
  the additive `link_mode` field is the hint.

## 12. Cutover record (2026-09-24)

First cutover, on a Pi Zero W whose `uap0` AP (hostapd, 2.4 GHz channel 3,
WPA2-PSK/CCMP) shares its single radio with the `wlan0` uplink.

- **Card**: `op=108` → `FA9CAW3AW3.00.01` — W-03, firmware 3.00.01.
- **MAC heuristic held**: the first association's MAC matched the factory-SSID
  tail, so the pre-staged `dhcp-host` line (§6.1) handed out the reserved IP on
  the very first join.
- **Rejoin latency**: DHCPACK within ~2 s of association, and `op=108` answering
  within ~3 s of association. Three clean joins — first station-mode boot plus the
  two §8 step 5 power cycles.
- **`STA_RETRY_CT` was not set** in this cutover (the edit changed `APPMODE` /
  `APPSSID` / `APPNETWORKKEY`; `MASTERCODE` was already present). The card's
  default retry behaviour has been fine so far. Add `STA_RETRY_CT=0` at the next
  card edit if joins ever prove flaky after a host reboot.
- **First sta-mode sync**: two closed logs (~1.4 MB) listed and downloaded in
  ~2 s; SCP succeeded on the first attempt.
- **What the hop had been costing** (measured just before cutover): each ap-mode
  sync took the `wlan0` uplink offline for ~1.7 min (join + the 90 s stability
  check + reconnect), and the first SCP after reconnecting timed out 3 times in
  2 days while the uplink's VPN re-established. Clients of the co-hosted `uap0` AP
  stayed associated through every hop — the uplink was the only casualty.

## 13. Reading the live log

In sta mode the card is reachable whenever its host device is powered, so the
still-growing CSV can be fetched ad hoc from anything on the AP's LAN. It's a
plain read — the FAT-cache hazard in §5.1 concerns card-side *writes*, not reads —
and returns whatever the host device has flushed so far. The card lists files in
directory (creation) order, so the newest is last:

```
f=$(curl -s "http://<card-ip>/command.cgi?op=100&DIR=/data_log" | tail -1 | cut -d, -f2)
curl -s "http://<card-ip>/data_log/$f" > live.csv
```

Substitute your `FLASHAIR_DIR` for `/data_log`. The daemon's own pipeline still
waits for a log to close before syncing it (stability check, §7).
