"""GitHub Actions shard of the Dukascopy TICK pull. Round 2 (2026-11-11): BTC, ETH, gold, silver gaps, WTI, copper / palladium gap.
Hourly tick files reduced to one mid price per second (last tick in the second; raw integer price, scaled later). Non-crypto skips
Saturdays (market shut); crypto runs 7 days. NEWEST hours first. Takes every (ins, day, hour) not in done_tick.txt, keeps jobs[SHARD::NSHARD],
writes tick_<k>.sqlite: s1(ins, ts, mid), done(ins, day, hour, n)."""
import datetime as dt, lzma, os, sqlite3, struct, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor
K, N = int(os.environ["SHARD"]), int(os.environ["NSHARD"])
D = dt.date; END = dt.date.today() - dt.timedelta(days=1)
RANGES = {"BTCUSD": [(D(2017, 5, 1), END)], "ETHUSD": [(D(2017, 12, 1), END)], "XAUUSD": [(D(2003, 5, 1), END)],
          "XAGUSD": [(D(2003, 5, 1), D(2019, 12, 31)), (D(2025, 2, 18), END)], "LIGHTCMDUSD": [(D(2018, 1, 1), END)],
          "COPPERCMDUSD": [(D(2025, 2, 20), END)], "XPDCMDUSD": [(D(2025, 2, 20), END)]}
CRYPTO = {"BTCUSD", "ETHUSD"}
done = {tuple(l.split()) for l in open("done_tick.txt") if l.strip()} if os.path.exists("done_tick.txt") else set()
JOBS = []
for ins, rr in RANGES.items():
    for a, b in rr:
        for k in range((b - a).days + 1):
            d = a + dt.timedelta(days=k)
            if (ins in CRYPTO or d.weekday() != 5):
                JOBS += [(ins, d, h) for h in range(24) if (ins, str(d), str(h)) not in done]
JOBS.sort(key=lambda j: (j[1], j[2], j[0]), reverse=True)                                  # newest first
JOBS = JOBS[K::N]
STOP = time.time() + float(os.environ.get("MAX_MIN", "330")) * 60
c = sqlite3.connect("tick_%d.sqlite" % K)
c.executescript("CREATE TABLE IF NOT EXISTS s1(ins TEXT, ts INT, mid INT, PRIMARY KEY(ins, ts)) WITHOUT ROWID;"
                "CREATE TABLE IF NOT EXISTS done(ins TEXT, day TEXT, hour INT, n INT, PRIMARY KEY(ins, day, hour));")
print("shard", K, "of", N, "jobs", len(JOBS), flush=True)


def one(j):
    ins, d, h = j
    if time.time() > STOP:
        return j, None
    u = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/%02dh_ticks.bi5" % (ins, d.year, d.month - 1, d.day, h)
    for k in range(int(os.environ.get("TRIES", "15"))):
        try:
            b = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=60).read()
            raw = lzma.decompress(b) if b else b""
            base = int(dt.datetime(d.year, d.month, d.day, h, tzinfo=dt.timezone.utc).timestamp())
            sec = {}
            for i in range(0, len(raw) - 19, 20):
                ms, ask, bid, _, _ = struct.unpack(">3i2f", raw[i:i + 20])
                sec[base + ms // 1000] = (ask + bid) // 2                                  # last tick in the second wins
            return j, sorted(sec.items())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return j, []
            time.sleep(6 * (k + 1))
        except Exception:
            time.sleep(6 * (k + 1))
    return j, None


n = got = 0
with ThreadPoolExecutor(int(os.environ.get("DUKA_THREADS", "3"))) as ex:
    for (ins, d, h), rows in ex.map(one, JOBS):
        n += 1
        if rows is None:
            continue
        got += 1
        c.executemany("INSERT OR REPLACE INTO s1 VALUES(?,?,?)", [(ins, t, p) for t, p in rows])
        c.execute("INSERT OR REPLACE INTO done VALUES(?,?,?,?)", (ins, str(d), h, len(rows)))
        if n % 200 == 0:
            c.commit(); print(n, "/", len(JOBS), "ok", got, ins, d, h, flush=True)
c.commit()
print("DONE shard", K, "ok", got, "of", len(JOBS), flush=True)
