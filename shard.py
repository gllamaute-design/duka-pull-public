"""GitHub Actions shard of the Dukascopy 1-minute pull (user 2026-10-28: split across runners, one IP each).
Takes every (ins, day) from 2020-01-01 to END not in done.txt, keeps jobs[SHARD::NSHARD], writes shard_<k>.sqlite (same schema as pull/duka_1m.py)."""
import datetime as dt, lzma, os, sqlite3, struct, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor
K, N = int(os.environ["SHARD"]), int(os.environ["NSHARD"])
INS = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "BRENTCMDUSD", "GASCMDUSD", "XPTCMDUSD"]
done = {tuple(l.split()) for l in open("done.txt") if l.strip()}
d0, d1 = dt.date(2020, 1, 1), dt.date.fromisoformat(open("end.txt").read().strip())
JOBS = [(i, d0 + dt.timedelta(days=k)) for k in range(0, (d1 - d0).days + 1) for i in INS if (i, str(d0 + dt.timedelta(days=k))) not in done][K::N]
# oldest first: other pulls work newest first, so the shards meet them without overlap
STOP = time.time() + float(os.environ.get("MAX_MIN", "330")) * 60                 # finish inside the 6 h job limit and still upload
c = sqlite3.connect("shard_%d.sqlite" % K)
c.executescript("CREATE TABLE IF NOT EXISTS m1(ins TEXT, ts INT, o INT, h INT, l INT, c INT, v REAL, PRIMARY KEY(ins, ts));"
                "CREATE TABLE IF NOT EXISTS done(ins TEXT, day TEXT, n INT, PRIMARY KEY(ins, day));")
print("shard", K, "of", N, "jobs", len(JOBS), flush=True)


def one(j):
    ins, d = j
    if time.time() > STOP:
        return ins, d, None
    u = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/BID_candles_min_1.bi5" % (ins, d.year, d.month - 1, d.day)
    for k in range(6):
        try:
            b = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"}), timeout=60).read()
            raw = lzma.decompress(b) if b else b""
            base = int(dt.datetime(d.year, d.month, d.day, tzinfo=dt.timezone.utc).timestamp())
            return ins, d, [(ins, base + r[0]) + r[1:] for r in (struct.unpack(">5if", raw[i:i + 24]) for i in range(0, len(raw) - 23, 24))]
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return ins, d, []
            time.sleep(6 * (k + 1))
        except Exception:
            time.sleep(6 * (k + 1))
    return ins, d, None


n = got = 0
with ThreadPoolExecutor(int(os.environ.get("DUKA_THREADS", "3"))) as ex:
    for ins, d, rows in ex.map(one, JOBS):
        n += 1
        if rows is None:
            continue
        got += 1
        c.executemany("INSERT OR REPLACE INTO m1 VALUES(?,?,?,?,?,?,?)", rows)
        c.execute("INSERT OR REPLACE INTO done VALUES(?,?,?)", (ins, str(d), len(rows)))
        if n % 50 == 0:
            c.commit(); print(n, "/", len(JOBS), "ok", got, ins, d, flush=True)
c.commit()
print("DONE shard", K, "ok", got, "of", len(JOBS), flush=True)
