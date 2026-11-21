"""
Nifty credit-spread BACKTEST  (nifty_bot.py ni j strategy / entry / SL / target / exit rules mujab)

Aa file nifty_bot.py ni SAME folder ma muko. Bot na signal functions (supertrend, ema, rsi_wma ane tame
/newstrategy thi banavela custom strategy) ahi thi j vapray che, etle condition bilkul bot jevi j rahe.

Rules (bot mujab):
  signal UP   -> SELL (ATM +/- SELL_OFFSET) PE + BUY hedge PE   (hedge premium ~ HEDGE_PCT x sold premium)
  signal DOWN -> SELL CE + BUY hedge CE
  SL / Target (% credit | Rs | points), signal flip exit+reverse, ONE_TRADE_PER_TREND, entry/last-entry/force-exit time,
  day loss/profit limit, max trades/day  -> badhu nifty_bot.py na settings thi

IMPORTANT (option prices):
  Expired NIFTY option no historical premium data Angel One pase nathi. Etle option premium Black-Scholes model thi
  banavyo che (Nifty spot candles + India VIX / fixed IV). Result approximate che - real fills/IV skew thi thodo farak
  padse. Tame potano real option data CSV hoy to ae mujab model badli shako.

Run (examples):
  python backtest.py                                   # supertrend, 3 min, last 365 days (Angel login thi data)
  python backtest.py --strategy all --tf 3,5,15        # badhi strategy x timeframe compare
  python backtest.py --saved paper                     # bot na saved settings (bot_state.json) vapro
  python backtest.py --spot-csv nifty_3m.csv --tf 3    # offline: columns time,open,high,low,close
  python backtest.py --sl pct:40 --tp pct:60 --offset 1 --hedge-pct 0.25 --iv 13
"""
import argparse
import html
import json
import math
import os
import sys
from datetime import date, datetime, time as dtime, timedelta

import numpy as np
import pandas as pd

try:
    import nifty_bot as nb
except Exception as e:          # noqa
    raise SystemExit(f"nifty_bot.py import na thayu ({e}). backtest.py ane nifty_bot.py ek j folder ma rakho "
                     f"ane bot na requirements install karo.")

TICK = 0.05
VIX_TOKEN = "99926017"          # India VIX (NSE)
PERIODS = [("1 Week", 7), ("2 Weeks", 14), ("1 Month", 30), ("3 Months", 90),
           ("6 Months", 180), ("1 Year", 365), ("All data", None)]


# ============================ DATA (Nifty spot candles) ============================
def _fetch_chunks(token, exchange, interval, start, end, max_days):
    """Angel getCandleData ne chunk ma (interval limit mujab) kheeche."""
    out, empty = [], 0
    hi = end
    while hi > start and empty < 2:
        lo = max(start, hi - timedelta(days=max_days - 1))
        d = nb.throttled(nb.smart.getCandleData, {
            "exchange": exchange, "symboltoken": token, "interval": interval,
            "fromdate": lo.strftime("%Y-%m-%d 09:15"), "todate": hi.strftime("%Y-%m-%d 15:30")})
        rows = (d or {}).get("data") if isinstance(d, dict) else None
        if rows:
            out.extend(rows)
            empty = 0
        else:
            empty += 1
        print(f"   {lo:%Y-%m-%d} -> {hi:%Y-%m-%d}: {len(rows or [])} candles")
        hi = lo - timedelta(days=1)
    df = pd.DataFrame(out, columns=["time", "open", "high", "low", "close", "volume"])
    if len(df):
        df["time"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
    return df


def clean_spot(df):
    df = df.copy()
    df["time"] = pd.to_datetime(df["time"])
    for c in ("open", "high", "low", "close"):
        df[c] = df[c].astype(float)
    t = df["time"].dt.time
    df = df[(df["time"].dt.weekday < 5) & (t >= dtime(9, 15)) & (t < dtime(15, 30))]
    return df.drop_duplicates("time").sort_values("time").reset_index(drop=True)


def load_spot(tf, days, csv_path=None):
    if csv_path:
        return clean_spot(pd.read_csv(csv_path))
    interval, _ = nb.TIMEFRAMES[tf]
    cache = os.path.join(nb.DATA_DIR, f"nifty_spot_{tf}m.csv")
    old = clean_spot(pd.read_csv(cache)) if os.path.exists(cache) else pd.DataFrame()
    if nb.smart is None:                 # bot ma already login hoy to fari login nahi (live session na bagade)
        nb.angel_login()
    end = datetime.now()
    start = end - timedelta(days=days)
    parts = [old] if len(old) else []
    mx = nb.ANGEL_MAX_DAYS.get(tf, 60)
    if len(old):
        if old["time"].min() > start + timedelta(days=3):
            print(f"Earlier data fetch ({tf}m)...")
            parts.append(_fetch_chunks(nb.NIFTY_TOKEN, "NSE", interval, start, old["time"].min().to_pydatetime(), mx))
        if old["time"].max() < end - timedelta(hours=20):
            print(f"Newer data fetch ({tf}m)...")
            parts.append(_fetch_chunks(nb.NIFTY_TOKEN, "NSE", interval,
                                       old["time"].max().to_pydatetime(), end, mx))
    else:
        print(f"Nifty {tf}m data fetch ({days} days)...")
        parts.append(_fetch_chunks(nb.NIFTY_TOKEN, "NSE", interval, start, end, mx))
    df = clean_spot(pd.concat([p for p in parts if len(p)], ignore_index=True))
    if not len(df):
        raise SystemExit("Angel thi koi candle data nathi malyo.")
    df.to_csv(cache, index=False)
    return df[df["time"] >= pd.Timestamp(start.date())].reset_index(drop=True)


def load_vix(df, vix_csv=None, use_angel=True):
    """date -> India VIX (%). Divas na IV tarike AAGLA divas no close vaprase (lookahead na thay)."""
    m = {}
    try:
        if vix_csv:
            v = pd.read_csv(vix_csv)
            v.columns = [c.lower() for c in v.columns]
            v["date"] = pd.to_datetime(v.iloc[:, 0]).dt.date
            m = dict(zip(v["date"], v["close"].astype(float)))
        elif use_angel and nb.smart is not None:
            a, b = df["time"].min().to_pydatetime() - timedelta(days=10), datetime.now()
            d = nb.throttled(nb.smart.getCandleData, {
                "exchange": "NSE", "symboltoken": VIX_TOKEN, "interval": "ONE_DAY",
                "fromdate": a.strftime("%Y-%m-%d 09:15"), "todate": b.strftime("%Y-%m-%d 15:30")})
            rows = (d or {}).get("data") or []
            m = {pd.Timestamp(r[0]).date(): float(r[4]) for r in rows}
    except Exception as e:
        print(f"VIX data nathi malyo ({e}) -> fixed IV vaprishu")
    if not m:
        return {}
    days = sorted(m)
    return {days[i]: m[days[i - 1]] for i in range(1, len(days))}


# ============================ OPTION PRICE MODEL ============================
def _ncdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price(S, K, T, sig, r, typ):
    if T <= 0 or sig <= 0:
        return max(S - K, 0.0) if typ == "CE" else max(K - S, 0.0)
    v = sig * math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sig * sig) * T) / v
    d2 = d1 - v
    if typ == "CE":
        p = S * _ncdf(d1) - K * math.exp(-r * T) * _ncdf(d2)
    else:
        p = K * math.exp(-r * T) * _ncdf(-d2) - S * _ncdf(-d1)
    return max(p, 0.0)


def tick(p):
    return round(p / TICK) * TICK


# ============================ EXPIRY CALENDAR ============================
def make_expiry_fn(days_set, last_day, cfg):
    """Nifty weekly expiry: Thursday (31 Aug 2025 sudhi), Tuesday (1 Sep 2025 thi). Holiday hoy to aagla trading day."""
    def is_td(d):
        return (d in days_set) if d <= last_day else d.weekday() < 5

    cache = {}

    def weekly(frm):
        res = set()
        for k in range(0, 120):
            c = frm + timedelta(days=k)
            if c.weekday() == (3 if c < date(2025, 9, 1) else 1):
                e = c
                while not is_td(e):
                    e -= timedelta(days=1)
                if e >= frm:
                    res.add(e)
        return sorted(res)

    def fn(day):
        if day in cache:
            return cache[day]
        exps = weekly(day)
        if cfg["expiry_skip_today"] and len(exps) > 1 and exps[0] == day:
            exps = exps[1:]
        mode = cfg["expiry_mode"]
        if mode.startswith("m"):
            last = {}
            for e in exps:
                last[(e.year, e.month)] = e
            exps = sorted(last.values())
        cache[day] = exps[min(int(mode[1]) - 1, len(exps) - 1)]
        return cache[day]
    return fn


# ============================ SIGNALS (bot na j functions) ============================
def make_dirs(strat, g):
    """Return function dir_at(i) = i-th candle BAND thaya pachi no signal (1 up, -1 down, 0 neutral)."""
    spec = nb.STRATEGIES.get(strat, {})
    if not spec.get("custom"):
        if strat == "ema":
            f = g["close"].ewm(span=nb.EMA_FAST, adjust=False).mean()
            s = g["close"].ewm(span=nb.EMA_SLOW, adjust=False).mean()
            arr = np.sign(f - s).astype(int).values
        elif strat == "rsi_wma":
            arr = np.array(nb.rsi_wma_calc(g)[3], dtype=int)
        else:
            arr = nb.supertrend(g)["dir"].astype(int).values
        return lambda i: int(arr[i])
    cache = {}

    def dir_at(i):                       # custom: bot jem j, aaj na candles + dummy row par compute_signal
        if i not in cache:
            sub = g.iloc[:i + 1]
            sub = pd.concat([sub, sub.tail(1)], ignore_index=True)
            cache[i] = int(nb.compute_signal(strat, sub)[0])
        return cache[i]
    return dir_at


# ============================ SIMULATION ============================
def sim_day(day, g, strat, tf, cfg, iv_atm, expiry_fn):
    n = len(g)
    dt = timedelta(minutes=tf)
    st = [t.to_pydatetime() for t in g["time"]]
    en = [s + dt for s in st]
    O, H, L, C = (g[c].values.astype(float) for c in ("open", "high", "low", "close"))
    dir_at = make_dirs(strat, g)
    exp_dt = datetime.combine(expiry_fn(day), dtime(15, 30))
    T0 = datetime.combine
    entry_start, last_entry = T0(day, cfg["entry_start"]), T0(day, cfg["last_entry"])
    force = T0(day, cfg["force_exit"])
    step, r, skew = cfg["step"], cfg["rate"], cfg["skew"]
    qty = cfg["lot_size"] * cfg["lots"]
    S_ = {"pos": None, "realized": 0.0, "closed": 0, "halt": False, "blocked": None}
    out = []

    def prem(S, K, typ, t):
        T = max((exp_dt - t).total_seconds(), 60.0) / (365 * 86400.0)
        sig = (iv_atm + skew * abs(K / S - 1) * 100) / 100.0
        return bs_price(S, K, T, sig, r, typ)

    def mark(p, S, t):
        return ((p["sp"] - prem(S, p["K"], p["typ"], t)) + (prem(S, p["Kh"], p["typ"], t) - p["hp"])) * qty

    def spot_at(i, t):
        fr = min(max((t - st[i]) / dt, 0.0), 1.0)
        return O[i] + (C[i] - O[i]) * fr

    def max_trades_hit():
        return cfg["max_trades_on"] and S_["closed"] >= cfg["max_trades"]

    def open_pos(direction, t, S, bar):
        if S_["halt"]:
            return
        if max_trades_hit():
            S_["halt"] = True
            return
        typ = "PE" if direction == 1 else "CE"
        sign = -1 if typ == "PE" else 1
        atm = round(S / step) * step
        K = atm + sign * cfg["offset"] * step
        sp = tick(prem(S, K, typ, t))
        if sp <= 0:
            return
        target, best = sp * cfg["hedge_pct"], None
        for k in range(1, 50):
            Kh = K + sign * k * step
            hp = tick(prem(S, Kh, typ, t))
            if hp <= 0:
                break
            if best is None or abs(hp - target) < abs(best[1] - target):
                best = (Kh, hp)
        if best is None or sp - best[1] <= 0:
            return
        credit = sp - best[1]
        S_["pos"] = {"dir": direction, "typ": typ, "K": K, "Kh": best[0], "sp": sp, "hp": best[1],
                     "credit": credit, "t_in": t, "bar": bar, "S_in": S,
                     "sl": -nb.calc_limit(cfg["sl_mode"], cfg["sl_val"], credit, qty),
                     "tp": nb.calc_limit(cfg["tp_mode"], cfg["tp_val"], credit, qty)}

    def close(p, t, pnl, reason, S_exit):
        costs = cfg["slip"] * 4 * qty + cfg["cost"]
        out.append({"date": str(day), "weekday": day.strftime("%a"), "entry_time": p["t_in"].strftime("%H:%M"),
                    "exit_time": t.strftime("%H:%M"),
                    "duration_min": round((t - p["t_in"]).total_seconds() / 60, 1),
                    "strategy": strat, "tf": tf, "type": p["typ"], "expiry": str(exp_dt.date()),
                    "sell_strike": p["K"], "hedge_strike": p["Kh"], "entry_spot": round(p["S_in"], 1),
                    "exit_spot": round(S_exit, 1), "sell_entry": round(p["sp"], 2), "hedge_entry": round(p["hp"], 2),
                    "credit_pts": round(p["credit"], 2), "gross_pnl": round(pnl, 2), "costs": round(costs, 2),
                    "net_pnl": round(pnl - costs, 2), "net_pts": round(pnl / qty, 2), "reason": reason})
        S_["realized"] += pnl
        S_["closed"] += 1
        S_["blocked"] = p["dir"]
        S_["pos"] = None
        if cfg["day_limit_on"] and (S_["realized"] <= -cfg["day_loss"] or S_["realized"] >= cfg["day_profit"]):
            S_["halt"] = True
        if max_trades_hit():
            S_["halt"] = True

    def manage(i):
        p = S_["pos"]
        seg_s = max(st[i], p["t_in"])
        if seg_s >= force:
            S = spot_at(i, seg_s)
            return close(p, seg_s, mark(p, S, seg_s), "Time exit", S)
        seg_e = min(en[i], force)
        sl_l, sl_r, tp_l, tp_r = p["sl"], "STOP LOSS", p["tp"], "TARGET"
        if cfg["day_limit_on"]:
            ll = -cfg["day_loss"] - S_["realized"]
            pl = cfg["day_profit"] - S_["realized"]
            if ll > sl_l:
                sl_l, sl_r = ll, "DAY LOSS LIMIT"
            if pl < tp_l:
                tp_l, tp_r = pl, "DAY PROFIT TARGET"
        if p["bar"] < i:                                            # gap check (bar na open par)
            p0 = mark(p, O[i], seg_s)
            if p0 <= sl_l:
                return close(p, seg_s, p0, sl_r + " (gap)", O[i])
            if p0 >= tp_l:
                return close(p, seg_s, p0, tp_r + " (gap)", O[i])
        tm = seg_s + (seg_e - seg_s) / 2
        if p["t_in"] > st[i]:                                       # entry bar (bar ni vachhe entry): entry thi close sudhi
            lo_, hi_ = min(p["S_in"], C[i]), max(p["S_in"], C[i])
        else:
            lo_, hi_ = L[i], H[i]
        adv, fav = (lo_, hi_) if p["typ"] == "PE" else (hi_, lo_)
        hit_sl = mark(p, adv, tm) <= sl_l
        hit_tp = mark(p, fav, tm) >= tp_l
        if hit_sl and hit_tp:
            hit_sl, hit_tp = (False, True) if cfg["tp_first"] else (True, False)
        if hit_sl:
            return close(p, tm, sl_l, sl_r, adv)
        if hit_tp:
            return close(p, tm, tp_l, tp_r, fav)
        if force <= en[i]:
            S = spot_at(i, force)
            return close(p, force, mark(p, S, force), "Time exit", S)

    for i in range(n):
        d_prev = dir_at(i - 1) if i >= 1 else 0
        if d_prev != 0 and S_["blocked"] is not None and d_prev != S_["blocked"]:
            S_["blocked"] = None                                    # trend palatayo
        if S_["pos"] is None and not S_["halt"] and d_prev != 0 and i >= 1:
            t_in = max(st[i], entry_start)
            blocked = cfg["one_trade"] and S_["blocked"] == d_prev
            if not blocked and t_in < en[i] and t_in <= last_entry:
                open_pos(d_prev, t_in, spot_at(i, t_in), i)
        if S_["pos"] is not None:
            manage(i)
        if S_["pos"] is not None and cfg["exit_on_flip"]:           # candle i band thaya pachi flip check
            d_now = dir_at(i)
            p = S_["pos"]
            if d_now != 0 and d_now != p["dir"]:
                close(p, en[i], mark(p, C[i], en[i]), "Signal flip", C[i])
                if en[i] <= last_entry and i + 1 < n:
                    open_pos(d_now, en[i], C[i], i + 1)
        if S_["pos"] is None and (S_["halt"] or st[i] >= last_entry) and i >= 1:
            break
    if S_["pos"] is not None:                                       # data vahelo puro thayo
        p = S_["pos"]
        close(p, en[n - 1], mark(p, C[n - 1], en[n - 1]), "Data end", C[n - 1])
    return out


def sim_day_spot(day, g, strat, tf, cfg):
    """Strategy-only: option vagar. UP signal = Nifty long, DOWN = short. P&L Nifty points ma."""
    n = len(g)
    dt = timedelta(minutes=tf)
    st = [t.to_pydatetime() for t in g["time"]]
    en = [x + dt for x in st]
    O, H, L, C = (g[c].values.astype(float) for c in ("open", "high", "low", "close"))
    dir_at = make_dirs(strat, g)
    entry_start, last_entry = datetime.combine(day, cfg["entry_start"]), datetime.combine(day, cfg["last_entry"])
    force = datetime.combine(day, cfg["force_exit"])
    sl, tp = cfg["spot_sl"], cfg["spot_tp"]
    S_ = {"pos": None, "closed": 0, "halt": False, "blocked": None}
    out = []

    def spot_at(i, t):
        fr = min(max((t - st[i]) / dt, 0.0), 1.0)
        return O[i] + (C[i] - O[i]) * fr

    def close(p, t, pts, reason, Sx):
        out.append({"date": str(day), "weekday": day.strftime("%a"), "entry_time": p["t_in"].strftime("%H:%M"),
                    "exit_time": t.strftime("%H:%M"),
                    "duration_min": round((t - p["t_in"]).total_seconds() / 60, 1), "strategy": strat, "tf": tf,
                    "type": "UP" if p["dir"] == 1 else "DOWN", "entry_spot": round(p["S_in"], 1),
                    "exit_spot": round(Sx, 1), "gross_pnl": round(pts, 2), "costs": 0.0,
                    "net_pnl": round(pts, 2), "net_pts": round(pts, 2), "reason": reason})
        S_["closed"] += 1
        S_["blocked"] = p["dir"]
        S_["pos"] = None
        if cfg["max_trades_on"] and S_["closed"] >= cfg["max_trades"]:
            S_["halt"] = True

    def open_pos(direction, t, S, bar):
        if S_["halt"]:
            return
        if cfg["max_trades_on"] and S_["closed"] >= cfg["max_trades"]:
            S_["halt"] = True
            return
        S_["pos"] = {"dir": direction, "S_in": S, "t_in": t, "bar": bar}

    def manage(i):
        p = S_["pos"]
        seg_s = max(st[i], p["t_in"])
        if seg_s >= force:
            S = spot_at(i, seg_s)
            return close(p, seg_s, (S - p["S_in"]) * p["dir"], "Time exit", S)
        seg_e = min(en[i], force)
        if sl or tp:
            if p["bar"] < i:
                p0 = (O[i] - p["S_in"]) * p["dir"]
                if sl and p0 <= -sl:
                    return close(p, seg_s, p0, "STOP LOSS (gap)", O[i])
                if tp and p0 >= tp:
                    return close(p, seg_s, p0, "TARGET (gap)", O[i])
            lo_, hi_ = (min(p["S_in"], C[i]), max(p["S_in"], C[i])) if p["t_in"] > st[i] else (L[i], H[i])
            adv, fav = (lo_, hi_) if p["dir"] == 1 else (hi_, lo_)
            hit_sl = bool(sl) and (adv - p["S_in"]) * p["dir"] <= -sl
            hit_tp = bool(tp) and (fav - p["S_in"]) * p["dir"] >= tp
            if hit_sl and hit_tp:
                hit_sl, hit_tp = (False, True) if cfg["tp_first"] else (True, False)
            tm = st[i] + (seg_e - seg_s) / 2 if seg_e > seg_s else seg_s
            if hit_sl:
                return close(p, tm, -sl, "STOP LOSS", adv)
            if hit_tp:
                return close(p, tm, tp, "TARGET", fav)
        if force <= en[i]:
            S = spot_at(i, force)
            return close(p, force, (S - p["S_in"]) * p["dir"], "Time exit", S)

    for i in range(n):
        d_prev = dir_at(i - 1) if i >= 1 else 0
        if d_prev != 0 and S_["blocked"] is not None and d_prev != S_["blocked"]:
            S_["blocked"] = None
        if S_["pos"] is None and not S_["halt"] and d_prev != 0 and i >= 1:
            t_in = max(st[i], entry_start)
            if not (cfg["one_trade"] and S_["blocked"] == d_prev) and t_in < en[i] and t_in <= last_entry:
                open_pos(d_prev, t_in, spot_at(i, t_in), i)
        if S_["pos"] is not None:
            manage(i)
        if S_["pos"] is not None and cfg["exit_on_flip"]:
            d_now = dir_at(i)
            p = S_["pos"]
            if d_now != 0 and d_now != p["dir"]:
                close(p, en[i], (C[i] - p["S_in"]) * p["dir"], "Signal flip", C[i])
                if en[i] <= last_entry and i + 1 < n:
                    open_pos(d_now, en[i], C[i], i + 1)
        if S_["pos"] is None and (S_["halt"] or st[i] >= last_entry) and i >= 1:
            break
    if S_["pos"] is not None:
        p = S_["pos"]
        close(p, en[n - 1], (C[n - 1] - p["S_in"]) * p["dir"], "Data end", C[n - 1])
    return out


def run_combo(df, strat, tf, cfg, ivmap, expiry_fn):
    rows = []
    for day, g in df.groupby(df["time"].dt.date, sort=True):
        g = g.reset_index(drop=True)
        if len(g) < 3:
            continue
        if cfg["spot_only"]:
            rows += sim_day_spot(day, g, strat, tf, cfg)
        else:
            rows += sim_day(day, g, strat, tf, cfg, ivmap.get(day, cfg["iv"]), expiry_fn)
    return pd.DataFrame(rows)


# ============================ METRICS / REPORT ============================
def metrics(t):
    if t is None or not len(t):
        return {"trades": 0, "win": 0.0, "net": 0.0, "gross": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "pf": 0.0,
                "mdd": 0.0, "best": 0.0, "worst": 0.0, "pts": 0.0, "days": 0, "green": 0.0}
    p = t["net_pnl"]
    w, lo = p[p > 0], p[p <= 0]
    eq = np.concatenate([[0.0], p.cumsum().values])
    dd = (eq - np.maximum.accumulate(eq)).min()
    dp = t.groupby("date")["net_pnl"].sum()
    return {"trades": len(t), "win": 100.0 * len(w) / len(t), "net": p.sum(), "gross": t["gross_pnl"].sum(),
            "avg_win": w.mean() if len(w) else 0.0, "avg_loss": lo.mean() if len(lo) else 0.0,
            "pf": (w.sum() / abs(lo.sum())) if lo.sum() else float("inf"), "mdd": dd, "best": p.max(),
            "worst": p.min(), "pts": t["net_pts"].sum(), "days": len(dp), "green": 100.0 * (dp > 0).mean()}


def period_slice(t, last_day, days):
    if days is None:
        return t
    cut = str(last_day - timedelta(days=days))
    return t[t["date"] > cut]


UNIT = {"pts": False}      # strategy-only mode ma P&L Nifty points ma


def inr(x):
    if UNIT["pts"]:
        return f"{x:+,.0f} pts"
    return f"{'-' if x < 0 else ''}₹{abs(x):,.0f}"


def cls(x):
    return "pos" if x > 0 else ("neg" if x < 0 else "")


def table(headers, rows):
    h = "".join(f"<th>{html.escape(str(x))}</th>" for x in headers)
    b = "".join("<tr>" + "".join(f"<td{(' class=%s' % c) if c else ''}>{html.escape(str(v))}</td>"
                                 for v, c in r) + "</tr>" for r in rows)
    return f"<div class='sc'><table><tr>{h}</tr>{b}</table></div>"


def metric_row(label, m):
    pf = "∞" if m["pf"] == float("inf") else f"{m['pf']:.2f}"
    return [(label, ""), (m["trades"], ""), (f"{m['win']:.0f}%", ""), (inr(m["net"]), cls(m["net"])),
            (f"{m['pts']:+.0f}", cls(m["pts"])), (inr(m["avg_win"]), "pos"), (inr(m["avg_loss"]), "neg"), (pf, ""),
            (inr(m["mdd"]), "neg"), (inr(m["best"]), "pos"), (inr(m["worst"]), "neg"),
            (f"{m['green']:.0f}%", "")]


MH = ["Period", "Trades", "Win %", "Net P&L", "Net pts", "Avg win", "Avg loss", "PF", "Max DD", "Best", "Worst",
      "Green days"]


def equity_svg(t, w=760, h=170):
    if not len(t):
        return ""
    y = np.concatenate([[0.0], t["net_pnl"].cumsum().values])
    lo, hi = min(y.min(), 0), max(y.max(), 0)
    rng = (hi - lo) or 1.0
    pts = " ".join(f"{10 + (w - 20) * k / max(len(y) - 1, 1):.1f},{h - 10 - (h - 20) * (v - lo) / rng:.1f}"
                   for k, v in enumerate(y))
    zero = h - 10 - (h - 20) * (0 - lo) / rng
    return (f"<svg viewBox='0 0 {w} {h}' width='100%' style='max-width:{w}px'>"
            f"<line x1='10' x2='{w - 10}' y1='{zero:.1f}' y2='{zero:.1f}' stroke='var(--mut)' stroke-dasharray='4'/>"
            f"<polyline fill='none' stroke='var(--acc)' stroke-width='1.8' points='{pts}'/></svg>")


def combo_section(key, t, last_day, first_day):
    strat, tf = key
    label = nb.STRATEGIES.get(strat, {}).get("label", strat)
    span = (last_day - first_day).days
    s = [f"<h2>{html.escape(label)} • {tf} min</h2>"]
    rows = [metric_row(nm, metrics(period_slice(t, last_day, d))) for nm, d in PERIODS if d is None or d <= span + 3]
    s.append("<h3>Period-wise</h3>" + table(MH, rows))
    s.append("<h3>Equity curve (net, trade by trade)</h3>" + equity_svg(t))
    if len(t):
        t = t.copy()
        t["month"] = t["date"].str[:7]
        mrows = [metric_row(m, metrics(g)) for m, g in t.groupby("month")]
        s.append("<h3>Month-wise</h3>" + table(["Month"] + MH[1:], [r for r in mrows]))
        for title, col in (("By signal type (PE=UP spread, CE=DOWN spread)", "type"), ("By exit reason", "reason"),
                           ("By weekday", "weekday")):
            rr = [metric_row(str(k), metrics(g)) for k, g in t.groupby(col)]
            s.append(f"<h3>{title}</h3>" + table([col.title()] + MH[1:], rr))
    return "\n".join(s)


CSS = """
:root{--bg:#fff;--fg:#1c1c1c;--mut:#888;--acc:#2563eb;--pos:#15803d;--neg:#b91c1c;--card:#f5f5f5}
@media (prefers-color-scheme:dark){:root{--bg:#161616;--fg:#e8e8e8;--mut:#888;--acc:#60a5fa;--pos:#4ade80;--neg:#f87171;--card:#232323}}
body{font:14px/1.5 system-ui,sans-serif;background:var(--bg);color:var(--fg);max-width:1100px;margin:0 auto;padding:16px}
h1{font-size:22px}h2{margin-top:36px;border-top:1px solid var(--mut);padding-top:16px}h3{font-size:15px;margin:18px 0 6px}
.sc{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:5px 9px;text-align:right;white-space:nowrap}th:first-child,td:first-child{text-align:left}
th{background:var(--card)}tr:nth-child(even) td{background:var(--card)}
.pos{color:var(--pos)}.neg{color:var(--neg)}.note{background:var(--card);padding:10px 14px;border-radius:8px;font-size:13px}
"""


def build_report(results, cfg, last_day, first_day, outdir, notes):
    cmp_rows = []
    avail = [(nm, d) for nm, d in PERIODS if d is None or d <= (last_day - first_day).days + 3]
    for (strat, tf), t in results.items():
        r = [(nb.STRATEGIES.get(strat, {}).get("label", strat), ""), (f"{tf}m", "")]
        for nm, d in avail:
            v = metrics(period_slice(t, last_day, d))["net"]
            r.append((inr(v), cls(v)))
        cmp_rows.append(r)
    sl = nb.limit_short(cfg["sl_mode"], cfg["sl_val"])
    tp = nb.limit_short(cfg["tp_mode"], cfg["tp_val"])
    title = "Nifty Credit-Spread Backtest Report"
    warn = ("⚠️ Option premium Black-Scholes model thi (IV: " + notes + "). Real expired option data na hoy, etle result "
            "approximate che — live/paper result thi thodo alag aavse. Net P&L = gross − slippage − costs.")
    if cfg["spot_only"]:
        title = "Nifty Strategy-Only Backtest Report"
        warn = ("Aa report fakt strategy na signal ni che: Nifty spot par entry/exit (UP = long, DOWN = short), "
                "P&L Nifty POINTS ma. Option premium, slippage ke cost ganya nathi.")
    setup = (f"Data: {first_day} → {last_day} | Lots {cfg['lots']} × {cfg['lot_size']} | Sell: "
             f"{nb.sell_label(cfg['offset'])} | Hedge ≈ {cfg['hedge_pct'] * 100:.0f}% premium | SL {sl} | Target {tp} | "
             f"Expiry {cfg['expiry_mode']} | Entry {cfg['entry_start']:%H:%M}-{cfg['last_entry']:%H:%M}, "
             f"force exit {cfg['force_exit']:%H:%M} | Flip exit {'ON' if cfg['exit_on_flip'] else 'OFF'} | "
             f"1 trade/trend {'ON' if cfg['one_trade'] else 'OFF'} | Day limit {'ON' if cfg['day_limit_on'] else 'OFF'} | "
             f"Max trades {cfg['max_trades'] if cfg['max_trades_on'] else 'OFF'} | Slippage {cfg['slip']} pt/fill + "
             f"₹{cfg['cost']:.0f}/trade costs")
    if cfg["spot_only"]:
        setup = (f"STRATEGY-ONLY (option vagar) | Data: {first_day} → {last_day} | SL {cfg['spot_sl'] or 'nathi'} pts | "
                 f"Target {cfg['spot_tp'] or 'nathi'} pts | Entry {cfg['entry_start']:%H:%M}-{cfg['last_entry']:%H:%M}, "
                 f"force exit {cfg['force_exit']:%H:%M} | Flip exit {'ON' if cfg['exit_on_flip'] else 'OFF'} | "
                 f"1 trade/trend {'ON' if cfg['one_trade'] else 'OFF'} | "
                 f"Max trades {cfg['max_trades'] if cfg['max_trades_on'] else 'OFF'}")
    parts = [f"<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
             f"<title>Nifty Backtest Report</title><style>{CSS}</style></head><body>",
             f"<h1>{title}</h1>",
             f"<p class='note'>{html.escape(setup)}</p>",
             f"<p class='note'>{html.escape(warn)}</p>",
             "<h2>Comparison – net P&L (period-wise)</h2>",
             table(["Strategy", "TF"] + [nm for nm, _ in avail], cmp_rows)]
    for key, t in results.items():
        parts.append(combo_section(key, t, last_day, first_day))
    parts.append("</body></html>")
    path = os.path.join(outdir, "report.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    return path


# ============================ MAIN ============================
def parse_limit(s):
    mode, _, val = s.partition(":")
    mode = nb.UNIT_ALIASES.get(mode.lower(), mode.lower())
    if mode not in nb.MODE_INFO:
        raise SystemExit(f"Khotu limit '{s}'. Format: pct:50 | amount:2000 | points:40")
    return mode, float(val)


def build_cfg(a):
    if a.saved:                                    # bot na saved settings (bot_state.json)
        try:
            with open(nb.STATE_FILE, encoding="utf-8") as f:
                d = json.load(f)
            nb.apply_settings((d.get("profiles") or {}).get(a.saved) or d.get("settings") or {})
            print(f"Saved '{a.saved}' settings lagu karya ({nb.STATE_FILE})")
        except Exception as e:
            print(f"Saved settings vachay nahi ({e}) -> nifty_bot.py na default vapriye chhiye")
    cfg = {"lots": nb.LOTS, "lot_size": nb.LOT_SIZE, "step": nb.STRIKE_STEP, "hedge_pct": nb.HEDGE_PCT,
           "offset": nb.SELL_OFFSET, "sl_mode": nb.SL_MODE, "sl_val": nb.SL_VALUE, "tp_mode": nb.TP_MODE,
           "tp_val": nb.TP_VALUE, "exit_on_flip": nb.EXIT_ON_FLIP, "entry_start": nb.ENTRY_START,
           "last_entry": nb.LAST_ENTRY, "force_exit": nb.FORCE_EXIT, "one_trade": nb.ONE_TRADE_PER_TREND,
           "expiry_mode": nb.EXPIRY_MODE, "expiry_skip_today": nb.EXPIRY_SKIP_TODAY,
           "day_limit_on": nb.DAY_LIMIT_ON, "day_loss": nb.DAY_LOSS_LIMIT, "day_profit": nb.DAY_PROFIT_LIMIT,
           "max_trades_on": nb.MAX_TRADES_ON, "max_trades": nb.MAX_TRADES_PER_DAY,
           "iv": a.iv, "skew": a.skew, "rate": a.rate, "slip": a.slip, "cost": a.cost, "tp_first": a.tp_first}
    if a.sl:
        cfg["sl_mode"], cfg["sl_val"] = parse_limit(a.sl)
    if a.tp:
        cfg["tp_mode"], cfg["tp_val"] = parse_limit(a.tp)
    for k, v in (("lots", a.lots), ("lot_size", a.lot_size), ("offset", a.offset), ("hedge_pct", a.hedge_pct),
                 ("expiry_mode", a.expiry)):
        if v is not None:
            cfg[k] = v
    if a.no_flip_exit:
        cfg["exit_on_flip"] = False
    if a.allow_retrade:
        cfg["one_trade"] = False
    if a.day_loss is not None or a.day_profit is not None:
        cfg["day_limit_on"] = True
        cfg["day_loss"] = a.day_loss if a.day_loss is not None else cfg["day_loss"]
        cfg["day_profit"] = a.day_profit if a.day_profit is not None else cfg["day_profit"]
    if a.max_trades:
        cfg["max_trades_on"], cfg["max_trades"] = True, a.max_trades
    return cfg


class _AP(argparse.ArgumentParser):
    def error(self, message):
        raise SystemExit(f"Argument error: {message}")


def main(argv=None, in_bot=False):
    ap = _AP(description="Nifty credit-spread backtest (nifty_bot.py rules)")
    ap.add_argument("--strategy", default=nb.DEFAULT_STRATEGY, help="key (supertrend/ema/rsi_wma/custom) | all | a,b")
    ap.add_argument("--tf", default=str(nb.CANDLE_MINUTES), help="candle minutes: 1,3,5,10,15,30 (comma = ghani)")
    ap.add_argument("--days", type=int, default=365, help="ketla divas no data")
    ap.add_argument("--spot-csv", help="offline Nifty candles CSV (time,open,high,low,close) - ek j --tf sathe")
    ap.add_argument("--saved", choices=["paper", "live"], help="bot_state.json na saved settings vapro")
    ap.add_argument("--sl"), ap.add_argument("--tp")
    ap.add_argument("--lots", type=int), ap.add_argument("--lot-size", type=int, dest="lot_size")
    ap.add_argument("--offset", type=int, help="sell strike: 0 ATM, +n OTM, -n ITM")
    ap.add_argument("--hedge-pct", type=float, dest="hedge_pct")
    ap.add_argument("--expiry", choices=list(nb.EXPIRY_MODES))
    ap.add_argument("--no-flip-exit", action="store_true"), ap.add_argument("--allow-retrade", action="store_true")
    ap.add_argument("--day-loss", type=float), ap.add_argument("--day-profit", type=float)
    ap.add_argument("--max-trades", type=int)
    ap.add_argument("--iv", type=float, default=14.0, help="fixed ATM IV %% (VIX na hoy tyare)")
    ap.add_argument("--skew", type=float, default=0.5, help="IV vadharo per 1%% strike distance (vol points)")
    ap.add_argument("--rate", type=float, default=0.065)
    ap.add_argument("--slip", type=float, default=0.5, help="slippage points per fill (4 fills/trade)")
    ap.add_argument("--cost", type=float, default=100.0, help="brokerage+taxes estimate Rs per trade")
    ap.add_argument("--tp-first", action="store_true", help="same candle ma SL+Target banne aave to Target gano")
    ap.add_argument("--vix-csv"), ap.add_argument("--no-vix", action="store_true")
    ap.add_argument("--strategy-only", "--spot-only", action="store_true", dest="strategy_only",
                    help="option vagar fakt strategy report: Nifty spot par UP=long/DOWN=short, P&L points ma")
    ap.add_argument("--out", default=os.path.join(nb.DATA_DIR, "backtest_report"))
    a = ap.parse_args(argv)

    if in_bot and a.saved:
        raise SystemExit("--saved bot ma nathi chalatu (bot na atyaar na settings j vaprase).")
    if not in_bot:
        nb.load_custom_strategies()
    cfg = build_cfg(a)
    cfg["spot_only"], cfg["spot_sl"], cfg["spot_tp"] = a.strategy_only, None, None
    UNIT["pts"] = a.strategy_only
    if a.strategy_only:                      # SL/Target fakt points ma (Nifty spot points); na aapo to nahi
        for nm, v in (("spot_sl", a.sl), ("spot_tp", a.tp)):
            if v:
                mode, val = parse_limit(v)
                if mode != "points":
                    raise SystemExit("--strategy-only ma --sl/--tp fakt points ma: jem ke --sl points:40")
                cfg[nm] = val
    tfs = [int(x) for x in a.tf.split(",")]
    strats = list(nb.STRATEGIES) if a.strategy == "all" else a.strategy.split(",")
    for s in strats:
        if s not in nb.STRATEGIES:
            raise SystemExit(f"Strategy '{s}' nathi. Available: {', '.join(nb.STRATEGIES)}")
    if a.spot_csv and len(tfs) > 1:
        raise SystemExit("--spot-csv sathe ek j --tf aapo.")
    os.makedirs(a.out, exist_ok=True)

    results, first_day, last_day, ivmap, notes = {}, None, None, {}, f"fixed {a.iv:g}%"
    for tf in tfs:
        if tf not in nb.TIMEFRAMES:
            raise SystemExit(f"TF {tf} support nathi. {list(nb.TIMEFRAMES)}")
        df = load_spot(tf, a.days, a.spot_csv)
        days_set = set(df["time"].dt.date)
        first_day, last_day = min(days_set), max(days_set)
        if not ivmap and not a.no_vix and not a.strategy_only and (a.vix_csv or not a.spot_csv):
            ivmap = load_vix(df, a.vix_csv)
            if ivmap:
                notes = f"India VIX (previous close), fixed {a.iv:g}% jya VIX na hoy"
        exp_fn = make_expiry_fn(days_set, last_day, cfg)
        for s in strats:
            print(f"Running {s} @ {tf}m on {len(days_set)} days ...")
            t = run_combo(df, s, tf, cfg, ivmap, exp_fn)
            results[(s, tf)] = t
            t.to_csv(os.path.join(a.out, f"trades_{s}_{tf}m.csv"), index=False, encoding="utf-8-sig")

    path = build_report(results, cfg, last_day, first_day, a.out, notes)
    avail = [(nm, d) for nm, d in PERIODS if d is None or d <= (last_day - first_day).days + 3]
    summary = summary_text(results, last_day, first_day, avail)
    print("\nNet P&L (after slippage + costs)")
    print(f"{'Strategy':34}{'TF':>4} " + "".join(f"{nm:>12}" for nm, _ in avail))
    for (s, tf), t in results.items():
        lab = nb.STRATEGIES.get(s, {}).get("label", s)[:33]
        print(f"{lab:34}{tf:>3}m " + "".join(f"{metrics(period_slice(t, last_day, d))['net']:>12,.0f}" for _, d in avail))
    print(f"\nReport: {os.path.abspath(path)}\nTrades CSV: {os.path.abspath(a.out)}")
    return path, summary


def summary_text(results, last_day, first_day, avail):
    """Telegram mate short text summary."""
    L = [f"📊 Backtest {first_day} → {last_day} " + ("(strategy-only, Nifty points)" if UNIT["pts"]
                                                       else "(net, slippage+cost pachhi)")]
    for (s, tf), t in results.items():
        L += ["", f"{nb.STRATEGIES.get(s, {}).get('label', s)} • {tf}m"]
        for nm, d in avail:
            m = metrics(period_slice(t, last_day, d))
            L.append(f"• {nm}: {inr(m['net'])} | {m['trades']} trades | win {m['win']:.0f}% | DD {inr(m['mdd'])}")
    L += ["", "ℹ️ Strategy-only: Nifty points ma, option/cost vagar." if UNIT["pts"]
          else "⚠️ Option price Black-Scholes model thi - approximate."]
    return "\n".join(L)


def run_from_bot(argv):
    """Telegram /backtest mate (thread ma chalavo). Return (report_path, summary_text)."""
    try:
        return main(argv, in_bot=True)
    except SystemExit as e:
        raise RuntimeError(str(e.code))


if __name__ == "__main__":
    main()
