"""
Nifty Supertrend(10,4) Credit-Spread Telegram Bot  (Paper + Angel One Live)

Supertrend GREEN -> SELL ATM PE + BUY hedge PE
Supertrend RED   -> SELL ATM CE + BUY hedge CE
Hedge strike     -> strike whose premium ~= HEDGE_PCT (30%) of ATM premium
Stop loss        -> loss  = 50% of net credit
Target           -> profit = 50% of net credit

Install:
    pip install "python-telegram-bot[job-queue]>=20" smartapi-python pyotp pandas requests logzero websocket-client

Run:
    python nifty_bot.py
Telegram commands:
    /start /run /stop /status /mode paper|live /exit /pnl
"""
import asyncio
import csv
import html
import io
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, time as dtime, timedelta

import numpy as np
import pandas as pd
import pyotp
import requests
from SmartApi import SmartConnect
try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except Exception:   # lib ma na hoy to REST fallback
    SmartWebSocketV2 = None
from telegram import (BotCommand, InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup, InputMediaPhoto,
                      MenuButtonCommands, Update)
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from dotenv import load_dotenv

load_dotenv()  # .env file mathi credentials vanche

# ----------------------------- CONFIG ---------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))  # tamaro chat id (security)

ANGEL_API_KEY = os.getenv("ANGEL_API_KEY", "")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID", "")
ANGEL_PIN = os.getenv("ANGEL_PIN", "")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "")

ST_PERIOD = 10
ST_MULT = 4.0
CANDLE_INTERVAL = "THREE_MINUTE"   # ONE_MINUTE, THREE_MINUTE, FIVE_MINUTE, FIFTEEN_MINUTE ...
CANDLE_MINUTES = 3
# minutes -> (Angel One interval, history lookback days indicators warm-up mate)
TIMEFRAMES = {
    1: ("ONE_MINUTE", 3),
    3: ("THREE_MINUTE", 6),
    5: ("FIVE_MINUTE", 8),
    10: ("TEN_MINUTE", 12),
    15: ("FIFTEEN_MINUTE", 15),
    30: ("THIRTY_MINUTE", 30),
}

LOT_SIZE = 65          # <-- Nifty current lot size verify karo
LOTS = 1
STRIKE_STEP = 50
HEDGE_PCT = 0.30       # hedge premium = 30% of ATM premium
# SL / Target: mode = "pct" (% of net credit) | "amount" (₹ total) | "points" (premium points per unit)
SL_MODE, SL_VALUE = "pct", 50.0
TP_MODE, TP_VALUE = "pct", 50.0
MODE_ORDER = ["pct", "amount", "points"]
MODE_INFO = {
    "pct": {"name": "% of credit", "step": 10, "lo": 5, "hi": 300, "default": 50.0},
    "amount": {"name": "₹ Amount", "step": 500, "lo": 100, "hi": 500000, "default": 2000.0},
    "points": {"name": "Points", "step": 5, "lo": 1, "hi": 1000, "default": 40.0},
}
UNIT_ALIASES = {"%": "pct", "pct": "pct", "percent": "pct",
                "amount": "amount", "amt": "amount", "rs": "amount", "₹": "amount",
                "point": "points", "points": "points", "pt": "points", "pts": "points"}
EXIT_ON_FLIP = True    # signal palte to exit + reverse
EMA_FAST = 9
EMA_SLOW = 21
RSI_PERIOD = 9        # RSI length
RSI_WMA_LEN = 21      # RSI nu WMA length
SMA_LEN = 5           # price SMA length
DEFAULT_STRATEGY = "supertrend"
# strategy registry: key -> (label, reference-line name). Navi strategy umervā compute_signal ma branch umero.
STRATEGIES = {
    "supertrend": {"label": f"Supertrend ({ST_PERIOD},{ST_MULT:g})", "line": "ST line"},
    "ema": {"label": f"EMA {EMA_FAST}/{EMA_SLOW} crossover", "line": f"EMA{EMA_SLOW}"},
    "rsi_wma": {"label": f"RSI{RSI_PERIOD} x WMA{RSI_WMA_LEN} + SMA{SMA_LEN}", "line": f"SMA{SMA_LEN}"},
}
PRODUCT_TYPE = "INTRADAY"

ENTRY_START = dtime(9, 20)
LAST_ENTRY = dtime(14, 45)
FORCE_EXIT = dtime(15, 15)
EXPIRY_MODE = "w1"            # kai expiry ma trade: w1/w2/w3 = weekly, m1/m2 = monthly
EXPIRY_SKIP_TODAY = False     # True: expiry day par aajni expiry chhodi ne next expiry vapro
EXPIRY_MODES = {"w1": "Weekly 1 (nearest)", "w2": "Weekly 2 (next)", "w3": "Weekly 3",
                "m1": "Monthly (current)", "m2": "Monthly (next)"}
ONE_TRADE_PER_TREND = True   # profit/loss book thaya pachi same trend ma navi trade nahi (trend flip ni rah)
HISTORY_DAYS = None          # None = auto (timeframe mujab). Indicator ma aagal na divas na candles ketla divas na levana
ANGEL_MAX_DAYS = {1: 30, 3: 60, 5: 100, 10: 100, 15: 200, 30: 200}     # Angel One history limit per interval
HISTORY_PRESETS = [2, 3, 5, 6, 8, 10, 15, 20, 30, 45, 60, 100, 200]
LOOP_SECONDS = 20
DASH_REFRESH = 2           # dashboard refresh seconds (live jevu). 2-3 sec Telegram mate safe
IMG_REFRESH = 10          # image dashboard refresh seconds (Telegram photo edit slow/limited)
IMG_CANDLES = 72          # image chart ma ketla candles
USE_WEBSOCKET = True       # Angel One live feed (tick-by-tick). False = REST polling
PNL_UPDATE_SECONDS = 30   # live PnL message kitli vaare update thay

NIFTY_TOKEN = "99926000"   # NSE Nifty 50 index
SCRIP_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
# -----------------------------------------------------------------------

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger("bot")

state = {
    "day": None,
    "blocked_dir": None,
    "img_msg_id": None,
    "img_chat": None,
    "img_t": 0.0,
    "img_busy": False,
    "strategy": DEFAULT_STRATEGY,
    "last_event": "—",
    "pnl_cache": None,
    "spot_cache": None,
    "dash_msg_id": None,
    "dash_chat": None,
    "dash_t": 0.0,
    "mode": "paper",       # paper | live
    "running": False,
    "position": None,      # dict
    "realized_pnl": 0.0,
    "last_signal_candle": None,
    "live_msg_id": None,
    "live_msg_t": 0.0,
    "trades": [],
}
smart: SmartConnect = None
scrips: pd.DataFrame = None

# ----------------------------- RATE LIMIT HELPERS ---------------------
MIN_GAP = 0.7                 # 2 Angel calls vacche minimum seconds
_api_lock = threading.Lock()
_last_call = [0.0]
_sig_cache = {"bucket": None, "val": None, "extra": ""}
_candle_cache = {"df": None}
_last_err = {"msg": "", "t": 0.0}


def throttled(fn, *args, retries=3, **kwargs):
    """Angel API call: gap rakhe ane 'access rate' error par retry kare."""
    for attempt in range(retries + 1):
        with _api_lock:
            wait = MIN_GAP - (time.time() - _last_call[0])
            if wait > 0:
                time.sleep(wait)
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                if "rate" in str(e).lower() and attempt < retries:
                    log.warning("Rate limit, retry %d", attempt + 1)
                    _last_call[0] = time.time()
                    time.sleep(2 * (attempt + 1))
                    continue
                raise
            finally:
                _last_call[0] = time.time()


# ----------------------------- ANGEL HELPERS --------------------------
def check_env():
    required = {
        "TELEGRAM_TOKEN": TELEGRAM_TOKEN if TELEGRAM_TOKEN != "YOUR_TELEGRAM_BOT_TOKEN" else "",
        "ANGEL_API_KEY": ANGEL_API_KEY,
        "ANGEL_CLIENT_ID": ANGEL_CLIENT_ID,
        "ANGEL_PIN": ANGEL_PIN,
        "ANGEL_TOTP_SECRET": ANGEL_TOTP_SECRET,
    }
    import re
    if TELEGRAM_TOKEN and not re.fullmatch(r"\d+:[A-Za-z0-9_-]+", TELEGRAM_TOKEN.strip()):
        raise SystemExit(
            "\nTELEGRAM_TOKEN no format khotu che. Check karo ke .env ma dar variable alag line ma che "
            "ane token pachi bijo koi text chotelo nathi.\n")
    missing = [k for k, v in required.items() if not v.strip()]
    if missing:
        raise SystemExit(
            "\n.env file ma aa values khali che: " + ", ".join(missing) +
            "\n.env file bot na folder ma j hovi joiye (naam bilkul '.env', '.env.txt' nahi), "
            "ane format: NAAM=value (space ke quotes vagar)\n")


def angel_login():
    global smart
    check_env()
    smart = SmartConnect(api_key=ANGEL_API_KEY)
    totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
    data = smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PIN, totp)
    if not data.get("status"):
        raise RuntimeError(f"Angel login failed: {data}")
    AUTH["jwt"] = data["data"]["jwtToken"]
    try:
        AUTH["feed"] = smart.getfeedToken()
    except Exception as e:
        log.warning("feed token nathi malyo: %s", e)
    log.info("Angel login OK")


def load_scrips():
    global scrips
    df = pd.DataFrame(requests.get(SCRIP_URL, timeout=60).json())
    df = df[(df["name"] == "NIFTY") & (df["instrumenttype"] == "OPTIDX") & (df["exch_seg"] == "NFO")].copy()
    df["strike_f"] = df["strike"].astype(float) / 100.0
    df["expiry_d"] = pd.to_datetime(df["expiry"], format="%d%b%Y")
    scrips = df
    log.info("Scrip master loaded: %d NIFTY options", len(df))


def _expiry_list():
    today = pd.Timestamp(datetime.now().date())
    exps = sorted(pd.Timestamp(e) for e in scrips["expiry_d"].unique() if pd.Timestamp(e) >= today)
    if EXPIRY_SKIP_TODAY and len(exps) > 1 and exps[0] == today:
        exps = exps[1:]                       # expiry day par next expiry
    return exps


def _monthly_list(exps):
    last = {}
    for e in exps:                            # exps sorted -> mahina no chhello = monthly expiry
        last[(e.year, e.month)] = e
    return sorted(last.values())


def select_expiry(mode=None):
    """EXPIRY_MODE mujab expiry date (pd.Timestamp). Jetli expiry na hoy to chhelli available."""
    mode = mode or EXPIRY_MODE
    exps = _expiry_list()
    lst = _monthly_list(exps) if mode.startswith("m") else exps
    return lst[min(int(mode[1]) - 1, len(lst) - 1)]


def nearest_expiry():
    return select_expiry()


def expiry_label(mode=None) -> str:
    mode = mode or EXPIRY_MODE
    try:
        return f"{EXPIRY_MODES[mode]} • {select_expiry(mode):%d %b %Y}"
    except Exception:
        return EXPIRY_MODES.get(mode, mode)


def find_option(strike: float, opt_type: str, expiry):
    r = scrips[(scrips["expiry_d"] == expiry) & (scrips["strike_f"] == float(strike))
               & (scrips["symbol"].str.endswith(opt_type))]
    if r.empty:
        raise ValueError(f"Option not found {strike}{opt_type}")
    row = r.iloc[0]
    return {"symbol": row["symbol"], "token": row["token"], "strike": strike, "type": opt_type}


def ltp(exchange, symbol, token) -> float:
    d = throttled(smart.ltpData, exchange, symbol, token)
    if not d.get("status"):
        raise RuntimeError(f"LTP error {d}")
    return float(d["data"]["ltp"])


def nifty_spot() -> float:
    return ltp("NSE", "Nifty 50", NIFTY_TOKEN)


def lookback_days() -> int:
    """Indicator ma ketla divas no data levo (aagal na divas na candles pan). Angel limit mujab clamp."""
    d = HISTORY_DAYS or TIMEFRAMES.get(CANDLE_MINUTES, (None, 6))[1]
    return int(min(d, ANGEL_MAX_DAYS.get(CANDLE_MINUTES, 30)))


def data_txt() -> str:
    df0 = _candle_cache["df"]
    if df0 is None or len(df0) == 0:
        return "-"
    return f"{len(df0)} candles • {df0['time'].iloc[0]:%d %b} → now"


def get_candles() -> pd.DataFrame:
    now = datetime.now()
    frm = (now - timedelta(days=lookback_days())).strftime("%Y-%m-%d 09:15")
    to = now.strftime("%Y-%m-%d %H:%M")
    d = throttled(smart.getCandleData, {"exchange": "NSE", "symboltoken": NIFTY_TOKEN,
                                        "interval": CANDLE_INTERVAL, "fromdate": frm, "todate": to})
    if not d.get("status") or not d.get("data"):
        raise RuntimeError(f"Candle error {d}")
    df = pd.DataFrame(d["data"], columns=["time", "open", "high", "low", "close", "volume"])
    df["time"] = pd.to_datetime(df["time"]).dt.tz_localize(None)
    return df


def place_order(opt, side, qty) -> str:
    """Live order. Paper mode ma call nathi thatu."""
    params = {
        "variety": "NORMAL", "tradingsymbol": opt["symbol"], "symboltoken": opt["token"],
        "transactiontype": side, "exchange": "NFO", "ordertype": "MARKET",
        "producttype": PRODUCT_TYPE, "duration": "DAY", "price": "0", "squareoff": "0",
        "stoploss": "0", "quantity": str(qty),
    }
    oid = throttled(smart.placeOrder, params, retries=0)
    log.info("Order %s %s %s -> %s", side, opt["symbol"], qty, oid)
    return str(oid)


# ----------------------------- PERSISTENCE ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "bot_state.json")        # restart par state pacho aave
HISTORY_FILE = os.path.join(BASE_DIR, "trades_history.csv")  # badha divas na trades (kayam)
HISTORY_COLS = ["date", "entry_time", "exit_time", "duration_min", "mode", "strategy", "timeframe", "type",
                "expiry", "lots", "qty", "entry_spot", "exit_spot",
                "sell_symbol", "sell_entry", "sell_exit", "sell_pnl",
                "hedge_symbol", "hedge_entry", "hedge_exit", "hedge_pnl",
                "net_credit_pts", "net_exit_pts", "net_points", "pnl", "sl_rule", "tp_rule", "reason"]
_last_save = [0.0]
restore_notes = []


def save_state():
    try:
        data = {
            "day": str(state["day"]) if state["day"] else str(datetime.now().date()),
            "mode": state["mode"],
            "strategy": state["strategy"],
            "blocked_dir": state["blocked_dir"],
            "realized_pnl": state["realized_pnl"],
            "trades": state["trades"],
            "position": state["position"],
            "last_event": state["last_event"],
            "settings": {"LOTS": LOTS, "HEDGE_PCT": HEDGE_PCT, "SL_MODE": SL_MODE, "SL_VALUE": SL_VALUE,
                         "TP_MODE": TP_MODE, "TP_VALUE": TP_VALUE, "EXIT_ON_FLIP": EXIT_ON_FLIP,
                         "ENTRY_START": ENTRY_START.strftime("%H:%M"), "LAST_ENTRY": LAST_ENTRY.strftime("%H:%M"),
                         "FORCE_EXIT": FORCE_EXIT.strftime("%H:%M"),
                         "ONE_TRADE_PER_TREND": ONE_TRADE_PER_TREND, "HISTORY_DAYS": HISTORY_DAYS,
                         "EXPIRY_MODE": EXPIRY_MODE, "EXPIRY_SKIP_TODAY": EXPIRY_SKIP_TODAY,
                         "CANDLE_MINUTES": CANDLE_MINUTES},
        }
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1, default=str)
        os.replace(tmp, STATE_FILE)
        _last_save[0] = time.time()
    except Exception as e:
        log.warning("save_state failed: %s", e)


def _normalize_row(r: dict) -> dict:
    if not r.get("exit_time") and r.get("time"):         # juna format: "time" = exit time
        r["exit_time"] = r["time"]
    return r


def append_history(t: dict):
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, newline="", encoding="utf-8-sig") as f:
                rd = csv.DictReader(f)
                old_cols, old_rows = rd.fieldnames or [], list(rd)
            if old_cols != HISTORY_COLS:                  # juna format -> navi columns ma migrate (data jalvay che)
                with open(HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=HISTORY_COLS, extrasaction="ignore", restval="")
                    w.writeheader()
                    w.writerows(_normalize_row(r) for r in old_rows)
        new = not os.path.exists(HISTORY_FILE)
        with open(HISTORY_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=HISTORY_COLS, extrasaction="ignore", restval="")
            if new:
                w.writeheader()
            w.writerow(t)
    except Exception as e:
        log.warning("history write failed: %s", e)


def read_history():
    if not os.path.exists(HISTORY_FILE):
        return []
    with open(HISTORY_FILE, newline="", encoding="utf-8-sig") as f:
        return [_normalize_row(r) for r in csv.DictReader(f)]


def load_state():
    global LOTS, HEDGE_PCT, SL_MODE, SL_VALUE, TP_MODE, TP_VALUE, EXIT_ON_FLIP
    global ENTRY_START, LAST_ENTRY, FORCE_EXIT, ONE_TRADE_PER_TREND, HISTORY_DAYS, EXPIRY_MODE, EXPIRY_SKIP_TODAY
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            d = json.load(f)
    except Exception as e:
        log.warning("state file vachay nahi: %s", e)
        return
    today = datetime.now().date()
    st = d.get("settings", {})
    LOTS = int(st.get("LOTS", LOTS))
    HEDGE_PCT = float(st.get("HEDGE_PCT", HEDGE_PCT))
    if st.get("SL_MODE") in MODE_INFO and st.get("TP_MODE") in MODE_INFO:
        SL_MODE, SL_VALUE = st["SL_MODE"], float(st.get("SL_VALUE", SL_VALUE))
        TP_MODE, TP_VALUE = st["TP_MODE"], float(st.get("TP_VALUE", TP_VALUE))
    elif "SL_PCT" in st:                     # juna version nu state file
        SL_MODE, SL_VALUE = "pct", float(st["SL_PCT"]) * 100
        TP_MODE, TP_VALUE = "pct", float(st.get("TARGET_PCT", 0.5)) * 100
    EXIT_ON_FLIP = bool(st.get("EXIT_ON_FLIP", EXIT_ON_FLIP))

    def _t(key, cur):
        try:
            return datetime.strptime(st[key], "%H:%M").time()
        except Exception:
            return cur
    ENTRY_START, LAST_ENTRY, FORCE_EXIT = _t("ENTRY_START", ENTRY_START), _t("LAST_ENTRY", LAST_ENTRY), _t("FORCE_EXIT", FORCE_EXIT)
    ONE_TRADE_PER_TREND = bool(st.get("ONE_TRADE_PER_TREND", ONE_TRADE_PER_TREND))
    HISTORY_DAYS = int(st["HISTORY_DAYS"]) if st.get("HISTORY_DAYS") else None
    if st.get("EXPIRY_MODE") in EXPIRY_MODES:
        EXPIRY_MODE = st["EXPIRY_MODE"]
    EXPIRY_SKIP_TODAY = bool(st.get("EXPIRY_SKIP_TODAY", EXPIRY_SKIP_TODAY))
    tfm = int(st.get("CANDLE_MINUTES", CANDLE_MINUTES))
    if tfm in TIMEFRAMES:
        set_timeframe(tfm)
    state["mode"] = d.get("mode", "paper")
    if d.get("strategy") in STRATEGIES:
        state["strategy"] = d["strategy"]
    state["last_event"] = d.get("last_event", "—")
    state["day"] = today
    if d.get("day") == str(today):
        state["trades"] = d.get("trades", [])
        state["blocked_dir"] = d.get("blocked_dir")
        state["realized_pnl"] = float(d.get("realized_pnl", 0))
        if state["trades"]:
            restore_notes.append(f"📅 Aajna {len(state['trades'])} trades restore thaya "
                                 f"(Realized ₹{state['realized_pnl']:,.0f})")
    pos = d.get("position")
    if pos:
        state["position"] = pos
        ws_wanted.setdefault(2, set()).update([pos["sell"]["token"], pos["hedge"]["token"]])
        note = (f"📌 Open position restore thayi: {pos['type']} spread "
                f"({pos['sell']['symbol']}) • mode {pos.get('mode', state['mode']).upper()}\n"
                f"SL/Target monitoring chalu che.")
        if d.get("day") != str(today):
            note += "\n⚠️ Aa juna divas ni position che, Angel One ma verify karo!"
        if pos.get("mode") == "live":
            note += "\n⚠️ LIVE position: Angel One ma verify karo."
        restore_notes.append(note)
    log.info("State restored from %s", STATE_FILE)


# ----------------------------- WEBSOCKET LIVE FEED --------------------
AUTH = {"jwt": None, "feed": None}
ws_prices = {}                      # token -> (ltp, timestamp)
ws_wanted = {1: {NIFTY_TOKEN}}      # exchangeType (1=NSE, 2=NFO) -> tokens
ws_ctl = {"sws": None, "open": False, "started": False}


def ws_ltp(token, max_age=5):
    v = ws_prices.get(str(token))
    if v and time.time() - v[1] <= max_age:
        return v[0]
    return None


def ws_subscribe(exch_type, tokens):
    ws_wanted.setdefault(exch_type, set()).update(tokens)
    sws = ws_ctl["sws"]
    if sws and ws_ctl["open"]:
        try:
            sws.subscribe("bot", 1, [{"exchangeType": exch_type, "tokens": list(tokens)}])
        except Exception as e:
            log.warning("WS subscribe failed: %s", e)


def ws_unsubscribe(exch_type, tokens):
    ws_wanted.get(exch_type, set()).difference_update(tokens)
    sws = ws_ctl["sws"]
    if sws and ws_ctl["open"]:
        try:
            sws.unsubscribe("bot", 1, [{"exchangeType": exch_type, "tokens": list(tokens)}])
        except Exception as e:
            log.warning("WS unsubscribe failed: %s", e)


def _ws_thread():
    while True:
        try:
            sws = SmartWebSocketV2(AUTH["jwt"], ANGEL_API_KEY, ANGEL_CLIENT_ID, AUTH["feed"])

            def on_open(wsapp):
                ws_ctl["open"] = True
                log.info("WS connected (live feed chalu)")
                tl = [{"exchangeType": et, "tokens": list(tk)} for et, tk in ws_wanted.items() if tk]
                sws.subscribe("bot", 1, tl)

            def on_data(wsapp, message):
                try:
                    tok = str(message.get("token", "")).strip('"\x00 ')
                    px = message.get("last_traded_price")
                    if tok and px is not None:
                        ws_prices[tok] = (float(px) / 100.0, time.time())
                except Exception:
                    pass

            def on_error(wsapp, error):
                log.warning("WS error: %s", error)

            def on_close(wsapp, *a):
                ws_ctl["open"] = False
                log.warning("WS closed")

            sws.on_open, sws.on_data, sws.on_error, sws.on_close = on_open, on_data, on_error, on_close
            ws_ctl["sws"] = sws
            sws.connect()          # blocking
        except Exception as e:
            log.warning("WS crashed: %s", e)
        ws_ctl["open"] = False
        time.sleep(5)              # reconnect


def start_ws():
    if not USE_WEBSOCKET or SmartWebSocketV2 is None or ws_ctl["started"]:
        log.info("WebSocket band/unavailable, REST polling vaparse")
        return
    if not AUTH["jwt"] or not AUTH["feed"]:
        log.warning("WS mate token nathi, REST vaparse")
        return
    ws_ctl["started"] = True
    threading.Thread(target=_ws_thread, daemon=True).start()


def ws_pnl():
    """Websocket prices thi PnL (REST call vagar). Fresh na hoy to None."""
    p = state["position"]
    if not p:
        return None
    s_ = ws_ltp(p["sell"]["token"])
    h_ = ws_ltp(p["hedge"]["token"])
    if s_ is None or h_ is None:
        return None
    pnl = ((p["sell_entry"] - s_) + (h_ - p["hedge_entry"])) * p["qty"]
    return pnl, s_, h_


# ----------------------------- SUPERTREND -----------------------------
def supertrend(df: pd.DataFrame, period=ST_PERIOD, mult=ST_MULT) -> pd.DataFrame:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()   # Wilder RMA (TradingView jevu)
    hl2 = (h + l) / 2
    ub, lb = (hl2 + mult * atr).values, (hl2 - mult * atr).values
    fub, flb = ub.copy(), lb.copy()
    close = c.values
    direction = [1] * len(df)   # 1 = green (uptrend), -1 = red
    for i in range(1, len(df)):
        fub[i] = ub[i] if (ub[i] < fub[i - 1] or close[i - 1] > fub[i - 1]) else fub[i - 1]
        flb[i] = lb[i] if (lb[i] > flb[i - 1] or close[i - 1] < flb[i - 1]) else flb[i - 1]
        if direction[i - 1] == -1 and close[i] > fub[i - 1]:
            direction[i] = 1
        elif direction[i - 1] == 1 and close[i] < flb[i - 1]:
            direction[i] = -1
        else:
            direction[i] = direction[i - 1]
    df = df.copy()
    df["dir"] = direction
    df["st"] = [flb[i] if direction[i] == 1 else fub[i] for i in range(len(df))]
    return df


def strategy_label() -> str:
    return STRATEGIES[state["strategy"]]["label"]


def set_timeframe(minutes: int):
    global CANDLE_INTERVAL, CANDLE_MINUTES
    CANDLE_MINUTES = minutes
    CANDLE_INTERVAL = TIMEFRAMES[minutes][0]
    _sig_cache["val"], _sig_cache["bucket"], _sig_cache["extra"] = None, None, ""


def trend_name(d: int) -> str:
    return {1: "🟢 BULLISH (UP)", -1: "🔴 BEARISH (DOWN)"}.get(d, "⚪ NEUTRAL")


def plan_text(d: int, atm: float) -> str:
    if d == 0:
        return "Wait (no trade)"
    if ONE_TRADE_PER_TREND and state["blocked_dir"] == d and not state["position"]:
        return "Wait for trend flip"
    return f"SELL {atm:.0f} {'PE' if d == 1 else 'CE'} + hedge"


def strategy_line() -> str:
    return STRATEGIES[state["strategy"]]["line"]


def rsi_wma_calc(df: pd.DataFrame):
    """RSI(9) (Wilder), RSI nu WMA(21), close nu SMA(5) ane carry-forward direction list."""
    c = df["close"]
    delta = c.diff()
    avg_up = delta.clip(lower=0).ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
    avg_dn = (-delta.clip(upper=0)).ewm(alpha=1 / RSI_PERIOD, adjust=False).mean()
    rsi = 100 - 100 / (1 + avg_up / avg_dn)
    rsi = rsi.mask(avg_dn == 0, 100.0).mask((avg_dn == 0) & (avg_up == 0), 50.0)
    w = np.arange(1, RSI_WMA_LEN + 1)
    wma = rsi.rolling(RSI_WMA_LEN).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)
    sma = c.rolling(SMA_LEN).mean()
    bull = ((rsi > wma) & (c > sma)).values      # RSI > WMA  AND  close > SMA5
    bear = ((rsi < wma) & (c < sma)).values      # RSI < WMA  AND  close < SMA5
    dirs, cur = [], 0
    for b_, r_ in zip(bull, bear):               # vachhe ni (mixed) candle ma pehlano trend chalu
        if b_:
            cur = 1
        elif r_:
            cur = -1
        dirs.append(cur)
    return rsi, wma, sma, dirs


def compute_signal(strategy: str, df: pd.DataFrame):
    """Return (direction, candle_time, reference_line). direction: 1 = bullish, -1 = bearish.
    Last COMPLETED candle (iloc[-2]) par j signal."""
    _sig_cache["extra"] = ""
    if strategy == "ema":
        fast = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
        slow = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
        d = 1 if fast.iloc[-2] > slow.iloc[-2] else -1
        return d, df["time"].iloc[-2], float(slow.iloc[-2])
    if strategy == "rsi_wma":
        c = df["close"]
        rsi, wma, sma, dirs = rsi_wma_calc(df)
        i = -2                                       # last COMPLETED candle
        _sig_cache["extra"] = (f"RSI{RSI_PERIOD} {rsi.iloc[i]:.1f} {'>' if rsi.iloc[i] > wma.iloc[i] else '<'} "
                               f"WMA{RSI_WMA_LEN} {wma.iloc[i]:.1f} | Close {c.iloc[i]:,.1f} "
                               f"{'>' if c.iloc[i] > sma.iloc[i] else '<'} SMA{SMA_LEN} {sma.iloc[i]:,.1f}")
        return dirs[i], df["time"].iloc[i], float(sma.iloc[i])
    # ---- default: supertrend
    d2 = supertrend(df)
    row = d2.iloc[-2]
    return int(row["dir"]), row["time"], float(row["st"])


def current_signal():
    """Last COMPLETED candle no supertrend direction. Navi candle bane tyare j fetch."""
    now = datetime.now()
    open_t = now.replace(hour=9, minute=15, second=0, microsecond=0)
    secs = (now - open_t).total_seconds()
    bucket = (state["strategy"], now.date(), int(secs // (60 * CANDLE_MINUTES)))
    # candle band thaya pachi 5 sec ni rah joi ne fetch
    ready = secs % (60 * CANDLE_MINUTES) >= 5
    if _sig_cache["val"] is not None and (_sig_cache["bucket"] == bucket or not ready):
        return _sig_cache["val"]
    df_ = get_candles()
    _candle_cache["df"] = df_            # image chart mate
    val = compute_signal(state["strategy"], df_)
    _sig_cache["bucket"], _sig_cache["val"] = bucket, val
    return val


# ----------------------------- STRATEGY -------------------------------
def limit_short(mode, value) -> str:
    return {"pct": f"{value:g}%", "amount": f"₹{value:,.0f}", "points": f"{value:g} pts"}[mode]


def limit_text(mode, value) -> str:
    if mode == "pct":
        return f"{value:g}% of net credit"
    if mode == "amount":
        return f"₹{value:,.0f} (total)"
    return f"{value:g} points (≈ ₹{value * LOT_SIZE * LOTS:,.0f} at {LOTS} lot)"


def calc_limit(mode, value, credit_unit, qty) -> float:
    """SL/Target ni rakam ₹ ma. credit_unit = net credit per unit (points)."""
    if mode == "pct":
        return value / 100 * credit_unit * qty
    if mode == "amount":
        return value
    return value * qty          # points: spread premium ma points x qty


def pick_hedge(opt_type, atm, atm_prem, expiry):
    """Binary search: strike jeno premium ~ HEDGE_PCT * atm_prem."""
    target = atm_prem * HEDGE_PCT
    sign = -1 if opt_type == "PE" else 1
    lo, hi = 1, 80   # steps away from ATM
    best = None
    while lo <= hi:
        mid = (lo + hi) // 2
        strike = atm + sign * mid * STRIKE_STEP
        try:
            o = find_option(strike, opt_type, expiry)
            p = ltp("NFO", o["symbol"], o["token"])
        except Exception:
            hi = mid - 1
            continue
        if best is None or abs(p - target) < abs(best[1] - target):
            best = (o, p)
        if p > target:
            lo = mid + 1
        else:
            hi = mid - 1
        if abs(p - target) / target < 0.05:
            break
    if best is None:
        raise RuntimeError("Hedge strike nathi malyo")
    return best


async def open_position(direction: int, app: Application):
    opt_type = "PE" if direction == 1 else "CE"
    spot = await asyncio.to_thread(nifty_spot)
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    expiry = nearest_expiry()
    qty = LOT_SIZE * LOTS

    sell_opt = find_option(atm, opt_type, expiry)
    sell_px = await asyncio.to_thread(ltp, "NFO", sell_opt["symbol"], sell_opt["token"])
    hedge_opt, hedge_px = await asyncio.to_thread(pick_hedge, opt_type, atm, sell_px, expiry)

    credit = sell_px - hedge_px  # per unit
    if credit <= 0:
        await notify(app, "Net credit <= 0, entry skip.")
        return

    if state["mode"] == "live":
        # pehla hedge BUY (margin benefit), pachi SELL
        await asyncio.to_thread(place_order, hedge_opt, "BUY", qty)
        await asyncio.to_thread(place_order, sell_opt, "SELL", qty)

    state["live_msg_id"] = None
    state["live_msg_t"] = 0.0
    state["position"] = {
        "dir": direction, "type": opt_type, "qty": qty, "expiry": str(expiry.date()),
        "sell": sell_opt, "sell_entry": sell_px,
        "hedge": hedge_opt, "hedge_entry": hedge_px,
        "credit": credit,
        "sl": -calc_limit(SL_MODE, SL_VALUE, credit, qty),
        "tp": calc_limit(TP_MODE, TP_VALUE, credit, qty),
        "sl_txt": limit_text(SL_MODE, SL_VALUE), "tp_txt": limit_text(TP_MODE, TP_VALUE),
        "time": datetime.now().strftime("%H:%M:%S"), "mode": state["mode"], "spot": spot,
        "lots": LOTS, "strategy": strategy_label(), "tf": CANDLE_MINUTES,
        "entry_dt": datetime.now().isoformat(timespec="seconds"),
    }
    ws_subscribe(2, [sell_opt["token"], hedge_opt["token"]])
    state["last_event"] = f"{datetime.now():%H:%M} ENTRY {opt_type} spread"
    save_state()
    await notify(app,
        f"✅ ENTRY [{state['mode'].upper()}] {strategy_label()} {'UP 🟢' if direction == 1 else 'DOWN 🔴'}\n"
        f"SELL {sell_opt['symbol']} @ {sell_px:.2f}\n"
        f"BUY  {hedge_opt['symbol']} @ {hedge_px:.2f}\n"
        f"Expiry: {expiry:%d %b %Y} ({EXPIRY_MODES[EXPIRY_MODE].split(' (')[0]})\n"
        f"Lots: {LOTS} | Qty: {qty} | Net credit/unit: {credit:.2f}\n"
        f"🛡 SL: -₹{abs(state['position']['sl']):,.0f} ({state['position']['sl_txt']})\n"
        f"🎯 Target: +₹{state['position']['tp']:,.0f} ({state['position']['tp_txt']})")


async def position_pnl():
    p = state["position"]
    s_ = ws_ltp(p["sell"]["token"])
    h_ = ws_ltp(p["hedge"]["token"])
    if s_ is None:
        s_ = await asyncio.to_thread(ltp, "NFO", p["sell"]["symbol"], p["sell"]["token"])
    if h_ is None:
        h_ = await asyncio.to_thread(ltp, "NFO", p["hedge"]["symbol"], p["hedge"]["token"])
    pnl = ((p["sell_entry"] - s_) + (h_ - p["hedge_entry"])) * p["qty"]
    return pnl, s_, h_


exit_lock = asyncio.Lock()


async def close_position(app: Application, reason: str):
    async with exit_lock:      # fast loop + tick ek sathe double exit na kare
        await _close_position(app, reason)


async def _close_position(app: Application, reason: str):
    p = state["position"]
    if not p:
        return
    pnl, s, h = await position_pnl()
    mode = p.get("mode", state["mode"])
    if mode == "live":
        # pehla sell leg buy back, pachi hedge sell
        await asyncio.to_thread(place_order, p["sell"], "BUY", p["qty"])
        await asyncio.to_thread(place_order, p["hedge"], "SELL", p["qty"])
    state["realized_pnl"] += pnl
    try:
        exit_spot = await spot_cached(5)
    except Exception:
        exit_spot = None
    trade = build_trade_record(p, s, h, pnl, reason, mode, exit_spot)
    state["trades"].append(trade)
    append_history(trade)
    state["last_event"] = f"{datetime.now():%H:%M} {reason} (₹{pnl:,.0f})"
    ws_unsubscribe(2, [p["sell"]["token"], p["hedge"]["token"]])
    state["position"] = None
    state["live_msg_id"] = None
    state["blocked_dir"] = p.get("dir")      # same trend ma navi trade nahi (jyaan sudhi trend flip na thay)
    save_state()
    extra = ""
    if ONE_TRADE_PER_TREND and "flip" not in reason.lower():
        extra = "\n⏸ Same trend ma navi trade nahi, trend flip ni rah."
    await notify(app, f"🔴 EXIT [{mode.upper()}]\n{trade_block(trade)}\n"
                      f"Day PnL: {money(state['realized_pnl'])}{extra}")


def live_text(pnl, s_ltp, h_ltp) -> str:
    p = state["position"]
    credit_total = p["credit"] * p["qty"]
    pct = (pnl / credit_total * 100) if credit_total else 0
    icon = "🟢" if pnl >= 0 else "🔴"
    return (
        f"{icon} LIVE PnL [{state['mode'].upper()}]\n"
        f"SELL {_strike_txt(p['sell']['symbol'])} | {p['sell_entry']:.2f} → {s_ltp:.2f} | "
        f"{money((p['sell_entry'] - s_ltp) * p['qty'])}\n"
        f"BUY {_strike_txt(p['hedge']['symbol'])} | {p['hedge_entry']:.2f} → {h_ltp:.2f} | "
        f"{money((h_ltp - p['hedge_entry']) * p['qty'])}\n"
        f"━━━━━━━━━━━━\n"
        f"Open PnL: ₹{pnl:,.0f}  ({pct:+.0f}% of credit)\n"
        f"Target: +₹{p['tp']:,.0f} | SL: -₹{abs(p['sl']):,.0f}\n"
        f"Day realized: ₹{state['realized_pnl']:,.0f}\n"
        f"Total day PnL: ₹{state['realized_pnl'] + pnl:,.0f}\n"
        f"Updated {datetime.now().strftime('%H:%M:%S')}")


async def update_live(app: Application, pnl, s_ltp, h_ltp, force=False):
    """Ek j Telegram message edit thay chhe (spam nahi)."""
    if not ALLOWED_CHAT_ID:
        return
    if not force and time.time() - state["live_msg_t"] < PNL_UPDATE_SECONDS:
        return
    text = live_text(pnl, s_ltp, h_ltp)
    state["live_msg_t"] = time.time()
    try:
        if state["live_msg_id"] and not force:
            await app.bot.edit_message_text(text, chat_id=ALLOWED_CHAT_ID, message_id=state["live_msg_id"])
        else:
            m = await app.bot.send_message(ALLOWED_CHAT_ID, text)
            state["live_msg_id"] = m.message_id
    except Exception as e:
        if "not modified" not in str(e).lower():
            log.warning("live pnl update failed: %s", e)
            state["live_msg_id"] = None   # next vakhte navo message


async def notify(app: Application, text: str, html_mode: bool = False):
    if ALLOWED_CHAT_ID:
        await app.bot.send_message(ALLOWED_CHAT_ID, text, parse_mode="HTML" if html_mode else None)


# ----------------------------- MAIN LOOP ------------------------------
def roll_day():
    """Navo divas hoy to trades ane realized PnL reset."""
    today = datetime.now().date()
    if state["day"] != today:
        state["day"] = today
        state["trades"] = []
        state["realized_pnl"] = 0.0
        state["blocked_dir"] = None          # navo divas -> trend block reset


async def tick(context: ContextTypes.DEFAULT_TYPE):
    roll_day()
    if not state["running"]:
        return
    app = context.application
    now = datetime.now()
    if now.weekday() >= 5 or now.time() < ENTRY_START:
        return
    try:
        # 1) manage open position
        if state["position"]:
            pnl, s_ltp, h_ltp = await position_pnl()
            state["pnl_cache"] = (pnl, s_ltp, h_ltp, time.time())
            p = state["position"]
            if now.time() >= FORCE_EXIT:
                return await close_position(app, "Time exit 15:15")
            if pnl <= p["sl"]:
                return await close_position(app, f"STOP LOSS ({pnl:.0f})")
            if pnl >= p["tp"]:
                return await close_position(app, f"TARGET ({pnl:.0f})")
            await update_live(app, pnl, s_ltp, h_ltp)

        # 2) signal (naya completed candle par j)
        direction, ctime, _ = await asyncio.to_thread(current_signal)
        new_candle = state["last_signal_candle"] != ctime
        state["last_signal_candle"] = ctime
        if direction == 0:        # neutral: koi navi entry/flip nahi
            return
        if state["blocked_dir"] is not None and direction != state["blocked_dir"]:
            state["blocked_dir"] = None                    # trend palatayo -> navi trade allowed
        blocked = ONE_TRADE_PER_TREND and state["blocked_dir"] == direction

        if state["position"]:
            if EXIT_ON_FLIP and direction != state["position"]["dir"]:
                await close_position(app, "Signal flip")
                if now.time() <= LAST_ENTRY:
                    await open_position(direction, app)
        elif now.time() <= LAST_ENTRY and not blocked and (new_candle or state["last_signal_candle"] is None):
            await open_position(direction, app)
    except Exception as e:
        log.exception("tick error")
        msg = str(e)
        # same error 10 minute ma ek j vaar Telegram par
        if msg != _last_err["msg"] or time.time() - _last_err["t"] > 600:
            _last_err["msg"], _last_err["t"] = msg, time.time()
            await notify(app, f"⚠️ Error: {msg}")
        # relogin fakt session/token error hoy tyare
        low = msg.lower()
        if any(k in low for k in ("token", "session", "invalid", "ag8001", "ag8002", "unauthor")):
            try:
                await asyncio.to_thread(angel_login)
            except Exception:
                pass


# ----------------------------- COMMANDS -------------------------------
def authorized(update: Update) -> bool:
    return ALLOWED_CHAT_ID == 0 or update.effective_chat.id == ALLOWED_CHAT_ID


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text(f"Your chat id: {update.effective_chat.id}")
    await update.message.reply_text(main_menu_text(), reply_markup=main_menu_kb())


async def cmd_menu(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(main_menu_text(), reply_markup=main_menu_kb())


async def cmd_run(update: Update, ctx):
    if not authorized(update):
        return
    state["running"] = True
    state["last_signal_candle"] = None
    await update.message.reply_text(f"▶️ Bot running in {state['mode'].upper()} mode")


async def cmd_stop(update: Update, ctx):
    if not authorized(update):
        return
    state["running"] = False
    await update.message.reply_text("⏸ Bot stopped (open position hoy to /exit vapro)")


async def cmd_mode(update: Update, ctx):
    if not authorized(update):
        return
    if state["position"]:
        return await update.message.reply_text("Position open che, pehla exit karo.")
    if not ctx.args or ctx.args[0] not in ("paper", "live"):
        return await update.message.reply_text("Use: /mode paper  or  /mode live")
    state["mode"] = ctx.args[0]
    await update.message.reply_text(f"Mode: {state['mode'].upper()}")


async def status_text() -> str:
    try:
        d, ct, st = await asyncio.to_thread(current_signal)
        sig = f"{trend_name(d)} ({strategy_line()} {st:.1f}, candle {ct})"
    except Exception as e:
        sig = f"error {e}"
    msg = f"Mode: {state['mode']} | Running: {state['running']}\n{strategy_label()}: {sig}\n"
    if state["position"]:
        pnl, s_, h_ = await position_pnl()
        p = state["position"]
        msg += (f"Position: SELL {p['sell']['symbol']} @ {p['sell_entry']:.2f} (now {s_:.2f})\n"
                f"          BUY  {p['hedge']['symbol']} @ {p['hedge_entry']:.2f} (now {h_:.2f})\n"
                f"Open PnL: ₹{pnl:,.0f} (SL {p['sl']:.0f} / TP {p['tp']:.0f})")
    else:
        msg += "No open position"
    return msg


async def cmd_status(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(await status_text())


def trades_text() -> str:
    lines = [f"{t['time']} {t.get('type', '')} {t['reason']}: ₹{t['pnl']:,.0f}" for t in state["trades"]] or ["No closed trades"]
    return "\n".join(lines)


async def pnl_text() -> str:
    lines = [trades_text()]
    open_pnl = 0.0
    if state["position"]:
        open_pnl, _, _ = await position_pnl()
        lines.append(f"Open position PnL: ₹{open_pnl:,.0f}")
    lines.append(f"Day realized: ₹{state['realized_pnl']:,.0f}")
    lines.append(f"TOTAL (realized + open): ₹{state['realized_pnl'] + open_pnl:,.0f}")
    return "\n".join(lines)


async def cmd_pnl(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(await pnl_text())


async def cmd_live(update: Update, ctx):
    """Live PnL no navo message (je auto-update thase)."""
    if not authorized(update):
        return
    if not state["position"]:
        return await update.message.reply_text(
            f"No open position\nDay realized: ₹{state['realized_pnl']:,.0f}")
    pnl, s_ltp, h_ltp = await position_pnl()
    state["live_msg_id"] = None
    await update_live(ctx.application, pnl, s_ltp, h_ltp, force=True)


async def cmd_exit(update: Update, ctx):
    if not authorized(update):
        return
    if not state["position"]:
        return await update.message.reply_text("No open position")
    await close_position(ctx.application, "Manual exit")


# ----------------------------- BUTTON MENU ----------------------------
HELP_TEXT = (
    "ℹ️ Help\n"
    "Strategy select karo: 🧠 Strategy button\n"
    "BULLISH/GREEN → SELL ATM PE + BUY hedge PE\n"
    "BEARISH/RED → SELL ATM CE + BUY hedge CE\n"
    "SL / Target = net credit na %.\n"
    "Settings navi trade thi lagu thase.\n"
    "Commands: /menu /dashboard /image /signal /summary /status /pnl /live /run /stop /exit /mode")


def main_menu_text() -> str:
    run = "🟢 RUNNING" if state["running"] else "🔴 STOPPED"
    mode = "⚡ LIVE" if state["mode"] == "live" else "📝 PAPER"
    pos = "📌 Open" if state["position"] else "➖ None"
    return (f"━━━ 🤖 NIFTY SPREAD BOT ━━━\n"
            f"Bot: {run}   Mode: {mode}\n"
            f"Strategy: 🧠 {strategy_label()} • ⏱{CANDLE_MINUTES}m\n"
            f"Position: {pos}\n"
            f"Day realized: ₹{state['realized_pnl']:,.0f}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Option select karo 👇")


def main_menu_kb() -> Markup:
    return Markup([
        [Btn("📺 Text Dashboard", callback_data="dashboard"), Btn("🖼 Image Dashboard", callback_data="image")],
        [Btn("🟢 Run Bot", callback_data="run"), Btn("🔴 Stop Bot", callback_data="stop")],
        [Btn("📡 Signal", callback_data="signal"), Btn("🧠 Strategy", callback_data="strategy")],
        [Btn("📅 Today's Summary", callback_data="summary"), Btn("💹 Live PnL", callback_data="live")],
        [Btn("📊 Status", callback_data="status"), Btn("💰 PnL", callback_data="pnl")],
        [Btn("📜 Today Trades", callback_data="trades"), Btn("🗂 History", callback_data="history")],
        [Btn("🚪 Exit Position", callback_data="exit")],
        [Btn("📝 Paper Mode", callback_data="mode_paper"), Btn("⚡ Live Mode", callback_data="mode_live")],
        [Btn("⚙️ Settings", callback_data="settings"), Btn("🔄 Refresh", callback_data="menu")],
        [Btn("❓ Help", callback_data="help"), Btn("❌ Close", callback_data="close")],
    ])


def strategy_text() -> str:
    return ("🧠 Strategy select karo\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"Active: {strategy_label()}  •  ⏱ {CANDLE_MINUTES} min\n\n"
            f"• Supertrend: Green → PE spread, Red → CE spread\n"
            f"• EMA {EMA_FAST}/{EMA_SLOW}: EMA{EMA_FAST} > EMA{EMA_SLOW} → PE spread, niche → CE spread\n"
            f"• RSI{RSI_PERIOD} x WMA{RSI_WMA_LEN} + SMA{SMA_LEN}: RSI > WMA ane close > SMA → PE spread; "
            f"RSI < WMA ane close < SMA → CE spread (candle close par)\n\n"
            "⏱ Timeframe niche thi select karo (badha strategy ma lagu).\n"
            "⚠️ Strategy/timeframe badalta bot pause thase (Run pharithi dabavo).\n"
            "Position open hoy tyare badli nahi shakay.")


def strategy_kb() -> Markup:
    rows = [[Btn(("✅ " if k == state["strategy"] else "") + v["label"], callback_data=f"strat_{k}")]
            for k, v in STRATEGIES.items()]
    tf = [Btn(("✅ " if m == CANDLE_MINUTES else "") + f"{m}m", callback_data=f"tf_{m}") for m in TIMEFRAMES]
    rows.append([Btn("⏱ Timeframe", callback_data="noop")])
    rows.append(tf[:3])
    rows.append(tf[3:])
    rows.append([Btn("⬅️ Back", callback_data="menu")])
    return Markup(rows)


def _shift_time(t: dtime, minutes: int) -> dtime:
    return (datetime.combine(datetime.today(), t) + timedelta(minutes=minutes)).time()


def set_time_value(which: str, t: dtime):
    """which: start | last | end. Error text return kare, sacha hoy to None."""
    global ENTRY_START, LAST_ENTRY, FORCE_EXIT
    if which == "start":
        if not (dtime(9, 15) <= t < LAST_ENTRY):
            return f"Start time 09:15 thi {LAST_ENTRY:%H:%M} ni pehla joiye."
        ENTRY_START = t
    elif which == "last":
        hi = min(FORCE_EXIT, dtime(15, 20))
        if not (ENTRY_START < t <= hi):
            return f"Last entry {ENTRY_START:%H:%M} pachi ane {hi:%H:%M} sudhi ma joiye."
        LAST_ENTRY = t
    else:
        if not (LAST_ENTRY <= t <= dtime(15, 25)):
            return f"End (square-off) {LAST_ENTRY:%H:%M} thi 15:25 vache joiye."
        FORCE_EXIT = t
    return None


def step_history(direction: int) -> int:
    presets = [p for p in HISTORY_PRESETS if p <= ANGEL_MAX_DAYS.get(CANDLE_MINUTES, 30)]
    cur = lookback_days()
    if direction > 0:
        nxt = [p for p in presets if p > cur]
        return nxt[0] if nxt else presets[-1]
    prv = [p for p in presets if p < cur]
    return prv[-1] if prv else presets[0]


def rules_text() -> str:
    return ("🕒 Time & Rules\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"🟢 Entry start: {ENTRY_START:%H:%M}\n"
            f"⏳ Last new entry: {LAST_ENTRY:%H:%M}\n"
            f"🔚 End (square-off): {FORCE_EXIT:%H:%M}\n"
            f"☝️ One trade per trend: {'ON' if ONE_TRADE_PER_TREND else 'OFF'}\n"
            f"📚 History: {lookback_days()} days{' (auto)' if HISTORY_DAYS is None else ''} • {CANDLE_MINUTES}m\n\n"
            "Time ±5 min step ma badlay. Exact time mate:\n"
            "/time start 09:30\n/time last 14:30\n/time end 15:10\n\n"
            "One trade per trend ON: SL/Target/exit thaya pachi same trend ma navi trade nahi, "
            "trend flip thay tyare j.\n"
            "History: Supertrend/indicator ma aagal na divas na candles pan gani che. "
            "Badalta bot pause thase.")


def rules_kb() -> Markup:
    def trow(lab, which, t):
        return [Btn("➖", callback_data=f"rule_t{which}_-1"), Btn(f"{lab} {t:%H:%M}", callback_data="noop"),
                Btn("➕", callback_data=f"rule_t{which}_1")]
    return Markup([
        trow("🟢 Start", "start", ENTRY_START),
        trow("⏳ Last entry", "last", LAST_ENTRY),
        trow("🔚 End", "end", FORCE_EXIT),
        [Btn(f"☝️ One trade/trend: {'ON' if ONE_TRADE_PER_TREND else 'OFF'}", callback_data="rule_otpt_0")],
        [Btn("➖", callback_data="rule_hist_-1"),
         Btn(f"📚 {lookback_days()}d{' auto' if HISTORY_DAYS is None else ''}", callback_data="noop"),
         Btn("➕", callback_data="rule_hist_1")],
        [Btn("⬅️ Settings", callback_data="settings"), Btn("🏠 Menu", callback_data="menu")],
    ])


def expiry_text() -> str:
    try:
        exps = _expiry_list()
        today = pd.Timestamp(datetime.now().date())
        lines = []
        for m, name in EXPIRY_MODES.items():
            d_ = select_expiry(m)
            lines.append(f"{'✅' if m == EXPIRY_MODE else '▫️'} {name}: {d_:%d %b %Y}"
                         + ("  (aaje expiry!)" if d_ == today else ""))
        body = "\n".join(lines)
    except Exception:
        body = "Expiry list haju load nathi thayi."
    return ("📅 Expiry select karo\n━━━━━━━━━━━━━━━━━━\n"
            f"Active: {expiry_label()}\n\n{body}\n\n"
            f"⏭ Expiry day par next expiry: {'ON' if EXPIRY_SKIP_TODAY else 'OFF'}\n\n"
            "Sell ane hedge banne same expiry ma thase. Navi trade thi lagu thase "
            "(chalu position ni expiry nahi badle).\n"
            "Monthly/next weekly ma spread vadhu hoy shake, premium moto pan liquidity ochhi.")


def expiry_kb() -> Markup:
    rows = []
    for m, name in EXPIRY_MODES.items():
        try:
            lab = f"{name.split(' (')[0]} • {select_expiry(m):%d %b}"
        except Exception:
            lab = name.split(' (')[0]
        rows.append([Btn(("✅ " if m == EXPIRY_MODE else "") + lab, callback_data=f"exp_{m}")])
    rows.append([Btn(f"⏭ Expiry day par next: {'ON' if EXPIRY_SKIP_TODAY else 'OFF'}", callback_data="exp_skip")])
    rows.append([Btn("⬅️ Settings", callback_data="settings"), Btn("🏠 Menu", callback_data="menu")])
    return Markup(rows)


def settings_text() -> str:
    return (f"⚙️ Settings (navi trade thi lagu)\n"
            f"Lots: {LOTS} (qty {LOT_SIZE * LOTS})\n"
            f"Hedge premium: {HEDGE_PCT * 100:.0f}% of ATM\n"
            f"🛡 Stop loss: {limit_text(SL_MODE, SL_VALUE)}\n"
            f"🎯 Target: {limit_text(TP_MODE, TP_VALUE)}\n"
            f"📅 Expiry: {expiry_label()}\n"
            f"Signal flip exit: {'ON' if EXIT_ON_FLIP else 'OFF'}\n"
            f"🕒 Time: {ENTRY_START:%H:%M} - {LAST_ENTRY:%H:%M} (exit {FORCE_EXIT:%H:%M}) • "
            f"1 trade/trend: {'ON' if ONE_TRADE_PER_TREND else 'OFF'}")


def settings_kb() -> Markup:
    return Markup([
        [Btn("➖ Lots", callback_data="set_lots_-1"), Btn(f"{LOTS}", callback_data="noop"),
         Btn("➕ Lots", callback_data="set_lots_1")],
        [Btn("➖ Hedge%", callback_data="set_hedge_-5"), Btn(f"{HEDGE_PCT * 100:.0f}%", callback_data="noop"),
         Btn("➕ Hedge%", callback_data="set_hedge_5")],
        [Btn(f"🛡 SL type: {MODE_INFO[SL_MODE]['name']} 🔄", callback_data="set_slm_0")],
        [Btn("➖", callback_data="set_slv_-1"), Btn(limit_short(SL_MODE, SL_VALUE), callback_data="noop"),
         Btn("➕", callback_data="set_slv_1")],
        [Btn(f"🎯 Target type: {MODE_INFO[TP_MODE]['name']} 🔄", callback_data="set_tpm_0")],
        [Btn("➖", callback_data="set_tpv_-1"), Btn(limit_short(TP_MODE, TP_VALUE), callback_data="noop"),
         Btn("➕", callback_data="set_tpv_1")],
        [Btn("📅 Expiry select", callback_data="expiry"), Btn("🕒 Time & Rules", callback_data="rules")],
        [Btn(f"🔁 Flip exit: {'ON' if EXIT_ON_FLIP else 'OFF'}", callback_data="set_flip_0")],
        [Btn("⬅️ Back", callback_data="menu")],
    ])


async def on_button_inner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global LOTS, HEDGE_PCT, SL_MODE, SL_VALUE, TP_MODE, TP_VALUE, EXIT_ON_FLIP, ONE_TRADE_PER_TREND, HISTORY_DAYS, EXPIRY_MODE, EXPIRY_SKIP_TODAY
    q = update.callback_query
    if ALLOWED_CHAT_ID and q.message.chat_id != ALLOWED_CHAT_ID:
        return await q.answer("Not allowed", show_alert=True)
    d = q.data
    app = ctx.application

    async def show(text, kb):
        try:
            await q.edit_message_text(text, reply_markup=kb)
        except Exception as e:
            if "not modified" not in str(e).lower():
                log.warning("menu edit failed: %s", e)

    async def show_html(text, kb):
        try:
            await q.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception as e:
            if "not modified" not in str(e).lower():
                log.warning("menu edit failed: %s", e)

    try:
        if d == "noop":
            return await q.answer()
        if d == "close":
            await q.answer()
            return await q.message.delete()
        if d == "menu":
            await q.answer()
            return await show(main_menu_text(), main_menu_kb())
        if d == "run":
            state["running"] = True
            state["last_signal_candle"] = None
            await q.answer("Bot chalu thayo ▶️")
            return await show(main_menu_text(), main_menu_kb())
        if d == "stop":
            state["running"] = False
            await q.answer("Bot band ⏸")
            return await show(main_menu_text(), main_menu_kb())
        if d == "image":
            await q.answer("Image dashboard banavi rahyo chhu...")
            return await start_image(app, q.message.chat_id)
        if d == "img_refresh":
            await q.answer("Refreshing...")
            state["img_msg_id"], state["img_chat"] = q.message.message_id, q.message.chat_id
            return await update_image(app, force=True)
        if d == "img_stop":
            state["img_msg_id"] = None
            await q.answer("Auto-update band")
            return await q.edit_message_caption(
                caption="⏹ Image dashboard auto-update band. /image thi pharithi chalu karo.",
                reply_markup=Markup([[Btn("🖼 Image", callback_data="image"), Btn("🏠 Menu", callback_data="menu_new")]]))
        if d == "dashboard":
            await q.answer("Dashboard khuli rahyu chhe...")
            return await start_dashboard(app, q.message.chat_id)
        if d == "dash_refresh":
            await q.answer("Refreshed")
            state["dash_msg_id"], state["dash_chat"] = q.message.message_id, q.message.chat_id
            return await update_dashboard(app, force=True)
        if d == "dash_stop":
            state["dash_msg_id"] = None
            await q.answer("Auto-update band")
            return await show("⏹ Dashboard auto-update band.\n/dashboard thi pharithi chalu karo.",
                              Markup([[Btn("📺 Dashboard", callback_data="dashboard"),
                                       Btn("🏠 Menu", callback_data="menu_new")]]))
        if d == "menu_new":
            await q.answer()
            return await q.message.reply_text(main_menu_text(), reply_markup=main_menu_kb())
        if d == "history":
            await q.answer()
            return await q.message.reply_text(history_menu_text(), reply_markup=history_kb())
        if d == "hmenu":
            await q.answer()
            return await show(history_menu_text(), history_kb())
        if d.startswith("h_"):
            parts = d.split("_")
            if parts[1] == "sum":
                await q.answer()
                return await show(
                    stats_text(load_trades(parts[2]), scope_label(parts[2]), daywise=parts[2] != "today"),
                    Markup([[Btn("🧾 Trade details", callback_data=f"h_tr_{parts[2]}_0"),
                             Btn("🗂 History", callback_data="hmenu")]]))
            if parts[1] == "tr":
                await q.answer()
                text, kb = trade_page(parts[2], int(parts[3]))
                return await show(text, kb)
            if parts[1] == "csv":
                data = history_csv_bytes()
                if not data:
                    return await q.answer("History khali che", show_alert=True)
                await q.answer("File moklu chhu...")
                return await q.message.reply_document(document=data, filename="trades_history.csv",
                                                      caption="📥 Full trade history (Excel ma khuli shake)")
        if d == "expiry":
            await q.answer()
            return await show(expiry_text(), expiry_kb())
        if d.startswith("exp_"):
            key = d[4:]
            if key == "skip":
                EXPIRY_SKIP_TODAY = not EXPIRY_SKIP_TODAY
            elif key in EXPIRY_MODES:
                EXPIRY_MODE = key
            await q.answer(f"Expiry: {expiry_label()}")
            return await show(expiry_text(), expiry_kb())
        if d == "rules":
            await q.answer()
            return await show(rules_text(), rules_kb())
        if d.startswith("rule_"):
            _, key, val = d.split("_", 2)
            v = int(val)
            if key in ("tstart", "tlast", "tend"):
                which = {"tstart": "start", "tlast": "last", "tend": "end"}[key]
                cur = {"start": ENTRY_START, "last": LAST_ENTRY, "end": FORCE_EXIT}[which]
                err = set_time_value(which, _shift_time(cur, v * 5))
                if err:
                    return await q.answer(err, show_alert=True)
            elif key == "otpt":
                ONE_TRADE_PER_TREND = not ONE_TRADE_PER_TREND
                if not ONE_TRADE_PER_TREND:
                    state["blocked_dir"] = None
            elif key == "hist":
                if state["position"]:
                    return await q.answer("Position open che, pehla exit karo", show_alert=True)
                HISTORY_DAYS = step_history(v)
                _sig_cache["val"], _sig_cache["bucket"] = None, None
                state["last_signal_candle"] = None
                state["running"] = False            # signal badlay to surprise entry na thay
            await q.answer("Updated")
            return await show(rules_text(), rules_kb())
        if d == "strategy":
            await q.answer()
            return await show(strategy_text(), strategy_kb())
        if d.startswith("tf_"):
            if state["position"]:
                return await q.answer("Position open che, pehla exit karo", show_alert=True)
            m = int(d[3:])
            if m in TIMEFRAMES:
                set_timeframe(m)
                state["last_signal_candle"] = None
                state["running"] = False        # surprise entry na thay etle pause
            await q.answer(f"Timeframe: {CANDLE_MINUTES} min • Bot paused")
            return await show(strategy_text(), strategy_kb())
        if d.startswith("strat_"):
            if state["position"]:
                return await q.answer("Position open che, pehla exit karo", show_alert=True)
            key = d[6:]
            if key in STRATEGIES:
                state["strategy"] = key
                _sig_cache["val"], _sig_cache["bucket"] = None, None
                state["last_signal_candle"] = None
                state["running"] = False        # surprise entry na thay etle pause
            await q.answer(f"Strategy: {strategy_label()} • Bot paused")
            return await show(strategy_text(), strategy_kb())
        if d == "signal":
            await q.answer("Signal check thai rahyo chhe...")
            return await q.message.reply_text(await signal_text())
        if d == "summary":
            await q.answer()
            return await q.message.reply_text(await summary_text())
        if d == "status":
            await q.answer("Status mangavi rahyo chhu...")
            return await q.message.reply_text(await status_text())
        if d == "pnl":
            await q.answer()
            return await q.message.reply_text(await pnl_text())
        if d == "trades":
            await q.answer()
            text, kb = trade_page("today", 0)
            return await q.message.reply_text(text, reply_markup=kb)
        if d == "help":
            await q.answer()
            return await q.message.reply_text(HELP_TEXT)
        if d == "live":
            if not state["position"]:
                return await q.answer("Koi open position nathi", show_alert=True)
            await q.answer()
            pnl, s_, h_ = await position_pnl()
            state["live_msg_id"] = None
            return await update_live(app, pnl, s_, h_, force=True)

        # ---- mode
        if d in ("mode_paper", "mode_live", "mode_live_yes"):
            if state["position"]:
                return await q.answer("Position open che, pehla exit karo", show_alert=True)
            if d == "mode_live":
                await q.answer()
                return await show("⚠️ LIVE mode ma Angel One ma REAL orders jashe.\nPakka live karvu che?",
                                  Markup([[Btn("✅ Yes, LIVE", callback_data="mode_live_yes"),
                                           Btn("↩️ Cancel", callback_data="menu")]]))
            state["mode"] = "live" if d == "mode_live_yes" else "paper"
            await q.answer(f"Mode: {state['mode'].upper()}")
            return await show(main_menu_text(), main_menu_kb())

        # ---- exit position (confirm)
        if d == "exit":
            if not state["position"]:
                return await q.answer("Koi open position nathi", show_alert=True)
            await q.answer()
            return await show("🚪 Position abhi exit karvi che?",
                              Markup([[Btn("✅ Yes, Exit", callback_data="exit_yes"),
                                       Btn("↩️ Cancel", callback_data="menu")]]))
        if d == "exit_yes":
            await q.answer("Exit thai rahyu chhe...")
            if state["position"]:
                await close_position(app, "Manual exit (button)")
            return await show(main_menu_text(), main_menu_kb())

        # ---- settings
        if d == "settings":
            await q.answer()
            return await show(settings_text(), settings_kb())
        if d.startswith("set_"):
            _, key, val = d.split("_", 2)
            v = int(val)
            if key == "lots":
                LOTS = max(1, min(50, LOTS + v))
            elif key == "hedge":
                HEDGE_PCT = max(0.10, min(0.60, round(HEDGE_PCT + v / 100, 2)))
            elif key == "slm":
                SL_MODE = MODE_ORDER[(MODE_ORDER.index(SL_MODE) + 1) % len(MODE_ORDER)]
                SL_VALUE = MODE_INFO[SL_MODE]["default"]
            elif key == "tpm":
                TP_MODE = MODE_ORDER[(MODE_ORDER.index(TP_MODE) + 1) % len(MODE_ORDER)]
                TP_VALUE = MODE_INFO[TP_MODE]["default"]
            elif key == "slv":
                i = MODE_INFO[SL_MODE]
                SL_VALUE = max(i["lo"], min(i["hi"], SL_VALUE + v * i["step"]))
            elif key == "tpv":
                i = MODE_INFO[TP_MODE]
                TP_VALUE = max(i["lo"], min(i["hi"], TP_VALUE + v * i["step"]))
            elif key == "flip":
                EXIT_ON_FLIP = not EXIT_ON_FLIP
            await q.answer("Updated")
            return await show(settings_text(), settings_kb())
        await q.answer()
    except Exception as e:
        log.exception("button error")
        await q.answer(f"Error: {str(e)[:150]}", show_alert=True)


# ----------------------------- SIGNAL / SUMMARY -----------------------
async def signal_text() -> str:
    d, ct, st = await asyncio.to_thread(current_signal)
    spot = await asyncio.to_thread(nifty_spot)
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    dist = abs(spot - st)
    trend, plan = trend_name(d), plan_text(d, atm)
    pos = "None"
    if state["position"]:
        p = state["position"]
        pos = f"{p['type']} spread (since {p['time']})"
    return (f"📡 {strategy_label()} • {CANDLE_MINUTES}m\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Trend: {trend}\n"
            f"Nifty spot: {spot:,.2f}\n"
            f"{strategy_line()}: {st:,.2f}\n"
            f"{_sig_cache['extra'] + chr(10) if _sig_cache['extra'] else ''}"
            f"Distance from line: {dist:,.1f} pts\n"
            f"Last closed candle: {ct:%H:%M}\n"
            f"Data: {data_txt()} ({lookback_days()}d)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Strategy action: {plan}\n"
            f"Open position: {pos}")


async def cmd_signal(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(await signal_text())


async def cmd_help(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(HELP_TEXT)


async def _cmd_limit(update: Update, ctx, which: str):
    global SL_MODE, SL_VALUE, TP_MODE, TP_VALUE
    if not authorized(update):
        return
    name, cmd = ("🛡 Stop loss", "sl") if which == "sl" else ("🎯 Target", "target")
    cur = limit_text(SL_MODE, SL_VALUE) if which == "sl" else limit_text(TP_MODE, TP_VALUE)
    usage = (f"{name} atyare: {cur}\n\n"
             f"Navi value mate:\n"
             f"/{cmd} 2000 amt   → ₹2,000\n"
             f"/{cmd} 40 pts     → 40 points\n"
             f"/{cmd} 50 %       → net credit na 50%\n"
             f"(unit na lakho to atyar no type j rahese)\nNavi trade thi lagu thase.")
    if not ctx.args:
        return await update.message.reply_text(usage)
    text = " ".join(ctx.args).lower().replace(",", "")
    m = re.match(r"^\s*(₹|rs\.?)?\s*(\d+(?:\.\d+)?)\s*(%|pct|percent|amount|amt|rs|₹|points?|pts?)?\s*$", text)
    if not m:
        return await update.message.reply_text("❌ Samajyu nahi.\n\n" + usage)
    prefix, num, unit = m.groups()
    cur_mode = SL_MODE if which == "sl" else TP_MODE
    mode = UNIT_ALIASES[unit] if unit else ("amount" if prefix else cur_mode)
    value = float(num)
    i = MODE_INFO[mode]
    if not i["lo"] <= value <= i["hi"]:
        return await update.message.reply_text(f"❌ {i['name']} ni range {i['lo']:g} thi {i['hi']:g} vache joiye.")
    if which == "sl":
        SL_MODE, SL_VALUE = mode, value
    else:
        TP_MODE, TP_VALUE = mode, value
    save_state()
    await update.message.reply_text(f"✅ {name} set: {limit_text(mode, value)}\nNavi trade thi lagu thase.")


async def cmd_sl(update: Update, ctx):
    await _cmd_limit(update, ctx, "sl")


async def cmd_target(update: Update, ctx):
    await _cmd_limit(update, ctx, "tp")


async def cmd_time(update: Update, ctx):
    if not authorized(update):
        return
    usage = (f"🕒 Time settings\nStart: {ENTRY_START:%H:%M} | Last entry: {LAST_ENTRY:%H:%M} | "
             f"End (square-off): {FORCE_EXIT:%H:%M}\n\n"
             "/time start 09:30\n/time last 14:30\n/time end 15:10")
    if len(ctx.args) != 2:
        return await update.message.reply_text(usage)
    which = {"start": "start", "entry": "start", "last": "last", "end": "end", "exit": "end",
             "squareoff": "end"}.get(ctx.args[0].lower())
    m = re.match(r"^(\d{1,2})[:.](\d{2})$", ctx.args[1])
    if not which or not m or int(m.group(1)) > 23 or int(m.group(2)) > 59:
        return await update.message.reply_text("❌ Samajyu nahi.\n\n" + usage)
    err = set_time_value(which, dtime(int(m.group(1)), int(m.group(2))))
    if err:
        return await update.message.reply_text("❌ " + err)
    save_state()
    await update.message.reply_text(f"✅ Set thayu.\nStart {ENTRY_START:%H:%M} | Last entry {LAST_ENTRY:%H:%M} | "
                                    f"End {FORCE_EXIT:%H:%M}")


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        await on_button_inner(update, ctx)
    finally:
        save_state()      # settings / mode / run badlay to save


async def post_init(app: Application):
    """Bot ni default Menu button (message box ni baju ma) ma commands batave."""
    await app.bot.set_my_commands([
        BotCommand("menu", "🏠 Main menu (buttons)"),
        BotCommand("dashboard", "📺 Text dashboard (live, 2 sec)"),
        BotCommand("image", "🖼 Image dashboard (chart + gauge)"),
        BotCommand("signal", "📡 Strategy signal"),
        BotCommand("summary", "📅 Today summary (full detail)"),
        BotCommand("trades", "🧾 Today trades (lot, rate, PnL)"),
        BotCommand("export", "📥 History CSV file"),
        BotCommand("expiry", "📅 Expiry select (weekly/monthly)"),
        BotCommand("history", "🗂 History + summary + CSV"),
        BotCommand("time", "🕒 Start/End time set (/time start 09:30)"),
        BotCommand("sl", "🛡 Stop loss set (/sl 2000 amt)"),
        BotCommand("target", "🎯 Target set (/target 40 pts)"),
        BotCommand("status", "📊 Bot status"),
        BotCommand("pnl", "💰 PnL"),
        BotCommand("live", "💹 Live PnL"),
        BotCommand("run", "🟢 Run bot"),
        BotCommand("stop", "🔴 Stop bot"),
        BotCommand("exit", "🚪 Exit position"),
        BotCommand("help", "❓ Help"),
    ])
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    if restore_notes:
        await notify(app, "♻️ Bot restart thayo, juno data restore karyo:\n" + "\n".join(restore_notes) +
                     "\n\nBot STOPPED che. Navi entry mate /menu → 🟢 Run Bot.")


# ----------------------------- DASHBOARD ------------------------------
def pbar(pnl, lo, hi, n=10) -> str:
    frac = 0.5 if hi == lo else (pnl - lo) / (hi - lo)
    frac = max(0.0, min(1.0, frac))
    f = round(frac * n)
    return "▓" * f + "░" * (n - f)


async def spot_cached(max_age=15):
    w = ws_ltp(NIFTY_TOKEN)
    if w is not None:
        return w
    c = state["spot_cache"]
    if c and time.time() - c[1] < max_age:
        return c[0]
    v = await asyncio.to_thread(nifty_spot)
    state["spot_cache"] = (v, time.time())
    return v


async def pnl_cached(max_age=15):
    r = ws_pnl()
    if r is not None:
        return r
    c = state["pnl_cache"]
    if c and time.time() - c[3] < max_age:
        return c[0], c[1], c[2]
    pnl, s_, h_ = await position_pnl()
    state["pnl_cache"] = (pnl, s_, h_, time.time())
    return pnl, s_, h_


def dashboard_kb() -> Markup:
    return Markup([
        [Btn("🔄 Refresh", callback_data="dash_refresh"), Btn("⏹ Stop Auto", callback_data="dash_stop")],
        [Btn("🏠 Menu", callback_data="menu_new")],
    ])


def bar(frac, n=10) -> str:
    frac = max(0.0, min(1.0, frac))
    f = round(frac * n)
    return "▓" * f + "░" * (n - f)


def money(x) -> str:
    return f"{'+' if x >= 0 else '-'}₹{abs(x):,.0f}"


def kv(rows, w=9) -> str:
    w = max([w] + [len(str(k)) + 1 for k, _ in rows])      # lambo label hoy to pan value ne space male
    return "<pre>" + "\n".join(f"{html.escape(str(k)):<{w}}{html.escape(str(v))}" for k, v in rows) + "</pre>"


async def dashboard_text() -> str:
    esc = html.escape
    now = datetime.now()
    mkt_open = now.weekday() < 5 and dtime(9, 15) <= now.time() <= dtime(15, 30)
    feed_ok = ws_ltp(NIFTY_TOKEN) is not None
    run = "🟢 Running" if state["running"] else "🔴 Stopped"
    mode = "⚡ LIVE" if state["mode"] == "live" else "📝 PAPER"
    L = ["<b>📺 NIFTY SPREAD BOT</b>",
         f"{'🟢 LIVE' if mkt_open else '🔴 CLOSED'} • {'⚡ WS' if feed_ok else '🌐 REST'} • 🕒 {now:%H:%M:%S}",
         f"{now:%a %d %b} • Bot {run} • {mode}", ""]

    # ---------------- TREND ----------------
    try:
        d, ct, st = await asyncio.to_thread(current_signal)
        spot = await spot_cached()
        gap = spot - st
        atm = round(spot / STRIKE_STEP) * STRIKE_STEP
        if mkt_open:
            open_t = now.replace(hour=9, minute=15, second=0, microsecond=0)
            bk = int((now - open_t).total_seconds() // (60 * CANDLE_MINUTES))
            nxt = open_t + timedelta(minutes=CANDLE_MINUTES * (bk + 1))
            candle = f"{ct:%H:%M} → next {nxt:%H:%M}"
        else:
            candle = f"{ct:%d %b %H:%M}"
        L.append(f"<b>📡 {esc(strategy_label())} • {CANDLE_MINUTES}m</b>  {trend_name(d)}")
        L.append(kv([
            ("Nifty", f"{spot:,.2f}"),
            ("Info", _sig_cache["extra"] or "-"),
            (strategy_line(), f"{st:,.2f}  {'▲' if gap >= 0 else '▼'} {abs(gap):,.0f}"),
            ("Gap", f"{bar(abs(gap) / 200)} {abs(gap):,.0f} pts"),
            ("Candle", candle),
            ("Data", data_txt()),
            ("Expiry", expiry_label()),
            ("Plan", plan_text(d, atm)),
        ]))
    except Exception as e:
        L.append(f"<b>📡 TREND</b>  n/a ({esc(str(e)[:50])})")
    L.append("")

    # ---------------- POSITION ----------------
    open_pnl = 0.0
    p = state["position"]
    if p:
        try:
            pnl, s_, h_ = await pnl_cached()
            open_pnl = pnl
            qty = p["qty"]
            credit = p["credit"] * qty
            pct = pnl / credit * 100 if credit else 0
            try:
                exp = f"{datetime.strptime(p['expiry'], '%Y-%m-%d'):%d %b}"
            except Exception:
                exp = p["expiry"]
            sell_pl = (p["sell_entry"] - s_) * qty
            buy_pl = (h_ - p["hedge_entry"]) * qty
            w = [4, 7, 7, 8]
            hl = lambda l, m, r: l + m.join("─" * (x + 2) for x in w) + r
            row = lambda a, b, c, d_: f"│ {a:<4} │ {b:>7} │ {c:>7} │ {d_:>8} │"
            table = "\n".join([
                hl("┌", "┬", "┐"), row("Leg", "Entry", "LTP", "PnL"), hl("├", "┼", "┤"),
                row("SELL", f"{p['sell_entry']:.2f}", f"{s_:.2f}", f"{sell_pl:+,.0f}"),
                row("BUY", f"{p['hedge_entry']:.2f}", f"{h_:.2f}", f"{buy_pl:+,.0f}"),
                hl("└", "┴", "┘")])
            L.append(f"<b>📌 {p['type']} SPREAD</b> • Exp {esc(exp)} • since {esc(p['time'])}")
            L.append(f"<pre>{table}</pre>")
            L.append(f"SELL <code>{esc(p['sell']['symbol'])}</code>")
            L.append(f"BUY  <code>{esc(p['hedge']['symbol'])}</code>")
            L.append("")
            L.append(f"{'🟢' if pnl >= 0 else '🔴'} <b>Open PnL {money(pnl)}</b>  ({pct:+.0f}%)")
            to_tp = max(0.0, pnl) / p["tp"] if p["tp"] else 0
            sl_buf = 1 - max(0.0, -pnl) / abs(p["sl"]) if p["sl"] else 1
            L.append("<b>📊 PnL METER</b>")
            L.append(kv([
                ("Range", f"SL {p['sl']:+,.0f} {pbar(pnl, p['sl'], p['tp'])} {p['tp']:+,.0f} TP"),
                ("Target", f"{bar(to_tp)} {to_tp * 100:.0f}%"),
                ("SL room", f"{bar(sl_buf)} {sl_buf * 100:.0f}%"),
                ("Credit", f"₹{credit:,.0f} • Qty {qty}"),
            ]))
        except Exception as e:
            L.append(f"<b>📌 POSITION</b> open (PnL n/a: {esc(str(e)[:40])})")
    else:
        L.append("<b>📌 POSITION</b>  ➖ none (signal/entry ni rah)")
    L.append("")

    # ---------------- TODAY ----------------
    tr = state["trades"]
    wins = sum(1 for t in tr if t["pnl"] > 0)
    total = state["realized_pnl"] + open_pnl
    wr = wins / len(tr) if tr else 0
    best = max((t["pnl"] for t in tr), default=0)
    worst = min((t["pnl"] for t in tr), default=0)
    L.append(f"<b>📅 TODAY</b>  {'🟢' if total >= 0 else '🔴'} <b>{money(total)}</b>")
    L.append(kv([
        ("Trades", f"{len(tr)}   ✅{wins} ❌{len(tr) - wins}"),
        ("Win", f"{bar(wr)} {wr * 100:.0f}%" if tr else "-"),
        ("Realized", money(state["realized_pnl"])),
        ("Open", money(open_pnl)),
        ("Best", money(best)),
        ("Worst", money(worst)),
        ("Last", state["last_event"]),
    ]))
    L.append(f"<i>🛡 SL {limit_short(SL_MODE, SL_VALUE)} • 🎯 TP {limit_short(TP_MODE, TP_VALUE)} • Hedge {HEDGE_PCT * 100:.0f}% • "
             f"Lots {LOTS} • 🕒 {ENTRY_START:%H:%M}-{LAST_ENTRY:%H:%M} (exit {FORCE_EXIT:%H:%M}) • ♻️ {DASH_REFRESH}s</i>")
    return "\n".join(L)


async def start_dashboard(app: Application, chat_id: int):
    try:
        text = await dashboard_text()
    except Exception as e:
        text = f"📺 Dashboard error: {html.escape(str(e))}"
    m = await app.bot.send_message(chat_id, text, reply_markup=dashboard_kb(), parse_mode="HTML")
    state["dash_msg_id"], state["dash_chat"], state["dash_t"] = m.message_id, chat_id, time.time()


async def update_dashboard(app: Application, force=False):
    if not state["dash_msg_id"]:
        return
    if not force and time.time() - state["dash_t"] < DASH_REFRESH:
        return
    state["dash_t"] = time.time()
    try:
        text = await dashboard_text()
        await app.bot.edit_message_text(text, chat_id=state["dash_chat"],
                                        message_id=state["dash_msg_id"], reply_markup=dashboard_kb(),
                                        parse_mode="HTML")
    except Exception as e:
        if hasattr(e, "retry_after"):                       # Telegram flood control
            state["dash_t"] = time.time() + float(e.retry_after)
        elif "not modified" not in str(e).lower():
            log.warning("dashboard update failed: %s", e)
            if "not found" in str(e).lower() or "can't be edited" in str(e).lower():
                state["dash_msg_id"] = None


async def cmd_dashboard(update: Update, ctx):
    if not authorized(update):
        return
    await start_dashboard(ctx.application, update.effective_chat.id)


# ----------------------------- FAST LOOP ------------------------------
_last_rest_exit = [0.0]


async def fast_exit_check(app: Application):
    """SL / Target / time exit - websocket price thi dar 2 sec e (running hoy ke na hoy)."""
    p = state["position"]
    if not p:
        return
    r = ws_pnl()
    if r is None:                                  # ws nathi -> REST, dar 20 sec e
        if time.time() - _last_rest_exit[0] < 20:
            return
        _last_rest_exit[0] = time.time()
        pnl, _, _ = await position_pnl()
    else:
        pnl = r[0]
    if datetime.now().time() >= FORCE_EXIT:
        return await close_position(app, "Time exit 15:15")
    if pnl <= p["sl"]:
        return await close_position(app, f"STOP LOSS ({pnl:.0f})")
    if pnl >= p["tp"]:
        return await close_position(app, f"TARGET ({pnl:.0f})")


async def fast_loop(context: ContextTypes.DEFAULT_TYPE):
    app = context.application
    try:
        await fast_exit_check(app)
    except Exception:
        log.exception("fast exit check error")
    await update_dashboard(app)
    if time.time() - _last_save[0] > 10:
        save_state()


# ----------------------------- IMAGE DASHBOARD ------------------------
def _clean(s) -> str:
    """Image ma emoji glyph nathi, etle badha non-BMP/emoji char kadhi nakho."""
    return re.sub(r"[^\x20-\u2fff\n]", "", str(s)).strip()


def plot_series(strategy: str, df: pd.DataFrame):
    """Chart mate (direction array, reference-line array) - puri df par."""
    if strategy == "ema":
        fast = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
        slow = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
        return np.where(fast > slow, 1, -1), slow.values
    if strategy == "rsi_wma":
        _, _, sma, dirs = rsi_wma_calc(df)
        return np.asarray(dirs), sma.values
    d2 = supertrend(df)
    return d2["dir"].values, d2["st"].values


async def build_snapshot() -> dict:
    now = datetime.now()
    d, ct, line_now = await asyncio.to_thread(current_signal)      # candle cache bharay
    df = _candle_cache["df"]
    spot = await spot_cached()
    dirs, line = plot_series(state["strategy"], df)
    N = IMG_CANDLES
    sub = df.iloc[-N:].reset_index(drop=True)
    o, h, l, c = (sub[k].values.astype(float) for k in ("open", "high", "low", "close"))
    c[-1] = spot                         # chalu candle live dekhay
    h[-1], l[-1] = max(h[-1], spot), min(l[-1], spot)
    times = [t.to_pydatetime() if hasattr(t, "to_pydatetime") else t for t in sub["time"]]
    mkt_open = now.weekday() < 5 and dtime(9, 15) <= now.time() <= dtime(15, 30)
    if mkt_open:
        open_t = now.replace(hour=9, minute=15, second=0, microsecond=0)
        bk = int((now - open_t).total_seconds() // (60 * CANDLE_MINUTES))
        nxt = (open_t + timedelta(minutes=CANDLE_MINUTES * (bk + 1))).strftime("%H:%M")
    else:
        nxt = "-"
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP

    pos = None
    p = state["position"]
    if p:
        try:
            pnl, s_, h_ = await pnl_cached()
            qty = p["qty"]
            t_entry = datetime.combine(now.date(), datetime.strptime(p["time"], "%H:%M:%S").time())
            idx = None
            for i, t in enumerate(times):
                if t <= t_entry < t + timedelta(minutes=CANDLE_MINUTES):
                    idx = i
            e_price = p.get("spot") or (float(c[idx]) if idx is not None else None)
            try:
                exp = f"{datetime.strptime(p['expiry'], '%Y-%m-%d'):%d %b}"
            except Exception:
                exp = p["expiry"]
            pos = {"type": p["type"], "exp": exp, "since": p["time"], "pnl": pnl, "sl": p["sl"], "tp": p["tp"],
                   "credit": p["credit"] * qty, "idx": idx, "e_price": e_price,
                   "legs": [("SELL", p["sell"]["symbol"], p["sell_entry"], s_, (p["sell_entry"] - s_) * qty),
                            ("BUY", p["hedge"]["symbol"], p["hedge_entry"], h_, (h_ - p["hedge_entry"]) * qty)]}
        except Exception as e:
            log.warning("snapshot position error: %s", e)

    tr = state["trades"]
    wins = sum(1 for t in tr if t["pnl"] > 0)
    open_pnl = pos["pnl"] if pos else 0.0
    return {
        "label": strategy_label(), "tf": CANDLE_MINUTES, "line_name": strategy_line(),
        "now": now, "mode": state["mode"], "running": state["running"],
        "feed_ok": ws_ltp(NIFTY_TOKEN) is not None, "mkt_open": mkt_open,
        "o": o, "h": h, "l": l, "c": c, "times": times,
        "dirs": np.asarray(dirs)[-N:], "line": np.asarray(line, dtype=float)[-N:],
        "spot": spot, "line_now": line_now, "trend": d, "plan": plan_text(d, atm),
        "candle": f"{ct:%H:%M}", "next": nxt, "pos": pos,
        "today": {"n": len(tr), "wins": wins, "losses": len(tr) - wins, "realized": state["realized_pnl"],
                  "open": open_pnl, "total": state["realized_pnl"] + open_pnl, "last": state["last_event"]},
        "footer": f"SL {limit_short(SL_MODE, SL_VALUE)}   •   TP {limit_short(TP_MODE, TP_VALUE)}   •   "
                  f"Hedge {HEDGE_PCT * 100:.0f}%   •   Lots {LOTS}   •   {ENTRY_START:%H:%M}-{LAST_ENTRY:%H:%M} / exit {FORCE_EXIT:%H:%M}",
    }


def render_png(sn: dict) -> bytes:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.patches import FancyBboxPatch, Rectangle, Wedge, Circle

    BG, CARD, CARD2 = "#0b1020", "#131a2e", "#18213a"
    TXT, MUTED = "#e8ecf8", "#7d8aa8"
    GREEN, RED, AMBER, BLUE = "#22d39a", "#ff5a6e", "#ffb547", "#5b8cff"
    W, H = 10.8, 16.9
    fig = Figure(figsize=(W, H), dpi=100, facecolor=BG)
    FigureCanvasAgg(fig)
    root = fig.add_axes([0, 0, 1, 1])
    root.set_xlim(0, W)
    root.set_ylim(0, H)
    root.axis("off")

    def card(x, y, w, h, fc=CARD):
        root.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=0.22",
                                      fc=fc, ec="#222c4a", lw=1.2))

    def text(x, y, s, size=12, color=TXT, weight="normal", ha="left", va="center"):
        root.text(x, y, _clean(s), fontsize=size, color=color, fontweight=weight, ha=ha, va=va)

    def pill(x, y, label, color, w):
        root.add_patch(FancyBboxPatch((x, y - 0.2), w, 0.4, boxstyle="round,pad=0,rounding_size=0.2",
                                      fc=color + "22", ec=color, lw=1.2))
        text(x + w / 2, y, label, 10.5, color, "bold", "center")

    now, spot, d = sn["now"], sn["spot"], sn["trend"]
    tcol = GREEN if d == 1 else (RED if d == -1 else AMBER)
    tname = {1: "BULLISH", -1: "BEARISH"}.get(d, "NEUTRAL")
    pos, td = sn["pos"], sn["today"]

    # ---------- header ----------
    text(0.5, H - 0.55, "NIFTY SPREAD BOT", 22, TXT, "bold")
    text(0.5, H - 1.0, f"{sn['label']}  •  {sn['tf']}m  •  {now:%a %d %b}"
         f"  •  Market {'OPEN' if sn['mkt_open'] else 'CLOSED'}", 11, MUTED)
    text(W - 0.5, H - 0.55, f"{now:%H:%M:%S}", 20, TXT, "bold", "right")
    x = W - 0.5
    for lab, col in [("RUNNING" if sn["running"] else "STOPPED", GREEN if sn["running"] else RED),
                     ("WS LIVE" if sn["feed_ok"] else "REST", GREEN if sn["feed_ok"] else AMBER),
                     ("LIVE" if sn["mode"] == "live" else "PAPER", AMBER if sn["mode"] == "live" else BLUE)]:
        w = 0.17 * len(lab) + 0.45
        x -= w
        pill(x, H - 1.0, lab, col, w)
        x -= 0.15

    # ---------- chart ----------
    cy1 = H - 1.3
    cy0 = cy1 - 4.6
    card(0.35, cy0, W - 0.7, cy1 - cy0)
    text(0.65, cy1 - 0.35, "NIFTY 50", 13, TXT, "bold")
    text(1.95, cy1 - 0.35, f"{spot:,.2f}", 13, tcol, "bold")
    root.add_patch(Circle((W - 3.2, cy1 - 0.35), 0.07, fc=GREEN))
    text(W - 3.05, cy1 - 0.35, "up", 9.5, MUTED)
    root.add_patch(Circle((W - 2.4, cy1 - 0.35), 0.07, fc=RED))
    text(W - 2.25, cy1 - 0.35, "down", 9.5, MUTED)
    n = len(sn["c"])
    ax = fig.add_axes([0.07, (cy0 + 0.45) / H, 0.855, (cy1 - cy0 - 1.0) / H], facecolor=CARD)
    ax.set_xlim(-1, n + 7)
    allv = np.r_[sn["l"], sn["h"], sn["line"][np.isfinite(sn["line"])]]
    pad = (allv.max() - allv.min()) * 0.08 or 10
    lo, hi = allv.min() - pad, allv.max() + pad
    ax.set_ylim(lo, hi)
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.tick_params(colors=MUTED, labelsize=9, length=0)
    ax.yaxis.tick_right()
    step = max(1, n // 6)
    ax.set_xticks(list(range(0, n, step)))
    ax.set_xticklabels([sn["times"][i].strftime("%H:%M") for i in range(0, n, step)])
    ax.grid(axis="y", color="#232c4a", lw=0.8)
    for i in range(n):
        col = GREEN if sn["c"][i] >= sn["o"][i] else RED
        ax.plot([i, i], [sn["l"][i], sn["h"][i]], color=col, lw=1.1, zorder=3)
        ax.add_patch(Rectangle((i - 0.32, min(sn["o"][i], sn["c"][i])), 0.64,
                               max(abs(sn["c"][i] - sn["o"][i]), (hi - lo) * 0.002), fc=col, ec=col, zorder=4))
    dirs, line = sn["dirs"], sn["line"]
    for i in range(1, n):
        if dirs[i] == dirs[i - 1] and np.isfinite(line[i]) and np.isfinite(line[i - 1]):
            ax.plot([i - 1, i], [line[i - 1], line[i]], color=GREEN if dirs[i] == 1 else (RED if dirs[i] == -1 else AMBER),
                    lw=2.4, zorder=5)
    ok = np.isfinite(line)
    ax.fill_between(range(n), line, sn["c"], where=(dirs == 1) & ok, color=GREEN, alpha=0.06, zorder=1)
    ax.fill_between(range(n), line, sn["c"], where=(dirs == -1) & ok, color=RED, alpha=0.06, zorder=1)
    if pos and pos["idx"] is not None and pos["e_price"]:
        ex, rng = pos["idx"], hi - lo
        if pos["type"] == "PE":      # bullish: arrow neeche thi upar
            ax.annotate("", xy=(ex, sn["l"][ex] - rng * 0.01), xytext=(ex, sn["l"][ex] - rng * 0.09),
                        arrowprops=dict(arrowstyle="-|>", color=AMBER, lw=2.2), zorder=7)
            ax.text(ex, sn["l"][ex] - rng * 0.11, f"ENTRY\n{pos['type']} spread", color=AMBER, fontsize=9.5,
                    fontweight="bold", ha="center", va="top", zorder=7)
        else:                        # bearish: arrow upar thi neeche
            ax.annotate("", xy=(ex, sn["h"][ex] + rng * 0.01), xytext=(ex, sn["h"][ex] + rng * 0.09),
                        arrowprops=dict(arrowstyle="-|>", color=AMBER, lw=2.2), zorder=7)
            ax.text(ex, sn["h"][ex] + rng * 0.11, f"ENTRY\n{pos['type']} spread", color=AMBER, fontsize=9.5,
                    fontweight="bold", ha="center", va="bottom", zorder=7)
        ax.axhline(pos["e_price"], color=AMBER, lw=1, ls=(0, (4, 4)), alpha=0.7, zorder=2)
    ax.axhline(spot, color=TXT, lw=0.9, ls=(0, (2, 3)), alpha=0.6, zorder=2)
    ax.text(n + 6.8, spot, f" {spot:,.1f} ", color=BG, fontsize=9.5, fontweight="bold", ha="right", va="center",
            bbox=dict(boxstyle="round,pad=0.25", fc=tcol, ec="none"), zorder=8)
    if np.isfinite(sn["line_now"]):
        ax.text(n + 6.8, sn["line_now"], f" {sn['line_name']} {sn['line_now']:,.0f} ", color=BG, fontsize=8.5,
                fontweight="bold", ha="right", va="center",
                bbox=dict(boxstyle="round,pad=0.2", fc=tcol, ec="none", alpha=0.85), zorder=8)

    # ---------- 3 KPI cards ----------
    ky = cy0 - 0.2 - 1.35
    cw = (W - 0.7 - 0.5) / 3
    gap = spot - sn["line_now"]
    kp = [("TREND", tname, sn["plan"], tcol),
          ("NIFTY", f"{spot:,.1f}", f"Last candle {sn['candle']}  >  next {sn['next']}", TXT),
          (f"GAP vs {sn['line_name']}", f"{abs(gap):,.0f} pts", "above line" if gap >= 0 else "below line", AMBER)]
    for k, (t1, t2, t3, col) in enumerate(kp):
        x0 = 0.35 + k * (cw + 0.25)
        card(x0, ky, cw, 1.35, CARD2)
        text(x0 + 0.25, ky + 1.07, t1, 10, MUTED, "bold")
        text(x0 + 0.25, ky + 0.68, t2, 19, col, "bold")
        text(x0 + 0.25, ky + 0.28, t3, 8.8, MUTED)

    # ---------- gauge ----------
    gy1 = ky - 0.2
    gy0 = gy1 - 3.4
    card(0.35, gy0, W - 0.7, gy1 - gy0)
    text(0.65, gy1 - 0.38, "LIVE PnL", 13, TXT, "bold")
    cx, cyg, R = W / 2, gy0 + 1.25, 1.85
    if pos:
        text(W - 0.65, gy1 - 0.38, f"{pos['type']} SPREAD  •  Exp {pos['exp']}  •  since {pos['since'][:5]}",
             10, MUTED, ha="right")
        sl, tp, pnl = pos["sl"], pos["tp"], pos["pnl"]
        for a1, a0, col in [(180, 120, RED), (120, 60, AMBER), (60, 0, GREEN)]:
            root.add_patch(Wedge((cx, cyg), R, a0, a1, width=0.36, fc=col, ec=BG, lw=3, alpha=0.9))
        frac = max(0.0, min(1.0, (pnl - sl) / (tp - sl) if tp != sl else 0.5))
        ang = np.deg2rad(180 - frac * 180)
        root.plot([cx, cx + (R - 0.5) * np.cos(ang)], [cyg, cyg + (R - 0.5) * np.sin(ang)],
                  color=TXT, lw=3.2, solid_capstyle="round", zorder=10)
        root.add_patch(Circle((cx, cyg), 0.16, fc=TXT, zorder=11))
        text(cx - R + 0.2, cyg - 0.3, f"SL\n-₹{abs(sl):,.0f}", 9.5, RED, "bold", "center")
        text(cx + R - 0.2, cyg - 0.3, f"TP\n+₹{tp:,.0f}", 9.5, GREEN, "bold", "center")
        pcol = GREEN if pnl >= 0 else RED
        sign = "+" if pnl >= 0 else "-"
        text(cx, cyg - 0.5, f"{sign}₹{abs(pnl):,.0f}", 30, pcol, "bold", "center")
        pc = pnl / pos["credit"] * 100 if pos["credit"] else 0
        tgt = (f"{pnl / tp * 100:.0f}% of target" if pnl >= 0 else f"{abs(pnl) / abs(sl) * 100:.0f}% of SL")
        text(cx, cyg - 0.98, f"{pc:+.0f}% of credit  •  {tgt}", 10.5, MUTED, ha="center")
    else:
        text(cx, cyg + 0.2, "NO OPEN POSITION", 24, MUTED, "bold", "center")
        text(cx, cyg - 0.45, f"Next entry: {sn['plan']}", 12, TXT, ha="center")
        text(cx, cyg - 0.95, "signal / candle close ni rah", 10, MUTED, ha="center")

    # ---------- legs table ----------
    ty = gy0 - 0.2
    th = 2.0
    card(0.35, ty - th, W - 0.7, th)
    cols = [0.65, 2.5, 5.6, 7.4, 9.1]
    for xx, lab in zip(cols, ["LEG", "SYMBOL", "ENTRY", "LTP", "PnL"]):
        text(xx, ty - 0.35, lab, 9.5, MUTED, "bold")
    root.plot([0.6, W - 0.6], [ty - 0.62, ty - 0.62], color="#25304f", lw=1)
    if pos:
        for r, (leg, sym, e, lt, pl) in enumerate(pos["legs"]):
            yy = ty - 0.95 - r * 0.5
            text(cols[0], yy, leg, 11.5, RED if leg == "SELL" else GREEN, "bold")
            text(cols[1], yy, sym, 10.5, TXT)
            text(cols[2], yy, f"{e:.2f}", 11.5, TXT)
            text(cols[3], yy, f"{lt:.2f}", 11.5, TXT, "bold")
            text(cols[4], yy, f"{pl:+,.0f}", 11.5, GREEN if pl >= 0 else RED, "bold")
    else:
        text(W / 2, ty - 1.3, "-  koi open position nathi  -", 11, MUTED, ha="center")

    # ---------- today ----------
    ay1 = ty - th - 0.2
    ay0 = ay1 - 2.6
    card(0.35, ay0, W - 0.7, ay1 - ay0)
    tot = td["total"]
    text(0.65, ay1 - 0.38, "TODAY", 13, TXT, "bold")
    text(W - 0.65, ay1 - 0.38, f"{'+' if tot >= 0 else '-'}₹{abs(tot):,.0f}", 20, GREEN if tot >= 0 else RED, "bold", "right")
    money = lambda v: f"{'+' if v >= 0 else '-'}₹{abs(v):,.0f}"
    stats = [("Trades", str(td["n"]), TXT), ("Wins", str(td["wins"]), GREEN), ("Losses", str(td["losses"]), RED),
             ("Realized", money(td["realized"]), GREEN if td["realized"] >= 0 else RED),
             ("Open", money(td["open"]), GREEN if td["open"] >= 0 else RED)]
    sw = (W - 1.3) / len(stats)
    for k, (lab, val, col) in enumerate(stats):
        x0 = 0.65 + k * sw
        text(x0, ay1 - 0.9, lab, 9.5, MUTED)
        text(x0, ay1 - 1.28, val, 15, col, "bold")
    by = ay1 - 1.8
    wr = td["wins"] / td["n"] if td["n"] else 0
    text(0.65, by, "Win rate", 9.5, MUTED)
    root.add_patch(FancyBboxPatch((1.9, by - 0.1), W - 3.6, 0.2, boxstyle="round,pad=0,rounding_size=0.1", fc="#1f2947", ec="none"))
    if wr > 0:
        root.add_patch(FancyBboxPatch((1.9, by - 0.1), (W - 3.6) * wr, 0.2, boxstyle="round,pad=0,rounding_size=0.1", fc=GREEN, ec="none"))
    text(W - 0.65, by, f"{wr * 100:.0f}%" if td["n"] else "-", 11, GREEN, "bold", "right")
    text(0.65, ay0 + 0.28, f"Last: {td['last']}", 9.5, MUTED)

    # ---------- footer ----------
    text(0.5, 0.5, sn["footer"], 10, MUTED)
    text(W - 0.5, 0.5, f"updated {now:%H:%M:%S}", 10, MUTED, ha="right")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor=BG, dpi=100)
    return buf.getvalue()


_render_lock = threading.Lock()


def _render_safe(sn: dict) -> bytes:
    with _render_lock:
        return render_png(sn)


async def make_image() -> bytes:
    sn = await build_snapshot()
    return await asyncio.to_thread(_render_safe, sn)


def image_kb() -> Markup:
    return Markup([
        [Btn("🔄 Refresh", callback_data="img_refresh"), Btn("⏹ Stop Auto", callback_data="img_stop")],
        [Btn("📺 Text Dashboard", callback_data="dashboard"), Btn("🏠 Menu", callback_data="menu_new")],
    ])


def image_caption() -> str:
    return f"🖼 Image Dashboard • {datetime.now():%H:%M:%S} • auto {IMG_REFRESH}s"


async def start_image(app: Application, chat_id: int):
    try:
        png = await make_image()
    except ImportError:
        return await app.bot.send_message(chat_id, "🖼 Image dashboard mate: pip install matplotlib karo, "
                                                   "pachi bot restart karo. (Text dashboard chalu j che.)")
    except Exception as e:
        log.exception("image dashboard error")
        return await app.bot.send_message(chat_id, f"🖼 Image error: {e}")
    m = await app.bot.send_photo(chat_id, photo=png, caption=image_caption(), reply_markup=image_kb())
    state["img_msg_id"], state["img_chat"], state["img_t"] = m.message_id, chat_id, time.time()


async def update_image(app: Application, force=False):
    if not state["img_msg_id"] or state["img_busy"]:
        return
    if not force and time.time() - state["img_t"] < IMG_REFRESH - 1:
        return
    state["img_busy"] = True
    state["img_t"] = time.time()
    try:
        png = await make_image()
        await app.bot.edit_message_media(chat_id=state["img_chat"], message_id=state["img_msg_id"],
                                         media=InputMediaPhoto(media=png, caption=image_caption()),
                                         reply_markup=image_kb())
    except Exception as e:
        if hasattr(e, "retry_after"):                       # Telegram flood control
            state["img_t"] = time.time() + float(e.retry_after)
        elif "not modified" not in str(e).lower():
            log.warning("image update failed: %s", e)
            if "not found" in str(e).lower() or "can't be edited" in str(e).lower():
                state["img_msg_id"] = None
    finally:
        state["img_busy"] = False


async def img_loop(context: ContextTypes.DEFAULT_TYPE):
    await update_image(context.application)


async def cmd_image(update: Update, ctx):
    if not authorized(update):
        return
    await start_image(ctx.application, update.effective_chat.id)


# ----------------------------- TRADE DETAIL / HISTORY / SUMMARY --------
def fnum(r: dict, k: str):
    try:
        v = r.get(k)
        return None if v in (None, "") else float(v)
    except Exception:
        return None


def scope_label(scope: str) -> str:
    return {"today": "Today", "7": "Last 7 days", "all": "All time"}.get(scope, scope)


def load_trades(scope: str = "all") -> list:
    today = datetime.now().date()
    out = []
    for r in read_history():
        try:
            dt = datetime.strptime(r.get("date", ""), "%Y-%m-%d").date()
        except Exception:
            continue
        if scope == "today" and dt != today:
            continue
        if scope == "7" and dt < today - timedelta(days=6):
            continue
        out.append(r)
    return out


def _short_sym(sym) -> str:
    return str(sym).replace("NIFTY", "", 1)


def _strike_txt(sym) -> str:
    """NIFTY17NOV2624500PE -> 24500PE (expiry header ma alag dekhay che)."""
    m = re.search(r"(\d{5})(CE|PE)$", str(sym))      # Nifty strike 5 digit (expiry year alag)
    return f"{m.group(1)}{m.group(2)}" if m else _short_sym(sym)


def _exp_txt(e) -> str:
    try:
        return f"{datetime.strptime(str(e), '%Y-%m-%d'):%d %b}"
    except Exception:
        return str(e or "-")


def build_trade_record(p: dict, s: float, h: float, pnl: float, reason: str, mode: str, exit_spot) -> dict:
    now = datetime.now()
    qty = p["qty"]
    lots = p.get("lots") or max(1, round(qty / LOT_SIZE))
    try:
        edt = datetime.fromisoformat(p["entry_dt"])
    except Exception:
        try:
            edt = datetime.combine(now.date(), datetime.strptime(p["time"], "%H:%M:%S").time())
        except Exception:
            edt = None
    dur = round((now - edt).total_seconds() / 60, 1) if edt and now >= edt else ""
    sell_pts = p["sell_entry"] - s
    hedge_pts = h - p["hedge_entry"]
    return {
        "date": str(now.date()), "entry_time": edt.strftime("%H:%M:%S") if edt else p.get("time", ""),
        "exit_time": now.strftime("%H:%M:%S"), "time": now.strftime("%H:%M:%S"), "duration_min": dur,
        "mode": mode, "strategy": p.get("strategy", ""), "timeframe": p.get("tf", ""),
        "type": p["type"], "expiry": p.get("expiry", ""), "lots": lots, "qty": qty,
        "entry_spot": round(p["spot"], 2) if p.get("spot") else "",
        "exit_spot": round(exit_spot, 2) if exit_spot else "",
        "sell_symbol": p["sell"]["symbol"], "sell_entry": p["sell_entry"], "sell_exit": s,
        "sell_pnl": round(sell_pts * qty, 2),
        "hedge_symbol": p["hedge"]["symbol"], "hedge_entry": p["hedge_entry"], "hedge_exit": h,
        "hedge_pnl": round(hedge_pts * qty, 2),
        "net_credit_pts": round(p["sell_entry"] - p["hedge_entry"], 2), "net_exit_pts": round(s - h, 2),
        "net_points": round(sell_pts + hedge_pts, 2), "pnl": round(pnl, 2),
        "sl_rule": p.get("sl_txt", ""), "tp_rule": p.get("tp_txt", ""), "reason": reason,
    }


def trade_block(r: dict, num=None) -> str:
    """Ek trade nu full detail - plain text (lot, rate, points, PnL, SL/TP rule)."""
    qty = fnum(r, "qty") or 0.0
    lots = fnum(r, "lots") or (qty / LOT_SIZE if qty else 0.0)
    pnl = fnum(r, "pnl") or 0.0
    et = r.get("entry_time") or "-"
    xt = r.get("exit_time") or r.get("time") or "-"
    dur = fnum(r, "duration_min")
    L = [f"{'#' + str(num) + ' ' if num else ''}{'🟢' if pnl >= 0 else '🔴'} {r.get('date', '')} | "
         f"{et} → {xt}{f' ({dur:.0f}m)' if dur is not None else ''}",
         f"{r.get('type', '')} spread • {str(r.get('mode', '')).upper()} • Exp {_exp_txt(r.get('expiry'))}"]
    if r.get("strategy"):
        L.append(f"{r['strategy']} • {r.get('timeframe', '')}m")
    L.append(f"Lots: {lots:g} (Qty {qty:g})")
    es, xs = fnum(r, "entry_spot"), fnum(r, "exit_spot")
    if es is not None:
        L.append(f"Nifty: {es:,.1f}" + (f" → {xs:,.1f}" if xs is not None else ""))
    for leg, k in (("SELL", "sell"), ("BUY", "hedge")):
        e, x = fnum(r, f"{k}_entry"), fnum(r, f"{k}_exit")
        if e is None:
            continue
        lp = fnum(r, f"{k}_pnl")
        if lp is None and x is not None and qty:
            lp = ((e - x) if k == "sell" else (x - e)) * qty
        L.append(f"{leg} {_strike_txt(r.get(k + '_symbol', ''))} | {e:.2f} → {('%.2f' % x) if x is not None else '-'}"
                 + (f" | {money(lp)}" if lp is not None else ""))
    cr, ex = fnum(r, "net_credit_pts"), fnum(r, "net_exit_pts")
    if cr is not None and ex is not None:
        L.append(f"Credit {cr:.2f} → Exit {ex:.2f} = {cr - ex:+.2f} pts")
    L.append(f"💰 Net PnL: {money(pnl)}  [{r.get('reason', '')}]")
    if r.get("sl_rule"):
        L.append(f"SL: {r['sl_rule']} | TP: {r.get('tp_rule', '')}")
    return "\n".join(L)


def _reason_key(r: dict) -> str:
    k = str(r.get("reason", "")).split(" (")[0].strip()
    return "Time exit" if k.startswith("Time exit") else (k or "-")


LINE = "━━━━━━━━━━━━━━━━━━"


def stats_text(rows: list, title: str, daywise: bool = False) -> str:
    head = f"📊 Summary • {title}"
    if not rows:
        return head + "\nKoi closed trade nathi."
    pn = [fnum(r, "pnl") or 0.0 for r in rows]
    wins = [x for x in pn if x > 0]
    losses = [x for x in pn if x <= 0]
    gp, gl, net = sum(wins), sum(losses), sum(pn)
    cum = peak = mdd = 0.0
    for x in pn:
        cum += x
        peak = max(peak, cum)
        mdd = min(mdd, cum - peak)
    durs = [d for d in (fnum(r, "duration_min") for r in rows) if d is not None]
    qty_total = sum(fnum(r, "qty") or 0 for r in rows)
    lots_total = sum((fnum(r, "lots") or ((fnum(r, "qty") or 0) / LOT_SIZE)) for r in rows)
    pts = [p for p in (fnum(r, "net_points") for r in rows) if p is not None]
    L = [head, LINE,
         f"Trades: {len(rows)}  (✅ {len(wins)} | ❌ {len(losses)})",
         f"Win rate: {len(wins) / len(rows) * 100:.0f}%",
         f"💰 Net PnL: {money(net)}",
         f"Gross profit: {money(gp)} | Gross loss: {money(gl)}",
         f"Avg win: {money(gp / len(wins)) if wins else '-'} | Avg loss: {money(gl / len(losses)) if losses else '-'}",
         f"Best: {money(max(pn))} | Worst: {money(min(pn))}",
         f"Profit factor: {f'{gp / abs(gl):.2f}' if gl else '-'}",
         f"Max drawdown: {money(mdd)}",
         f"Avg time: {f'{sum(durs) / len(durs):.0f} min' if durs else '-'}",
         f"Lots: {lots_total:g} (Qty {qty_total:g})",
         f"Net points: {f'{sum(pts):+.1f}' if pts else '-'}"]

    def group(keyf, label):
        g = {}
        for r, x in zip(rows, pn):
            c = g.setdefault(keyf(r), [0, 0.0, 0])
            c[0] += 1
            c[1] += x
            c[2] += 1 if x > 0 else 0
        L.extend(["", label])
        L.extend(f"• {k or '-'}: {c[0]} trades | {money(c[1])} | ✅{c[2]}" for k, c in g.items())
    group(lambda r: f"{r.get('type', '-')} spread", "By type")
    group(_reason_key, "By exit reason")
    if any(r.get("strategy") for r in rows):
        group(lambda r: r.get("strategy") or "-", "By strategy")
    if daywise:
        days = {}
        for r, x in zip(rows, pn):
            c = days.setdefault(r.get("date", "-"), [0, 0.0])
            c[0] += 1
            c[1] += x
        L.extend(["", "Daywise"])
        L.extend(f"• {d_}: {c[0]} trades | {money(c[1])}" for d_, c in list(days.items())[-10:][::-1])
    return "\n".join(L)


TRADE_PAGE = 4


def trade_page(scope: str, page: int):
    rows = load_trades(scope)
    total = len(rows)
    pages = max(1, -(-total // TRADE_PAGE))
    page = max(0, min(page, pages - 1))
    order = list(range(total - 1, -1, -1))               # newest first
    chunk = order[page * TRADE_PAGE:(page + 1) * TRADE_PAGE]
    head = f"🧾 Trades • {scope_label(scope)} ({total}) • page {page + 1}/{pages}"
    if not chunk:
        text = head + "\nKoi closed trade nathi."
    else:
        text = head + "\n" + LINE + "\n" + ("\n" + LINE + "\n").join(trade_block(rows[i], i + 1) for i in chunk)
    nav = []
    if page > 0:
        nav.append(Btn("⬅️ Prev", callback_data=f"h_tr_{scope}_{page - 1}"))
    nav.append(Btn(f"📄 {page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(Btn("Next ➡️", callback_data=f"h_tr_{scope}_{page + 1}"))
    return text, Markup([nav, [Btn("📊 Summary", callback_data=f"h_sum_{scope}"),
                               Btn("🗂 History", callback_data="hmenu")]])


def history_menu_text() -> str:
    allr, today = load_trades("all"), load_trades("today")
    tot = lambda rows: sum(fnum(r, "pnl") or 0 for r in rows)
    return (f"🗂 Trade History\n{LINE}\n"
            f"Today: {len(today)} trades | {money(tot(today))}\n"
            f"All time: {len(allr)} trades | {money(tot(allr))}\n\n"
            "Select karo 👇")


def history_kb() -> Markup:
    return Markup([
        [Btn("📅 Today Summary", callback_data="h_sum_today"), Btn("📅 Today Trades", callback_data="h_tr_today_0")],
        [Btn("📆 7 Days Summary", callback_data="h_sum_7"), Btn("📆 7 Days Trades", callback_data="h_tr_7_0")],
        [Btn("📊 All Summary", callback_data="h_sum_all"), Btn("🧾 All Trades", callback_data="h_tr_all_0")],
        [Btn("🏠 Menu", callback_data="menu_new")],
    ])


def history_csv_bytes():
    if not os.path.exists(HISTORY_FILE):
        return None
    with open(HISTORY_FILE, "rb") as f:
        b = f.read()
    if not b.strip():
        return None
    return b if b.startswith(b"\xef\xbb\xbf") else b"\xef\xbb\xbf" + b      # Excel ma ₹ ane text barabar


async def summary_text() -> str:
    """Aajna divas nu detailed summary (+ open position hoy to te pan) - plain text."""
    roll_day()
    text = stats_text(load_trades("today"), f"Today • {datetime.now():%d %b %Y}")
    extra = [f"Mode: {state['mode'].upper()} | Bot: {'Running' if state['running'] else 'Stopped'}"]
    if state["position"]:
        open_pnl, _, _ = await position_pnl()
        p = state["position"]
        extra.append(f"📌 Open {p['type']} spread (lots {p.get('lots', '-')}): {money(open_pnl)}")
        extra.append(f"TOTAL incl. open: {money(state['realized_pnl'] + open_pnl)}")
    return text + "\n" + LINE + "\n" + "\n".join(extra)


async def cmd_summary(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(await summary_text())


async def cmd_trades(update: Update, ctx):
    if not authorized(update):
        return
    text, kb = trade_page("today", 0)
    await update.message.reply_text(text, reply_markup=kb)


async def cmd_expiry(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(expiry_text(), reply_markup=expiry_kb())


async def cmd_history(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(history_menu_text(), reply_markup=history_kb())


async def cmd_export(update: Update, ctx):
    """Optional: full history CSV file (menu ma button nathi, fakt /export)."""
    if not authorized(update):
        return
    data = history_csv_bytes()
    if not data:
        return await update.message.reply_text("History khali che.")
    await update.message.reply_document(document=data, filename="trades_history.csv",
                                        caption="📥 Full trade history (Excel ma khuli shake)")


# ----------------------------- BOOT -----------------------------------
def main():
    load_state()
    angel_login()
    load_scrips()
    start_ws()
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    for name, fn in [("start", cmd_start), ("run", cmd_run), ("stop", cmd_stop), ("mode", cmd_mode),
                     ("status", cmd_status), ("pnl", cmd_pnl), ("live", cmd_live), ("exit", cmd_exit),
                     ("signal", cmd_signal), ("summary", cmd_summary), ("help", cmd_help),
                     ("dashboard", cmd_dashboard), ("history", cmd_history),
                     ("sl", cmd_sl), ("target", cmd_target), ("tp", cmd_target), ("image", cmd_image), ("time", cmd_time), ("trades", cmd_trades), ("export", cmd_export), ("expiry", cmd_expiry)]:
        app.add_handler(CommandHandler(name, fn))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CallbackQueryHandler(on_button))
    app.job_queue.run_repeating(tick, interval=LOOP_SECONDS, first=5)
    app.job_queue.run_repeating(fast_loop, interval=DASH_REFRESH, first=3)
    app.job_queue.run_repeating(img_loop, interval=IMG_REFRESH, first=8)
    app.run_polling()


if __name__ == "__main__":
    main()
