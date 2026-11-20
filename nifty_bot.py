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
    /start /run /stop /status /mode paper|live /exit /pnl /newstrategy
"""
import asyncio
import csv
import shutil
import html
import io
import json
import logging
import os
import re
import threading
import time
from contextlib import nullcontext
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
from telegram import (BotCommand, InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup,
                      MenuButtonCommands, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update)
from telegram.ext import (Application, CallbackQueryHandler, CommandHandler, ContextTypes,
                          MessageHandler, filters)

from dotenv import load_dotenv

load_dotenv()  # .env file mathi credentials vanche

# ----------------------------- CONFIG ---------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))  # tamaro chat id (security)
# Paper / Live na alert alag chat (group) ma moklva mate - darek chat no Telegram sound alag set kari shakay
PAPER_ALERT_CHAT_ID = int(os.getenv("PAPER_ALERT_CHAT_ID", "0") or 0)
LIVE_ALERT_CHAT_ID = int(os.getenv("LIVE_ALERT_CHAT_ID", "0") or 0)

ANGEL_API_KEY = os.getenv("ANGEL_API_KEY", "")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID", "")
ANGEL_PIN = os.getenv("ANGEL_PIN", "")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET", "")
# Static IP proxy (optional). Format: http://user:pass@host:port   (Railway Variables ma PROXY_URL set karo)
PROXY_URL = os.getenv("PROXY_URL", "").strip()
if not PROXY_URL and os.getenv("STATICIP_HOST", "").strip():
    # STATICIP_HOST / _PORT / _USER(NAME) / _PASS(WORD) thi proxy URL banavo
    from urllib.parse import quote as _q
    _h = os.getenv("STATICIP_HOST", "").strip()
    _p = os.getenv("STATICIP_PORT", "").strip()
    _u = (os.getenv("STATICIP_USER") or os.getenv("STATICIP_USERNAME") or "").strip()
    _w = (os.getenv("STATICIP_PASS") or os.getenv("STATICIP_PASSWORD") or "").strip()
    _auth = f"{_q(_u, safe='')}:{_q(_w, safe='')}@" if _u else ""
    PROXY_URL = f"http://{_auth}{_h}{(':' + _p) if _p and ':' not in _h else ''}"

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
HEDGE_PCT = 0.30       # hedge premium = 30% of SOLD option premium (hedge, sell strike thi bahar)
SELL_OFFSET = 0        # sell strike: 0=ATM, +1..+5 = OTM-1..5, -1..-5 = ITM-1..5 (Telegram Settings / /strike)
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
_BUILTIN_STRATEGIES = {k: dict(v) for k, v in STRATEGIES.items()}   # built-in strategy backup (restore mate)
MAX_CUSTOM_STRATEGIES = 15
PRODUCT_TYPE = "INTRADAY"

ENTRY_START = dtime(9, 20)
LAST_ENTRY = dtime(14, 45)
FORCE_EXIT = dtime(15, 15)
EXPIRY_MODE = "w1"            # kai expiry ma trade: w1/w2/w3 = weekly, m1/m2 = monthly
EXPIRY_SKIP_TODAY = False     # True: expiry day par aajni expiry chhodi ne next expiry vapro
EXPIRY_MODES = {"w1": "Weekly 1 (nearest)", "w2": "Weekly 2 (next)", "w3": "Weekly 3",
                "m1": "Monthly (current)", "m2": "Monthly (next)"}
DAY_LIMIT_ON = False          # divas nu profit/loss limit ON/OFF (Telegram Settings ma thi badlay)
DAY_LOSS_LIMIT = 2000.0       # ₹ - divas no total loss aatlo thay to aaj nu trading band
DAY_PROFIT_LIMIT = 3000.0     # ₹ - divas no total profit aatlo thay to aaj nu trading band
DAY_LIMIT_STEP = 500.0
MAX_TRADES_ON = False        # divas na max trades nu limit ON/OFF
MAX_TRADES_PER_DAY = 5        # ON hoy tyare: divas ma max ketla trade. Aatla thay pachhi aakho divas trading band
ALERT_SOUND = True            # entry/exit Telegram alert sound sathe (False = silent notification)
CANDLE_BREAKOUT = True        # badhi strategy ma extra condition: close > pichhli candle High (bullish) / close < pichhli candle Low (bearish)
ONE_TRADE_PER_TREND = True   # profit/loss book thaya pachi same trend ma navi trade nahi (trend flip ni rah)
HISTORY_DAYS = None          # None = auto (timeframe mujab). Indicator ma aagal na divas na candles ketla divas na levana
ANGEL_MAX_DAYS = {1: 30, 3: 60, 5: 100, 10: 100, 15: 200, 30: 200}     # Angel One history limit per interval
HISTORY_PRESETS = [2, 3, 5, 6, 8, 10, 15, 20, 30, 45, 60, 100, 200]
LOOP_SECONDS = 3            # signal/exit check kitli vaare (pehla 20 sec hatu = entry late)
DASH_REFRESH = 2           # dashboard refresh seconds (live jevu). 2-3 sec Telegram mate safe
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
    "strategy": DEFAULT_STRATEGY,
    "last_event": "—",
    "pnl_cache": None,
    "spot_cache": None,
    "dash_msg_id": None,
    "dash_chat": None,
    "dash_t": 0.0,
    "dash_compact": False,   # True = Compact Dashboard
    "dash_last": "",         # last sent text (same hoy to edit na karvu)
    "mode": "paper",       # paper | live
    "running": False,
    "day_halt": None,      # divas limit hit thayu to reason (aaj nu trading band)
    "limit_override": False,  # True = aaj mate limit ignore (user e Resume karyu)
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


_login_lock = threading.RLock()
_last_login = [0.0]
SESSION_ERR_KEYS = ("ag8001", "ag8002", "ag8003", "invalid token", "token expired",
                    "session expired", "unauthor", "invalid session")


def _is_session_error(x) -> bool:
    """Exception text ke API response dict ma token/session expiry error che?"""
    if isinstance(x, dict):
        if x.get("status") in (True, "true", "True"):
            return False
        x = f"{x.get('errorCode', '')} {x.get('message', '')}"
    low = str(x).lower()
    return any(k in low for k in SESSION_ERR_KEYS)


def relogin(force=False):
    """Thread-safe relogin. 20 sec ma ek j vaar (bija thread e hamna j karyu hoy to skip)."""
    with _login_lock:
        if not force and time.time() - _last_login[0] < 20:
            return
        angel_login()


def throttled(fn, *args, retries=3, fast=False, **kwargs):
    """Angel API call: gap rakhe, 'access rate' par retry kare, ane Invalid Token (AG8001)
    aave to automatic relogin karine pacho call kare."""
    name = getattr(fn, "__name__", None)
    relogged = False
    for attempt in range(retries + 2):
        with (nullcontext() if fast else _api_lock):        # fast=True (orders): lock/gap vagar turant
            wait = 0 if fast else MIN_GAP - (time.time() - _last_call[0])
            if wait > 0:
                time.sleep(wait)
            try:
                # hamesha latest `smart` object vapro (relogin pachi navo object bane che)
                call = getattr(smart, name) if (name and hasattr(smart, name)) else fn
                res = call(*args, **kwargs)
                err = _is_session_error(res) if isinstance(res, dict) else False
                exc = None
            except Exception as e:
                res, err, exc = None, _is_session_error(e), e
            finally:
                if not fast:
                    _last_call[0] = time.time()
        if err and not relogged:
            log.warning("Session/token error (%s) -> relogin", exc or res)
            relogged = True
            relogin(force=True)
            continue
        if exc is not None:
            if "rate" in str(exc).lower() and attempt < retries:
                log.warning("Rate limit, retry %d", attempt + 1)
                time.sleep(2 * (attempt + 1))
                continue
            raise exc
        return res
    return res


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
    last = None
    for i in range(3):
        try:
            s_ = SmartConnect(api_key=ANGEL_API_KEY)
            if PROXY_URL:
                # badha Angel REST calls (login, candles, orders) static IP proxy thi jase
                px = {"http": PROXY_URL, "https": PROXY_URL}
                try:
                    s_.reqsession.proxies.update(px)
                except Exception:
                    s_ = SmartConnect(api_key=ANGEL_API_KEY, proxies=px)
            totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
            data = s_.generateSession(ANGEL_CLIENT_ID, ANGEL_PIN, totp)
            if not data.get("status"):
                raise RuntimeError(f"Angel login failed: {data}")
            smart = s_
            AUTH["jwt"] = data["data"]["jwtToken"]
            try:
                AUTH["feed"] = smart.getfeedToken()
            except Exception as e:
                log.warning("feed token nathi malyo: %s", e)
            _last_login[0] = time.time()
            log.info("Angel login OK")
            if PROXY_URL:
                try:
                    ip = requests.get("https://api.ipify.org", proxies={"http": PROXY_URL, "https": PROXY_URL},
                                      timeout=10).text.strip()
                    log.info("Proxy thi bahar nu IP: %s (aa j IP Angel One ma register hovo joie)", ip)
                except Exception as e:
                    log.warning("Proxy IP check fail: %s", e)
            # websocket ne navo token male e mate junu connection band karo (thread auto reconnect kare che)
            try:
                sws = ws_ctl.get("sws")
                if sws and ws_ctl.get("open"):
                    sws.close_connection()
            except Exception:
                pass
            return
        except Exception as e:
            last = e
            log.warning("Angel login try %d failed: %s", i + 1, e)
            time.sleep(3)      # TOTP navo code mate thodi vaar ruko
    raise RuntimeError(f"Angel login failed after retries: {last}")


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
    oid = throttled(smart.placeOrder, params, retries=0, fast=True)
    log.info("Order %s %s %s -> %s", side, opt["symbol"], qty, oid)
    if not oid or str(oid).lower() == "none":
        raise RuntimeError("Angel One e order id nathi aapyo (order nathi gayo)")
    return str(oid)


# ----------------------------- ANGEL ORDER CONFIRMATION ---------------
def confirm_order(oid, wait=8.0) -> dict:
    """Angel One orderBook thi order ni SACHI status + average fill price. Terminal status ke timeout sudhi poll."""
    info = {"oid": str(oid), "status": "unknown", "avg": None, "filled": 0, "text": ""}
    end = time.time() + wait
    time.sleep(0.5)
    while True:
        try:
            ob = throttled(smart.orderBook, fast=True)
            rows = (ob.get("data") or []) if isinstance(ob, dict) else []
            for o in rows:
                if str(o.get("orderid")) == str(oid):
                    info["status"] = str(o.get("orderstatus") or "unknown").lower()
                    info["text"] = str(o.get("text") or "")
                    try:
                        info["avg"] = float(o.get("averageprice") or 0) or None
                    except Exception:
                        pass
                    try:
                        info["filled"] = int(float(o.get("filledshares") or 0))
                    except Exception:
                        pass
                    break
        except Exception as e:
            info["text"] = f"orderbook error: {e}"
        if info["status"] in ("complete", "rejected", "cancelled") or time.time() > end:
            return info
        time.sleep(0.8)


def fill_ok(info) -> bool:
    return bool(info) and info.get("status") == "complete"


def _cancel_quiet(oid):
    if not oid:
        return
    try:
        throttled(smart.cancelOrder, str(oid), "NORMAL", fast=True)
    except Exception as e:
        log.warning("cancel %s fail: %s", oid, e)


def live_entry_orders(hedge_opt, sell_opt, qty) -> dict:
    """Pehla hedge BUY (fill confirm), pachhi sell leg SELL (fill confirm). Fail thay to safe rite rok."""
    out = {"ok": False, "msg": "", "sell_px": None, "hedge_px": None, "orders": {}}
    try:
        h_oid = place_order(hedge_opt, "BUY", qty)
    except Exception as e:
        out["msg"] = f"❌ ENTRY cancel: hedge BUY order nathi gayo.\n{e}\nKoi position nathi."
        return out
    h_info = confirm_order(h_oid)
    out["orders"]["hedge"] = h_info
    if not fill_ok(h_info):
        _cancel_quiet(h_oid)
        out["msg"] = (f"❌ ENTRY cancel: hedge BUY #{h_oid} {h_info['status']} {h_info['text']}\n"
                      f"Sell order moklyo nathi. Koi position nathi. (Angel One ma check karo)")
        return out
    try:
        s_oid = place_order(sell_opt, "SELL", qty)
        s_info = confirm_order(s_oid)
    except Exception as e:
        s_oid, s_info = None, {"oid": None, "status": "error", "avg": None, "filled": 0, "text": str(e)}
    out["orders"]["sell"] = s_info
    if not fill_ok(s_info):
        _cancel_quiet(s_oid)
        msg = f"❌ ENTRY fail: SELL leg {s_info['status']} {s_info['text']}\nHedge BUY thayelo hato, ene square-off kari rahyo chhu..."
        try:
            x_oid = place_order(hedge_opt, "SELL", qty)
            x_info = confirm_order(x_oid)
            msg += (f"\n↩️ Hedge SELL #{x_oid} {x_info['status']}" + (f" @ {x_info['avg']:.2f}" if x_info["avg"] else ""))
            if not fill_ok(x_info):
                msg += "\n⚠️ Hedge band na thayo! Angel One ma manually band karo."
        except Exception as e:
            msg += f"\n⚠️ Hedge square-off order fail: {e}\nAngel One ma manually hedge band karo!"
        out["msg"] = msg
        return out
    out["sell_px"], out["hedge_px"] = s_info["avg"], h_info["avg"]
    out["ok"] = True
    return out


def live_exit_orders(p) -> dict:
    """Pehla sell leg BUY back (fill confirm), pachhi hedge SELL. Sell leg band na thay to hedge nathi vechatu."""
    qty = p["qty"]
    out = {"ok": False, "msg": "", "s": None, "h": None, "orders": {}, "warn": ""}
    try:
        s_oid = place_order(p["sell"], "BUY", qty)
    except Exception as e:
        out["msg"] = f"❌ EXIT fail: sell leg BUY order nathi gayo.\n{e}\nPosition haju bot ma open che. 30 sec pachhi fari try thase."
        return out
    s_info = confirm_order(s_oid)
    out["orders"]["sell_exit"] = s_info
    if not fill_ok(s_info):
        _cancel_quiet(s_oid)
        out["msg"] = (f"❌ EXIT fail: sell leg BUY #{s_oid} {s_info['status']} {s_info['text']}\n"
                      f"Position haju bot ma open che (Angel One ma check karo). 30 sec pachhi fari try thase.")
        return out
    out["s"] = s_info["avg"] or ws_ltp(p["sell"]["token"]) or p["sell_entry"]
    try:
        h_oid = place_order(p["hedge"], "SELL", qty)
        h_info = confirm_order(h_oid)
    except Exception as e:
        h_oid, h_info = None, {"oid": None, "status": "error", "avg": None, "filled": 0, "text": str(e)}
    out["orders"]["hedge_exit"] = h_info
    if fill_ok(h_info):
        out["h"] = h_info["avg"] or ws_ltp(p["hedge"]["token"]) or p["hedge_entry"]
    else:
        _cancel_quiet(h_oid)
        out["h"] = ws_ltp(p["hedge"]["token"]) or p["hedge_entry"]
        out["warn"] = (f"⚠️ Hedge SELL {h_info['status']} {h_info['text']}\n"
                       f"Hedge Angel One ma haju open hoi shake, manually band karo! (PnL ma hedge price andaje che)")
    out["ok"] = True
    return out


def fills_text(orders, ltp_ref=None, exit_=False) -> str:
    """Telegram mate Angel One na sacha orders ni vigat."""
    if not orders:
        return ""
    names = ([("sell_exit", "BUY  (sell leg band)"), ("hedge_exit", "SELL (hedge band)")] if exit_
             else [("hedge", "BUY  hedge"), ("sell", "SELL leg")])
    refs = {}
    if ltp_ref and not exit_:
        refs = {"sell": ltp_ref[0], "hedge": ltp_ref[1]}
    L = ["📒 Angel One orders (sacha fill)"]
    for key, label in names:
        i = orders.get(key)
        if not i:
            continue
        ok = "✅" if i.get("status") == "complete" else "❌"
        px = f" @ {i['avg']:.2f}" if i.get("avg") else ""
        ref = f" (signal LTP {refs[key]:.2f})" if key in refs and refs[key] else ""
        L.append(f"{ok} {label} #{i.get('oid')} {i.get('status')}{px}{ref}")
    return "\n".join(L)


# ----------------------------- ANGEL -> BOT TRADE SYNC ---------------
ORDER_RE = re.compile(r"^NIFTY(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$")
_OID_COLS = ("sell_entry_oid", "hedge_entry_oid", "sell_exit_oid", "hedge_exit_oid")


def _fl(x) -> float:
    try:
        return float(x or 0)
    except Exception:
        return 0.0


def _parse_ts(o):
    for k in ("updatetime", "exchtime", "ordertime"):
        v = str(o.get(k) or "").strip()
        for fmt in ("%d-%b-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(v, fmt)
            except Exception:
                pass
    return None


def _wavg(rows) -> float:
    q = sum(x[0] for x in rows)
    return sum(x[0] * x[1] for x in rows) / q if q else 0.0


def angel_trade_groups(rows):
    """Angel orderBook na executed NIFTY option orders ne 'flat thi flat' round-trip (trade) ma vahenche."""
    execd = []
    for o in rows or []:
        sym = str(o.get("tradingsymbol") or "")
        filled, avg = _fl(o.get("filledshares")), _fl(o.get("averageprice"))
        if not ORDER_RE.match(sym) or filled <= 0 or avg <= 0:
            continue
        execd.append({"oid": str(o.get("orderid")), "sym": sym, "tok": str(o.get("symboltoken") or ""),
                      "side": str(o.get("transactiontype") or "").upper(), "qty": filled, "px": avg, "ts": _parse_ts(o)})
    execd.sort(key=lambda e: (e["ts"] or datetime.min, e["oid"]))
    groups, cur, net = [], [], {}
    for e in execd:
        cur.append(e)
        net[e["sym"]] = net.get(e["sym"], 0.0) + (e["qty"] if e["side"] == "BUY" else -e["qty"])
        if all(abs(v) < 0.5 for v in net.values()):
            groups.append(cur)
            cur, net = [], {}
    return groups, {k: v for k, v in net.items() if abs(v) >= 0.5}


def group_to_record(g):
    by = {}
    for e in g:
        by.setdefault(e["sym"], []).append(e)
    if len(by) != 2:
        return None
    legs = {}
    for sym, es in by.items():
        first = es[0]["side"]
        role = "sell" if first == "SELL" else "hedge"
        ex_side = "BUY" if first == "SELL" else "SELL"
        en = [(x["qty"], x["px"]) for x in es if x["side"] == first]
        ex = [(x["qty"], x["px"]) for x in es if x["side"] == ex_side]
        legs[role] = {"sym": sym, "entry": _wavg(en), "exit": _wavg(ex), "qty": sum(q for q, _ in en),
                      "oid_in": es[0]["oid"], "oid_out": es[-1]["oid"]}
    if set(legs) != {"sell", "hedge"}:
        return None
    S, H = legs["sell"], legs["hedge"]
    qty = S["qty"]
    sell_pts, hedge_pts = S["entry"] - S["exit"], H["exit"] - H["entry"]
    t0, t1 = g[0]["ts"], g[-1]["ts"]
    m = ORDER_RE.match(S["sym"])
    try:
        exp = str(datetime.strptime(m.group(1), "%d%b%y").date())
    except Exception:
        exp = ""
    return {
        "date": str((t0 or datetime.now()).date()),
        "entry_time": t0.strftime("%H:%M:%S") if t0 else "", "exit_time": t1.strftime("%H:%M:%S") if t1 else "",
        "time": t1.strftime("%H:%M:%S") if t1 else "",
        "duration_min": round((t1 - t0).total_seconds() / 60, 1) if t0 and t1 else "",
        "mode": "live", "strategy": "", "timeframe": "", "type": m.group(3), "expiry": exp,
        "lots": max(1, round(qty / LOT_SIZE)), "qty": qty, "entry_spot": "", "exit_spot": "",
        "sell_symbol": S["sym"], "sell_entry": round(S["entry"], 2), "sell_exit": round(S["exit"], 2),
        "sell_pnl": round(sell_pts * qty, 2),
        "hedge_symbol": H["sym"], "hedge_entry": round(H["entry"], 2), "hedge_exit": round(H["exit"], 2),
        "hedge_pnl": round(hedge_pts * qty, 2),
        "net_credit_pts": round(S["entry"] - H["entry"], 2), "net_exit_pts": round(S["exit"] - H["exit"], 2),
        "net_points": round(sell_pts + hedge_pts, 2), "pnl": round((sell_pts + hedge_pts) * qty, 2),
        "sl_rule": "", "tp_rule": "", "reason": "Angel thi sync", "price_src": "angel",
        "sell_entry_oid": S["oid_in"], "hedge_entry_oid": H["oid_in"],
        "sell_exit_oid": S["oid_out"], "hedge_exit_oid": H["oid_out"],
        "charges": "", "net_pnl": "",
    }


def _known_trades():
    """Bot ma pehla thi record thayela trades: order IDs + (date, symbol, entry time)."""
    ids, sigs = set(), []
    for r in list(state["trades"]) + read_history():
        for k in _OID_COLS:
            if r.get(k):
                ids.add(str(r[k]))
        try:
            et = datetime.strptime(str(r.get("entry_time") or ""), "%H:%M:%S")
        except Exception:
            et = None
        sigs.append((str(r.get("date")), str(r.get("sell_symbol")), et))
    return ids, sigs


def _is_known(rec, g_ids, known) -> bool:
    ids, sigs = known
    if g_ids & ids:
        return True
    try:
        t0 = datetime.strptime(rec["entry_time"], "%H:%M:%S")
    except Exception:
        t0 = None
    for d_, sym, et in sigs:
        if d_ == rec["date"] and sym == rec["sell_symbol"] and (
                et is None or t0 is None or abs((et - t0).total_seconds()) <= 180):
            return True
    return False


def sync_angel_trades() -> dict:
    """Angel orderBook thi aaj na trades shodhe je bot ma record nathi (blocking; thread ma chalavo)."""
    ob = throttled(smart.orderBook)
    if not isinstance(ob, dict) or not ob.get("status"):
        return {"error": f"Orderbook nathi malyu: {ob}"}
    groups, open_legs = angel_trade_groups(ob.get("data") or [])
    known = _known_trades()
    added, skipped = [], 0
    for g in groups:
        rec = group_to_record(g)
        if not rec:
            continue
        if _is_known(rec, {e["oid"] for e in g}, known):
            skipped += 1
            continue
        try:
            ch = fetch_charges([{"side": e["side"], "symbol": e["sym"], "token": e["tok"], "qty": e["qty"],
                                 "price": e["px"]} for e in g])
            if ch[0] is not None:
                rec["charges"], rec["net_pnl"] = round(ch[0], 2), round(rec["pnl"] - ch[0], 2)
        except Exception:
            pass
        added.append(rec)
    return {"added": added, "skipped": skipped, "groups": len(groups), "open": open_legs}


async def apply_angel_sync() -> str:
    res = await asyncio.to_thread(sync_angel_trades)
    if res.get("error"):
        return "❌ " + res["error"]
    today = str(datetime.now().date())
    for rec in res["added"]:
        if rec["date"] == today:
            state["trades"].append(rec)
            state["realized_pnl"] += rec["pnl"]
        append_history(rec)
    if res["added"]:
        save_state()
    L = [f"📥 Angel sync: {res['groups']} trade Angel ma • {len(res['added'])} navu bot ma umeryu • "
         f"{res['skipped']} pehla thi hatu"]
    for rec in res["added"]:
        L.append(f"➕ {rec['entry_time']} {rec['type']} {_strike_txt(rec['sell_symbol'])}/{_strike_txt(rec['hedge_symbol'])} "
                 f"→ {money(rec['pnl'])}" + (f" (charges ₹{rec['charges']:,.2f})" if rec["charges"] != "" else ""))
    if res["open"] and not state["position"]:
        L.append("⚠️ Angel ma open position che je bot ne khabar nathi (bot ma position nathi):")
        for sym, q in res["open"].items():
            L.append(f"   {sym}: {'+' if q > 0 else ''}{q:g}")
        L.append("Angel app ma check karo / band karo.")
    return "\n".join(L)


_chg_cache = {"sig": None, "t": 0.0, "val": None}


def _flatten_breakup(items, out):
    for it in items or []:
        sub = it.get("breakup") or []
        if sub:
            _flatten_breakup(sub, out)
            continue
        try:
            amt = float(it.get("amount") or 0)
        except Exception:
            amt = 0.0
        if amt:
            nm = str(it.get("name") or "?")
            out[nm] = out.get(nm, 0.0) + amt


def fetch_charges(orders: list):
    """Angel One Brokerage Calculator (estimateCharges) thi executed orders na charges.
    orders: [{'side','symbol','token','qty','price'}]. Return (total, breakup_dict) athva (None, error_text)."""
    if not orders:
        return 0.0, {}
    try:
        smart.estimateCharges
    except AttributeError:
        return None, "smartapi version ma estimateCharges nathi (pip install -U smartapi-python)"
    total, brk, last_err = 0.0, {}, ""
    for i in range(0, len(orders), 10):
        chunk, done = orders[i:i + 10], False
        # Angel API na format ma thodi chokkas nathi -> 3 variant try kare
        for sym_mode, int_price in (("under", False), ("full", False), ("under", True)):
            params = {"orders": [{
                "product_type": PRODUCT_TYPE, "transaction_type": o["side"], "quantity": str(int(o["qty"])),
                "price": str(int(round(o["price"]))) if int_price else f"{o['price']:.2f}",
                "exchange": "NFO", "symbol_name": "NIFTY" if sym_mode == "under" else o["symbol"],
                "token": str(o["token"])} for o in chunk]}
            try:
                r = throttled(smart.estimateCharges, params, retries=1)
            except Exception as e:
                last_err = str(e)[:80]
                continue
            d = (r or {}).get("data") if isinstance(r, dict) else None
            if not (isinstance(r, dict) and r.get("status") and d):
                last_err = str((r or {}).get("message") or r)[:80] if isinstance(r, dict) else str(r)[:80]
                continue
            summ = d.get("summary") or d
            part = {}
            _flatten_breakup(summ.get("breakup") or d.get("breakup") or [], part)
            try:
                tc = float(summ.get("total_charges"))
            except Exception:
                tc = sum(part.values())
            total += tc
            for k, v in part.items():
                brk[k] = brk.get(k, 0.0) + v
            done = True
            break
        if not done:
            return None, last_err or "charges nathi malya"
    return total, brk


def _chg_line(brk: dict) -> str:
    top = sorted(brk.items(), key=lambda kv: -kv[1])[:6]
    return " • ".join(f"{k.replace('Angel One ', '')} {v:,.2f}" for k, v in top)


def _orders_pages(max_len=3400) -> list:
    """Aaj na badha Angel One orders + niche summary (buy/sell value, charges, net)."""
    ob = throttled(smart.orderBook)
    if not isinstance(ob, dict) or not ob.get("status"):
        return [f"❌ Orderbook nathi malyu: {ob}"]
    rows = ob.get("data") or []
    if not rows:
        return ["📒 Angel One: aaj koi order nathi."]
    ICON = {"complete": "✅", "rejected": "❌", "cancelled": "⏹", "open": "⏳", "trigger pending": "⏳"}

    def fl(x):
        try:
            return float(x or 0)
        except Exception:
            return 0.0

    lines, cnt = [], {}
    buy_v = sell_v = buy_q = sell_q = 0.0
    execd = []
    for i, o in enumerate(rows, 1):
        st = str(o.get("orderstatus") or "").lower()
        cnt[st] = cnt.get(st, 0) + 1
        t = str(o.get("updatetime") or o.get("exchtime") or "")[-8:]
        side = str(o.get("transactiontype") or "").upper()
        avg, filled = fl(o.get("averageprice")), fl(o.get("filledshares"))
        px = avg or fl(o.get("price"))
        txt = f" - {str(o.get('text'))[:38]}" if st == "rejected" and o.get("text") else ""
        lines.append(f"{i}. {t} {side} {_strike_txt(o.get('tradingsymbol'))} x{o.get('quantity')} "
                     f"@{px:.2f} {ICON.get(st, '❔')}{txt}")
        if filled > 0 and avg > 0:
            if side == "BUY":
                buy_v, buy_q = buy_v + avg * filled, buy_q + filled
            else:
                sell_v, sell_q = sell_v + avg * filled, sell_q + filled
            execd.append({"side": side, "symbol": o.get("tradingsymbol"), "token": o.get("symboltoken"),
                          "qty": filled, "price": avg, "oid": o.get("orderid")})
    # ---- summary ----
    done_, rej, can = cnt.get("complete", 0), cnt.get("rejected", 0), cnt.get("cancelled", 0)
    other = len(rows) - done_ - rej - can
    S = [LINE, "📊 AAJ NO SUMMARY",
         f"Orders: {len(rows)}  (✅ {done_} | ❌ {rej} | ⏹ {can}" + (f" | ⏳ {other}" if other else "") + ")",
         f"Buy : ₹{buy_v:,.0f}  ({buy_q:g} qty)",
         f"Sell: ₹{sell_v:,.0f}  ({sell_q:g} qty)"]
    gross = sell_v - buy_v
    note = "  (open position/adhuri qty, final nathi)" if (state.get("position") or abs(buy_q - sell_q) > 0.5) else ""
    S.append(f"Gross: {money(gross)}{note}")
    try:
        n_ang = len(angel_trade_groups(rows)[0])
        S.append(f"🔁 Angel round-trips: {n_ang} | Bot ma record: {len(state['trades'])}"
                 + ("  → bot ma umervā /sync" if n_ang != len(state["trades"]) else ""))
    except Exception:
        pass
    sig = tuple((e["oid"], e["qty"], e["price"]) for e in execd)
    if _chg_cache["sig"] == sig and time.time() - _chg_cache["t"] < 60:
        res = _chg_cache["val"]
    else:
        res = fetch_charges(execd) if execd else (0.0, {})
        if res[0] is not None:
            _chg_cache.update(sig=sig, t=time.time(), val=res)
    if res[0] is None:
        S.append(f"💸 Charges: Angel thi nathi malya ({res[1]})")
    else:
        S.append(f"💸 Charges (Angel est.): ₹{res[0]:,.2f}")
        if res[1]:
            S.append("   " + _chg_line(res[1]))
        S.append(f"💰 Net after charges: {money(gross - res[0])}{note}")
    # ---- pages (summary hamesha chhelle) ----
    head = f"📒 Angel One orders • aaj ({len(rows)})"
    pages, cur = [], [head]
    for ln in lines:
        if sum(len(x) + 1 for x in cur) + len(ln) > max_len:
            pages.append("\n".join(cur))
            cur = [f"{head} (chalu)"]
        cur.append(ln)
    summ_txt = "\n".join(S)
    if sum(len(x) + 1 for x in cur) + len(summ_txt) > 4000:
        pages.append("\n".join(cur))
        cur = []
    pages.append("\n".join(cur + [summ_txt]) if cur else summ_txt)
    return pages


def _orders_text() -> str:
    return "\n\n".join(_orders_pages())


# ----------------------------- PERSISTENCE ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Railway Volume vapro to Variables ma DATA_DIR=/data set karo (files tya save thase, deploy par delete nahi thay)
DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")        # restart par state pacho aave
HISTORY_FILE = os.path.join(DATA_DIR, "trades_history.csv")  # badha divas na trades (kayam)
HISTORY_COLS = ["date", "entry_time", "exit_time", "duration_min", "mode", "strategy", "timeframe", "type",
                "expiry", "lots", "qty", "entry_spot", "exit_spot",
                "sell_symbol", "sell_entry", "sell_exit", "sell_pnl",
                "hedge_symbol", "hedge_entry", "hedge_exit", "hedge_pnl",
                "net_credit_pts", "net_exit_pts", "net_points", "pnl", "sl_rule", "tp_rule", "reason",
                "price_src", "sell_entry_oid", "hedge_entry_oid", "sell_exit_oid", "hedge_exit_oid",
                "charges", "net_pnl"]
_last_save = [0.0]
restore_notes = []


def save_state():
    try:
        PROFILES[state["mode"]] = snapshot_settings()
        data = {
            "day": str(state["day"]) if state["day"] else str(datetime.now().date()),
            "mode": state["mode"],
            "running": state["running"],
            "dash_msg_id": state["dash_msg_id"], "dash_chat": state["dash_chat"],
            "dash_compact": state["dash_compact"],
            "strategy": state["strategy"],
            "blocked_dir": state["blocked_dir"],
            "day_halt": state["day_halt"],
            "limit_override": state["limit_override"],
            "realized_pnl": state["realized_pnl"],
            "trades": state["trades"],
            "position": state["position"],
            "last_event": state["last_event"],
            "settings": PROFILES[state["mode"]],
            "profiles": PROFILES,
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


PROFILES = {"paper": {}, "live": {}}       # paper ane live mate alag alag setting
_cfg_lock = threading.RLock()              # settings swap thay tyare worker threads ne wrong value na dekhay


def snapshot_settings() -> dict:
    """Atyaar ni (active mode ni) badhi setting."""
    return {"LOTS": LOTS, "HEDGE_PCT": HEDGE_PCT, "SELL_OFFSET": SELL_OFFSET, "SL_MODE": SL_MODE, "SL_VALUE": SL_VALUE,
            "TP_MODE": TP_MODE, "TP_VALUE": TP_VALUE, "EXIT_ON_FLIP": EXIT_ON_FLIP,
            "ENTRY_START": ENTRY_START.strftime("%H:%M"), "LAST_ENTRY": LAST_ENTRY.strftime("%H:%M"),
            "FORCE_EXIT": FORCE_EXIT.strftime("%H:%M"),
            "ONE_TRADE_PER_TREND": ONE_TRADE_PER_TREND, "HISTORY_DAYS": HISTORY_DAYS,
            "EXPIRY_MODE": EXPIRY_MODE, "EXPIRY_SKIP_TODAY": EXPIRY_SKIP_TODAY, "CANDLE_MINUTES": CANDLE_MINUTES,
            "DAY_LIMIT_ON": DAY_LIMIT_ON, "DAY_LOSS_LIMIT": DAY_LOSS_LIMIT, "DAY_PROFIT_LIMIT": DAY_PROFIT_LIMIT,
            "MAX_TRADES_ON": MAX_TRADES_ON, "MAX_TRADES_PER_DAY": MAX_TRADES_PER_DAY, "ALERT_SOUND": ALERT_SOUND,
            "CANDLE_BREAKOUT": CANDLE_BREAKOUT,
            "strategy": state["strategy"]}


def apply_settings(st: dict):
    """Setting dict ne globals ma lagu kare (mode badalta / file load thata)."""
    global LOTS, HEDGE_PCT, SL_MODE, SL_VALUE, TP_MODE, TP_VALUE, EXIT_ON_FLIP
    global ENTRY_START, LAST_ENTRY, FORCE_EXIT, ONE_TRADE_PER_TREND, HISTORY_DAYS, EXPIRY_MODE, EXPIRY_SKIP_TODAY
    global DAY_LIMIT_ON, DAY_LOSS_LIMIT, DAY_PROFIT_LIMIT, SELL_OFFSET, MAX_TRADES_PER_DAY, MAX_TRADES_ON, ALERT_SOUND, CANDLE_BREAKOUT
    st = st or {}
    CANDLE_BREAKOUT = bool(st.get("CANDLE_BREAKOUT", CANDLE_BREAKOUT))
    _mt = int(st.get("MAX_TRADES_PER_DAY", MAX_TRADES_PER_DAY))
    MAX_TRADES_PER_DAY = max(1, min(100, _mt)) if _mt > 0 else MAX_TRADES_PER_DAY
    # juni file ma ON/OFF nahotu: count > 0 hoy to ON gano
    MAX_TRADES_ON = bool(st["MAX_TRADES_ON"]) if "MAX_TRADES_ON" in st else (_mt > 0)
    ALERT_SOUND = bool(st.get("ALERT_SOUND", ALERT_SOUND))
    LOTS = int(st.get("LOTS", LOTS))
    HEDGE_PCT = float(st.get("HEDGE_PCT", HEDGE_PCT))
    SELL_OFFSET = max(-5, min(5, int(st.get("SELL_OFFSET", SELL_OFFSET))))
    if st.get("SL_MODE") in MODE_INFO and st.get("TP_MODE") in MODE_INFO:
        SL_MODE, SL_VALUE = st["SL_MODE"], float(st.get("SL_VALUE", SL_VALUE))
        TP_MODE, TP_VALUE = st["TP_MODE"], float(st.get("TP_VALUE", TP_VALUE))
    elif "SL_PCT" in st:                     # juna version nu state file
        SL_MODE, SL_VALUE = "pct", float(st["SL_PCT"]) * 100
        TP_MODE, TP_VALUE = "pct", float(st.get("TARGET_PCT", 0.5)) * 100
    EXIT_ON_FLIP = bool(st.get("EXIT_ON_FLIP", EXIT_ON_FLIP))
    DAY_LIMIT_ON = bool(st.get("DAY_LIMIT_ON", DAY_LIMIT_ON))
    DAY_LOSS_LIMIT = float(st.get("DAY_LOSS_LIMIT", DAY_LOSS_LIMIT))
    DAY_PROFIT_LIMIT = float(st.get("DAY_PROFIT_LIMIT", DAY_PROFIT_LIMIT))

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
    if st.get("strategy") in STRATEGIES:
        state["strategy"] = st["strategy"]
    _sig_cache["val"], _sig_cache["bucket"] = None, None


def _with_profile(prof: str, fn):
    """Fn ne 'prof' ni setting ma chalave (sync, koi await nahi). Active mode na hoy to globals thodi vaar swap karine pacha."""
    active = state["mode"]
    if prof == active:
        _view["inactive"] = False
        return fn()
    if not _cfg_lock.acquire(timeout=4):
        raise RuntimeError("Bot thodo busy che, 2 second pachhi fari dabavo")
    try:
        PROFILES[active] = snapshot_settings()
        sc = dict(_sig_cache)
        apply_settings(PROFILES[prof] or PROFILES[active])
        _view["inactive"] = True
        try:
            return fn()
        finally:
            PROFILES[prof] = snapshot_settings()
            apply_settings(PROFILES[active])
            _sig_cache.update(sc)
            _view["inactive"] = False
    finally:
        _cfg_lock.release()


def copy_profile(prof: str):
    """Bija mode ni setting 'prof' ma copy. Error text athva None."""
    other = "live" if prof == "paper" else "paper"
    active = state["mode"]
    if prof == active and state["position"]:
        return "Position open che, pehla exit karo"
    src = snapshot_settings() if other == active else PROFILES.get(other)
    if not src:
        return f"{other.upper()} ni setting haju save nathi"
    PROFILES[prof] = dict(src)
    if prof == active:
        apply_settings(PROFILES[prof])
        state["running"] = False
        state["last_signal_candle"] = None
    return None


def _do_action(d: str, inactive: bool):
    """Setting button no sync action. Return (toast, alert)."""
    global LOTS, HEDGE_PCT, SL_MODE, SL_VALUE, TP_MODE, TP_VALUE, EXIT_ON_FLIP, ONE_TRADE_PER_TREND
    global HISTORY_DAYS, EXPIRY_MODE, EXPIRY_SKIP_TODAY, DAY_LIMIT_ON, DAY_LOSS_LIMIT, DAY_PROFIT_LIMIT, SELL_OFFSET
    global MAX_TRADES_PER_DAY, MAX_TRADES_ON, ALERT_SOUND, CANDLE_BREAKOUT
    if d.startswith("set_"):
        _, key, val = d.split("_", 2)
        v = int(val)
        if key == "lots":
            LOTS = max(1, min(50, LOTS + v))
        elif key == "soff":
            SELL_OFFSET = max(-5, min(5, SELL_OFFSET + v))
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
        elif key == "dlim":
            DAY_LIMIT_ON = not DAY_LIMIT_ON
            if not inactive:
                if not DAY_LIMIT_ON:
                    state["day_halt"] = None
                state["limit_override"] = False
        elif key == "dll":
            DAY_LOSS_LIMIT = max(DAY_LIMIT_STEP, min(200000.0, DAY_LOSS_LIMIT + v * DAY_LIMIT_STEP))
        elif key == "dlp":
            DAY_PROFIT_LIMIT = max(DAY_LIMIT_STEP, min(200000.0, DAY_PROFIT_LIMIT + v * DAY_LIMIT_STEP))
        elif key in ("mtd", "mton"):
            if key == "mton":
                MAX_TRADES_ON = not MAX_TRADES_ON
            else:
                MAX_TRADES_PER_DAY = max(1, min(100, MAX_TRADES_PER_DAY + v))
            if not inactive and str(state.get("day_halt") or "").startswith("MAX TRADES") \
                    and max_trades_reason(after_close=True) is None:
                state["day_halt"] = None                       # limit vadhari/OFF karyu to trading pacho chalu
        elif key == "snd":
            ALERT_SOUND = not ALERT_SOUND
        elif key == "cbo":
            CANDLE_BREAKOUT = not CANDLE_BREAKOUT
            if not inactive:
                _sig_cache["val"], _sig_cache["bucket"] = None, None      # signal pharithi gano
        elif key == "dres":
            if inactive:
                return "Aa mode chalu nathi", True
            state["day_halt"] = None
            state["limit_override"] = True
            state["last_signal_candle"] = None
        return "Updated", False
    if d.startswith("exp_"):
        key = d[4:]
        if key == "skip":
            EXPIRY_SKIP_TODAY = not EXPIRY_SKIP_TODAY
        elif key in EXPIRY_MODES:
            EXPIRY_MODE = key
        return f"Expiry: {expiry_label()}", False
    if d.startswith("rule_"):
        _, key, val = d.split("_", 2)
        v = int(val)
        if key in ("tstart", "tlast", "tend"):
            which = {"tstart": "start", "tlast": "last", "tend": "end"}[key]
            cur = {"start": ENTRY_START, "last": LAST_ENTRY, "end": FORCE_EXIT}[which]
            err = set_time_value(which, _shift_time(cur, v * 5))
            if err:
                return err, True
        elif key == "otpt":
            ONE_TRADE_PER_TREND = not ONE_TRADE_PER_TREND
            if not ONE_TRADE_PER_TREND and not inactive:
                state["blocked_dir"] = None
        elif key == "hist":
            if state["position"] and not inactive:
                return "Position open che, pehla exit karo", True
            HISTORY_DAYS = step_history(v)
            if not inactive:
                _sig_cache["val"], _sig_cache["bucket"] = None, None
                state["last_signal_candle"] = None
                state["running"] = False
        return "Updated", False
    if d.startswith("tf_"):
        if state["position"] and not inactive:
            return "Position open che, pehla exit karo", True
        m = int(d[3:])
        if m in TIMEFRAMES:
            set_timeframe(m)
            if not inactive:
                state["last_signal_candle"] = None
                state["running"] = False
        return f"Timeframe: {CANDLE_MINUTES} min" + ("" if inactive else " • Bot paused"), False
    if d.startswith("strat_"):
        if state["position"] and not inactive:
            return "Position open che, pehla exit karo", True
        key = d[6:]
        if key in STRATEGIES:
            state["strategy"] = key
            if not inactive:
                _sig_cache["val"], _sig_cache["bucket"] = None, None
                state["last_signal_candle"] = None
                state["running"] = False
        return f"Strategy: {strategy_label()}" + ("" if inactive else " • Bot paused"), False
    return "", False


def _prefix_kb(kb, prof: str, back_cb: str):
    """Badha callbacks ne 'cfg|<prof>|' lagavo, ⬅️ Back ne back_cb par fervo."""
    rows = []
    for row in kb.inline_keyboard:
        new = []
        for b in row:
            cd = b.callback_data
            if cd == "noop" or cd is None:
                new.append(b)
            elif cd == "menu" and b.text.startswith("⬅️"):
                new.append(Btn(b.text, callback_data=back_cb))
            elif cd == "menu":
                new.append(b)                                   # 🏠 Menu
            else:
                new.append(Btn(b.text, callback_data=f"cfg|{prof}|{cd}"))
        rows.append(new)
    return Markup(rows)


def cfg_handle(prof: str, d: str):
    """Setting action + page, 'prof' mode ni setting par. Return (toast, alert, text, kb)."""
    out = {"toast": "", "alert": False}
    icon = "⚡ LIVE" if prof == "live" else "📝 PAPER"

    def run():
        inactive = _view["inactive"]
        if d not in ("settings", "expiry", "rules", "strategy"):
            out["toast"], out["alert"] = _do_action(d, inactive)
        if d == "expiry" or d.startswith("exp_"):
            t, k, back = expiry_text(), expiry_kb(), "settings"
        elif d == "rules" or d.startswith("rule_"):
            t, k, back = rules_text(), rules_kb(), "settings"
        elif d == "strategy" or d.startswith(("tf_", "strat_")):
            t, k, back = strategy_text(), strategy_kb(), f"cfg|{prof}|settings"
            t = f"{icon} profile\n" + t
        else:
            return settings_text(prof), _prefix_kb(settings_kb(prof), prof, "settings")
        if not t.startswith(icon):
            t = f"{icon} profile\n" + t
        return t, _prefix_kb(k, prof, back)

    text, kb = _with_profile(prof, run)
    return out["toast"], out["alert"], text, kb


def settings_hub():
    """Settings ni pehli screen: Paper ane Live alag button."""
    def summ(prof):
        def f():
            return (f"Lots {LOTS} • {sell_label(SELL_OFFSET)} • Hedge {HEDGE_PCT * 100:.0f}% • "
                    f"SL {limit_short(SL_MODE, SL_VALUE)} • TP {limit_short(TP_MODE, TP_VALUE)}\n"
                    f"   {strategy_label()} {CANDLE_MINUTES}m • {ENTRY_START:%H:%M}-{LAST_ENTRY:%H:%M} • "
                    f"Limit {'ON' if DAY_LIMIT_ON else 'OFF'}")
        return _with_profile(prof, f)
    act = state["mode"]
    text = (f"⚙️ Settings\nChalu mode: {'⚡ LIVE' if act == 'live' else '📝 PAPER'}\n\n"
            f"📝 PAPER{' ✅' if act == 'paper' else ''}\n   {summ('paper')}\n\n"
            f"⚡ LIVE{' ✅' if act == 'live' else ''}\n   {summ('live')}\n\n"
            "Kai mode ni setting badalvi che? 👇\n(Banne ni setting alag save thay che)")
    kb = Markup([[Btn("📝 Paper settings", callback_data="cfg|paper|settings"),
                  Btn("⚡ Live settings", callback_data="cfg|live|settings")],
                 [Btn("🏠 Menu", callback_data="menu")]])
    return text, kb


def mode_trades() -> list:
    """Aaj na trades fakt atyaar na mode na (paper / live alag)."""
    return [t for t in state["trades"] if t.get("mode", state["mode"]) == state["mode"]]


def mode_realized() -> float:
    return sum(float(t.get("pnl") or 0) for t in mode_trades())


def switch_mode(new: str):
    """Mode badalo: juna mode ni setting save, navi mode ni setting load. Bot pause (surprise entry na thay)."""
    old = state["mode"]
    if new == old:
        return
    PROFILES[old] = snapshot_settings()
    state["mode"] = new
    apply_settings(PROFILES.get(new) or PROFILES[old])      # pehli vaar: juni setting nu copy
    PROFILES[new] = snapshot_settings()
    state["running"] = False
    state["last_signal_candle"] = None
    state["limit_override"] = False
    state["day_halt"] = day_limit_reason(mode_realized())
    save_state()


def load_state():
    global LOTS, HEDGE_PCT, SL_MODE, SL_VALUE, TP_MODE, TP_VALUE, EXIT_ON_FLIP
    global ENTRY_START, LAST_ENTRY, FORCE_EXIT, ONE_TRADE_PER_TREND, HISTORY_DAYS, EXPIRY_MODE, EXPIRY_SKIP_TODAY
    global DAY_LIMIT_ON, DAY_LOSS_LIMIT, DAY_PROFIT_LIMIT, SELL_OFFSET
    if not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            d = json.load(f)
    except Exception as e:
        log.warning("state file vachay nahi: %s", e)
        return
    today = datetime.now().date()
    st0 = d.get("settings", {})
    legacy = dict(st0, strategy=d.get("strategy"))
    profs = d.get("profiles") or {}
    PROFILES["paper"] = profs.get("paper") or dict(legacy)      # juna file ma banne mate same setting
    PROFILES["live"] = profs.get("live") or dict(legacy)
    state["mode"] = d.get("mode", "paper") if d.get("mode") in ("paper", "live") else "paper"
    apply_settings(PROFILES[state["mode"]])
    state["mode"] = d.get("mode", "paper")
    state["dash_msg_id"] = d.get("dash_msg_id")
    state["dash_chat"] = d.get("dash_chat")
    state["dash_compact"] = bool(d.get("dash_compact", False))
    if d.get("running"):
        state["running"] = True
        restore_notes.append(f"▶️ Bot pachho RUNNING ({state['mode'].upper()} mode) - /stop thi band karo.")
    if d.get("strategy") in STRATEGIES:
        state["strategy"] = d["strategy"]
    state["last_event"] = d.get("last_event", "—")
    state["day"] = today
    if d.get("day") == str(today):
        state["trades"] = d.get("trades", [])
        state["blocked_dir"] = d.get("blocked_dir")
        state["day_halt"] = d.get("day_halt")
        state["limit_override"] = bool(d.get("limit_override", False))
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


_poll = {"batch": True, "warn_t": 0.0}


def _poll_once():
    """Je tokens na price 3 sec thi juna che (WS nathi/tick nathi) te REST thi ek j batch call ma lavo."""
    EX = {1: "NSE", 2: "NFO"}
    stale = {}
    for et, toks in list(ws_wanted.items()):
        for t in list(toks):
            v = ws_prices.get(str(t))
            if not v or time.time() - v[1] > 3:
                stale.setdefault(et, []).append(str(t))
    req = {EX[et]: toks for et, toks in stale.items() if et in EX and toks}
    if not req:
        return
    if _poll["batch"]:
        try:
            fn = smart.getMarketData
        except AttributeError:
            _poll["batch"] = False         # juno smartapi version -> ltpData fallback
        else:
            r = throttled(fn, "LTP", req, retries=1)
            rows = ((r or {}).get("data") or {}).get("fetched") if isinstance(r, dict) else None
            for row in rows or []:
                tok, px = str(row.get("symbolToken", "")).strip(), row.get("ltp")
                if tok and px is not None:
                    ws_prices[tok] = (float(px), time.time())
            return
    # fallback: Nifty + position na 2 legs (ek ek call)
    ws_prices[NIFTY_TOKEN] = (ltp("NSE", "Nifty 50", NIFTY_TOKEN), time.time())
    pos = state.get("position")
    if pos:
        for leg in (pos["sell"], pos["hedge"]):
            ws_prices[str(leg["token"])] = (ltp("NFO", leg["symbol"], leg["token"]), time.time())


def _price_poller():
    while True:
        time.sleep(1.5)
        try:
            now = datetime.now()
            if smart is None or now.weekday() >= 5 or not (dtime(9, 14) <= now.time() <= dtime(15, 31)):
                continue
            _poll_once()
        except Exception as e:
            if time.time() - _poll["warn_t"] > 60:
                _poll["warn_t"] = time.time()
                log.warning("price poll error: %s", e)


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


def sell_label(off: int) -> str:
    return "ATM" if off == 0 else (f"OTM-{off}" if off > 0 else f"ITM-{-off}")


def sell_strike_for(opt_type: str, atm: float) -> float:
    """OTM: PE ma niche, CE ma upar. ITM: ulatu."""
    sign = -1 if opt_type == "PE" else 1
    return atm + sign * SELL_OFFSET * STRIKE_STEP


def plan_text(d: int, atm: float) -> str:
    if d == 0:
        return "Wait (no trade)"
    if ONE_TRADE_PER_TREND and state["blocked_dir"] == d and not state["position"]:
        return "Wait for trend flip"
    ot = "PE" if d == 1 else "CE"
    return f"SELL {sell_strike_for(ot, atm):.0f} {ot} ({sell_label(SELL_OFFSET)}) + hedge"


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


# ------------------------ CUSTOM STRATEGY ENGINE ----------------------
CUSTOM_FILE = os.path.join(DATA_DIR, "custom_strategies.json")   # tamari banavel strategy kayam save
_PRICE_NAMES = {"close", "open", "high", "low"}
_FUNCS = {"ema": 1, "sma": 1, "wma": 1, "rsi": 1, "rsiwma": 2, "macd": 2, "macdsig": 3, "st": 2, "stdir": 2}
_OP_RE = re.compile(r"^(.+?)(>=|<=|==|=|>|<)(.+)$")
_OPND_RE = re.compile(r"^([a-z]+)(?:\(([\d.,]+)\))?(?:\[(\d+)\])?$")
_MIRROR_OP = {">": "<", "<": ">", ">=": "<=", "<=": ">=", "=": "="}
CUSTOM_HELP = (
    "➕ Navi strategy banavo\n"
    "━━━━━━━━━━━━━━━━━━\n"
    "Niche mujab ek j message ma moklo 👇\n\n"
    "name: RSI Trend\n"
    "bullish: close > ema(20) and rsi(14) > 55\n"
    "bearish: close < ema(20) and rsi(14) < 45\n\n"
    "• bullish = PE spread sell, bearish = CE spread sell\n"
    "• 'bearish:' na lakho to bullish ni ulti condition aapo-aap banse (RSI no number 100-x thase)\n"
    "• Ek line ma ghani condition 'and' thi jodo (badhi sachi hovi joiye)\n"
    "• Koi condition match na thay to pehlano trend chalu rahe\n"
    "• Candle breakout ON hoy to te pan badhi strategy ma lagu pade\n\n"
    "Vapari shakay tevu:\n"
    "close, open, high, low  (pachhli candle: high[1], low[1])\n"
    "ema(n)  sma(n)  wma(n)  rsi(n)  rsiwma(n,m)\n"
    "macd(fast,slow)  macdsig(fast,slow,sig)\n"
    "st(period,mult) = supertrend line,  stdir(period,mult) = up / down\n"
    "Operators: >  <  >=  <=  =\n\n"
    "Example 2:\n"
    "name: ST + EMA\n"
    "bullish: stdir(10,3) = up and ema(9) > ema(21)\n\n"
    "Example 3:\n"
    "name: Breakout\n"
    "bullish: close > high[1] and close > sma(20)"
)


def _parse_operand(t: str):
    t = t.strip().lower().replace(" ", "")
    if not t:
        raise ValueError("Khali operand")
    if t in ("up", "down"):
        return ("num", 1.0 if t == "up" else -1.0)
    try:
        return ("num", float(t))
    except ValueError:
        pass
    m = _OPND_RE.match(t)
    if not m:
        raise ValueError(f"'{t}' samjayu nahi")
    name, args, shift = m.group(1), m.group(2), int(m.group(3) or 0)
    if shift > 50:
        raise ValueError("Shift [k] max 50")
    if name in _PRICE_NAMES and args is None:
        return ("price", name, shift, f"{name}[{shift}]" if shift else name)
    if name in _FUNCS:
        if args is None:
            raise ValueError(f"{name}(...) ma number joiye")
        try:
            a = [float(x) for x in args.split(",") if x != ""]
        except ValueError:
            raise ValueError(f"{name}({args}) ma number kharab che")
        if len(a) != _FUNCS[name]:
            raise ValueError(f"{name}() ma {_FUNCS[name]} number joiye")
        if name in ("st", "stdir"):
            if not (1 <= a[0] <= 200 and a[0] == int(a[0]) and 0.1 <= a[1] <= 20):
                raise ValueError(f"{name}(period 1-200, mult 0.1-20)")
            a = [int(a[0]), float(a[1])]
        else:
            if any(x < 1 or x > 200 or x != int(x) for x in a):
                raise ValueError(f"{name}() na number 1 thi 200 ni vachhe (purna sankhya) joiye")
            a = [int(x) for x in a]
        txt = f"{name}({','.join(f'{x:g}' for x in a)})" + (f"[{shift}]" if shift else "")
        return ("fn", name, a, shift, txt)
    raise ValueError(f"'{name}' indicator ni ovkhato nathi")


def _op_text(o) -> str:
    if o[0] == "num":
        return f"{o[1]:g}"
    return o[3] if o[0] == "price" else o[4]


def _parse_cond(text: str):
    t = text.strip().lower().replace(" ", "")
    m = _OP_RE.match(t)
    if not m:
        raise ValueError(f"'{text.strip()}' ma operator (>  <  >=  <=  =) nathi")
    lhs, op, rhs = _parse_operand(m.group(1)), m.group(2), _parse_operand(m.group(3))
    op = "=" if op == "==" else op
    if lhs[0] == "num" and rhs[0] == "num":
        raise ValueError(f"'{text.strip()}' ma banne baju number che")
    return (lhs, op, rhs)


def _cond_text(c) -> str:
    return f"{_op_text(c[0])} {c[1]} {_op_text(c[2])}"


def _mirror_cond(c):
    lhs, op, rhs = c
    if rhs[0] == "num" and lhs[0] == "fn":
        if lhs[1] in ("rsi", "rsiwma"):
            rhs = ("num", 100.0 - rhs[1])
        elif lhs[1] == "stdir" and op == "=":
            rhs = ("num", -rhs[1])
    if lhs[0] == "price" and lhs[1] == "close" and rhs[0] == "price" and rhs[1] in ("high", "low"):
        rhs = ("price", "low" if rhs[1] == "high" else "high", rhs[2],
               ("low" if rhs[1] == "high" else "high") + (f"[{rhs[2]}]" if rhs[2] else ""))   # breakout: high[1] <-> low[1]
    return (lhs, _MIRROR_OP[op], rhs)


def _rsi_series(c: pd.Series, n: int) -> pd.Series:
    delta = c.diff()
    up = delta.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-delta.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    r = 100 - 100 / (1 + up / dn)
    return r.mask(dn == 0, 100.0).mask((dn == 0) & (up == 0), 50.0)


def _wma_series(s_: pd.Series, n: int) -> pd.Series:
    w = np.arange(1, n + 1)
    return s_.rolling(n).apply(lambda x: np.dot(x, w) / w.sum(), raw=True)


def _op_series(o, df: pd.DataFrame, cache: dict) -> pd.Series:
    if o[0] == "num":
        return pd.Series(o[1], index=df.index)
    key = _op_text(o)
    if key in cache:
        return cache[key]
    c = df["close"]
    if o[0] == "price":
        ser = df[o[1]].shift(o[2]) if o[2] else df[o[1]]
    else:
        name, a, shift = o[1], o[2], o[3]
        if name == "ema":
            ser = c.ewm(span=a[0], adjust=False).mean()
        elif name == "sma":
            ser = c.rolling(a[0]).mean()
        elif name == "wma":
            ser = _wma_series(c, a[0])
        elif name == "rsi":
            ser = _rsi_series(c, a[0])
        elif name == "rsiwma":
            ser = _wma_series(_rsi_series(c, a[0]), a[1])
        elif name == "macd":
            ser = c.ewm(span=a[0], adjust=False).mean() - c.ewm(span=a[1], adjust=False).mean()
        elif name == "macdsig":
            m_ = c.ewm(span=a[0], adjust=False).mean() - c.ewm(span=a[1], adjust=False).mean()
            ser = m_.ewm(span=a[2], adjust=False).mean()
        else:                                           # st / stdir
            d2 = supertrend(df, int(a[0]), float(a[1]))
            ser = d2["st"] if name == "st" else d2["dir"].astype(float)
        if shift:
            ser = ser.shift(shift)
    cache[key] = ser
    return ser


def _cond_series(c, df, cache) -> pd.Series:
    a, b = _op_series(c[0], df, cache), _op_series(c[2], df, cache)
    op = c[1]
    if op == ">":
        r = a > b
    elif op == "<":
        r = a < b
    elif op == ">=":
        r = a >= b
    elif op == "<=":
        r = a <= b
    else:
        r = (a - b).abs() < 1e-9
    return r.fillna(False)


def _custom_signal(spec: dict, df: pd.DataFrame):
    """spec = STRATEGIES[key]. Return (direction, candle_time, reference_line)."""
    cache = {}
    bull = pd.Series(True, index=df.index)
    for c in spec["bull"]:
        bull = bull & _cond_series(c, df, cache)
    bear = pd.Series(True, index=df.index)
    for c in spec["bear"]:
        bear = bear & _cond_series(c, df, cache)
    dirs, cur = [], 0
    for b_, r_ in zip(bull.values, bear.values):        # banne nahi (mixed) to pehlano trend chalu
        if b_ and not r_:
            cur = 1
        elif r_ and not b_:
            cur = -1
        dirs.append(cur)
    i = -2                                              # last COMPLETED candle

    def val(o):
        return float(_op_series(o, df, cache).iloc[i])

    def part(c):
        ok = bool(_cond_series(c, df, cache).iloc[i])
        try:
            return (f"{_op_text(c[0])} {val(c[0]):,.1f} {c[1]} {_op_text(c[2])} {val(c[2]):,.1f} "
                    f"{'✔' if ok else '✘'}")
        except Exception:
            return f"{_cond_text(c)} {'✔' if ok else '✘'}"
    _sig_cache["extra"] = ("BULLISH: " + "; ".join(part(c) for c in spec["bull"]) +
                           " | BEARISH: " + "; ".join(part(c) for c in spec["bear"]))
    ref = float(df["close"].iloc[i])
    for c in spec["bull"]:
        for o in (c[0], c[2]):
            if o[0] == "fn" and o[1] != "stdir":
                v = val(o)
                if v == v:
                    ref = v
                    break
        else:
            continue
        break
    return dirs[i], df["time"].iloc[i], ref


def _line_name(bull) -> str:
    for c in bull:
        for o in (c[0], c[2]):
            if o[0] == "fn" and o[1] != "stdir":
                return o[4].upper()
    return "Close"


def parse_strategy_text(txt: str) -> dict:
    """Telegram message -> strategy spec. ValueError aape to user ne dekhadvano message."""
    name, buy, sell = "", [], []
    for raw in re.split(r"[\n;]+", txt):
        ln = raw.strip()
        if not ln:
            continue
        m = re.match(r"^(name|naam|bullish|bearish|buy|bull|long|sell|bear|short)\s*[:=]\s*(.+)$", ln, re.I)
        if not m:
            raise ValueError(f"Line samjayi nahi: '{ln}'\nShuruat name: / bullish: / bearish: thi karo.")
        k, body = m.group(1).lower(), m.group(2).strip()
        if k in ("name", "naam"):
            name = re.sub(r"\s+", " ", body)[:28]
            continue
        conds = [x for x in re.split(r"\s+(?:and|&&|&)\s+", body, flags=re.I) if x.strip()]
        (buy if k in ("bullish", "buy", "bull", "long") else sell).extend(_parse_cond(x) for x in conds)
    if not buy:
        raise ValueError("Ochhama ochhi ek 'bullish:' condition joiye.")
    if len(buy) > 6 or len(sell) > 6:
        raise ValueError("Ek side ma max 6 condition.")
    auto = not sell
    if auto:
        sell = [_mirror_cond(c) for c in buy]
    return {"name": name, "bull": buy, "bear": sell, "auto_mirror": auto}


def _test_spec(spec: dict):
    """Dummy data par chalavi ne check - error aave to save pehla j pakdai jay."""
    rng = np.random.default_rng(1)
    n = 400
    close = 24000 + np.cumsum(rng.normal(0, 8, n))
    df = pd.DataFrame({"time": pd.date_range("2026-01-01 09:15", periods=n, freq="3min"),
                       "open": close - 1, "high": close + 4, "low": close - 4, "close": close, "volume": 1})
    _custom_signal(spec, df)


def _register_custom(key: str, label: str, bull: list, bear: list):
    STRATEGIES[key] = {"label": label, "line": _line_name(bull), "custom": True, "bull": bull, "bear": bear}


def save_custom_strategies():
    try:
        data = {"custom": {k: {"label": v["label"], "bull": [_cond_text(c) for c in v["bull"]],
                               "bear": [_cond_text(c) for c in v["bear"]]}
                           for k, v in STRATEGIES.items() if v.get("custom")},
                "removed": [k for k in _BUILTIN_STRATEGIES if k not in STRATEGIES]}
        tmp = CUSTOM_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1, ensure_ascii=False)
        os.replace(tmp, CUSTOM_FILE)
    except Exception as e:
        log.warning("custom strategies save failed: %s", e)


def ensure_valid_strategy():
    if state["strategy"] not in STRATEGIES and STRATEGIES:
        state["strategy"] = next(iter(STRATEGIES))


def load_custom_strategies():
    """Bot start par (load_state pehla) saved custom strategy ane remove kareli built-in lagu kare."""
    if not os.path.exists(CUSTOM_FILE):
        return
    try:
        with open(CUSTOM_FILE, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        log.warning("custom strategies vachay nahi: %s", e)
        return
    for k, v in (data.get("custom") or {}).items():
        try:
            _register_custom(k, v["label"], [_parse_cond(x) for x in v["bull"]],
                             [_parse_cond(x) for x in v["bear"]])
        except Exception as e:
            log.warning("custom strategy %s skip: %s", k, e)
    for k in data.get("removed") or []:
        if k in STRATEGIES and not STRATEGIES[k].get("custom") and len(STRATEGIES) > 1:
            STRATEGIES.pop(k, None)
    ensure_valid_strategy()


def add_custom_strategy(txt: str):
    """Return (key, label, spec). ValueError aape to message user ne batavo."""
    if sum(1 for v in STRATEGIES.values() if v.get("custom")) >= MAX_CUSTOM_STRATEGIES:
        raise ValueError(f"Max {MAX_CUSTOM_STRATEGIES} custom strategy. Pehla koi juni remove karo.")
    spec = parse_strategy_text(txt)
    n = 1
    while f"c{n}" in STRATEGIES:
        n += 1
    key = f"c{n}"
    label = spec["name"] or f"Custom {n}"
    if any(v["label"].lower() == label.lower() for v in STRATEGIES.values()):
        raise ValueError(f"'{label}' naam pehla thi che. Biju naam aapo.")
    _test_spec(spec)
    _register_custom(key, label, spec["bull"], spec["bear"])
    save_custom_strategies()
    return key, label, spec


def strategy_in_use(key: str) -> bool:
    if state["strategy"] == key:
        return True
    return any((PROFILES.get(p) or {}).get("strategy") == key for p in ("paper", "live"))


def remove_strategy(key: str):
    """Error text athva None (sachu remove thayu)."""
    if key not in STRATEGIES:
        return "Aa strategy pehla thi nathi"
    if len(STRATEGIES) <= 1:
        return "Chhelli strategy delete nahi thay"
    if strategy_in_use(key):
        return "Aa strategy Paper/Live ma select che. Pehla biji strategy select karo."
    STRATEGIES.pop(key)
    save_custom_strategies()
    return None


def restore_builtin_strategies():
    for k, v in _BUILTIN_STRATEGIES.items():
        STRATEGIES.setdefault(k, dict(v))
    save_custom_strategies()


def strategy_detail(key: str) -> str:
    v = STRATEGIES[key]
    return (f"• {v['label']} (custom)\n   BULLISH: " + " AND ".join(_cond_text(c) for c in v["bull"]) +
            "\n   BEARISH: " + " AND ".join(_cond_text(c) for c in v["bear"]))


def _strategy_signal(strategy: str, df: pd.DataFrame):
    """Return (direction, candle_time, reference_line). direction: 1 = bullish, -1 = bearish.
    Last COMPLETED candle (iloc[-2]) par j signal."""
    _sig_cache["extra"] = ""
    if STRATEGIES.get(strategy, {}).get("custom"):
        return _custom_signal(STRATEGIES[strategy], df)
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


def compute_signal(strategy: str, df: pd.DataFrame):
    """Strategy signal + (ON hoy to) candle breakout condition:
    bullish: chhelli band candle no close > tena pehlani candle no High
    bearish: chhelli band candle no close < tena pehlani candle no Low
    Condition na male to signal neutral (0) = navi entry/flip nahi; strategy trend ave to pachhi ni candle ma male to j entry."""
    d, ct, ref = _strategy_signal(strategy, df)
    if not CANDLE_BREAKOUT or d == 0 or len(df) < 4:
        return d, ct, ref
    c, prev_h, prev_l = float(df["close"].iloc[-2]), float(df["high"].iloc[-3]), float(df["low"].iloc[-3])
    ok = (c > prev_h) if d == 1 else (c < prev_l)
    note = (f"Breakout: close {c:,.1f} {'>' if d == 1 else '<'} prev {'High' if d == 1 else 'Low'} "
            f"{prev_h if d == 1 else prev_l:,.1f} {'OK' if ok else 'pending'}")
    _sig_cache["extra"] = (_sig_cache["extra"] + " | " if _sig_cache.get("extra") else "") + note
    return (d if ok else 0), ct, ref


def current_signal():
    """Last COMPLETED candle no supertrend direction. Navi candle bane tyare j fetch."""
    now = datetime.now()
    open_t = now.replace(hour=9, minute=15, second=0, microsecond=0)
    secs = (now - open_t).total_seconds()
    bucket = (state["strategy"], now.date(), int(secs // (60 * CANDLE_MINUTES)))
    # candle band thaya pachi 2 sec ni rah joi ne fetch (Angel ma candle na aave to niche retry thase)
    ready = secs % (60 * CANDLE_MINUTES) >= 2
    if _sig_cache["val"] is not None and (_sig_cache["bucket"] == bucket or not ready):
        return _sig_cache["val"]
    df_ = get_candles()
    _candle_cache["df"] = df_            # image chart mate
    val = compute_signal(state["strategy"], df_)
    if bucket[2] >= 1:
        # haju navi bandh thayeli candle Angel ma nathi aavi? to cache na karo, next tick ma fari fetch (max 8 vaar)
        expected = pd.Timestamp(open_t + timedelta(minutes=CANDLE_MINUTES * (bucket[2] - 1)))
        if pd.Timestamp(val[1]) < expected:
            if _sig_cache.get("try_bucket") != bucket:
                _sig_cache["try_bucket"], _sig_cache["tries"] = bucket, 0
            _sig_cache["tries"] += 1
            if _sig_cache["tries"] <= 8:
                return val
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


def _locked(fn):
    def w(*a, **k):
        with _cfg_lock:
            return fn(*a, **k)
    w.__name__ = fn.__name__
    w.__doc__ = fn.__doc__
    return w


def pick_legs_batch(opt_type, atm, expiry):
    """ATM + 49 hedge-candidate strikes na prices ek j API call ma. (sell_opt, sell_px, hedge_opt, hedge_px)"""
    sign = -1 if opt_type == "PE" else 1
    sub = scrips[(scrips["expiry_d"] == expiry) & (scrips["symbol"].str.endswith(opt_type))]
    by_strike = {float(r.strike_f): r for r in sub.itertuples()}
    cands = []
    for step in range(0, 50):
        r = by_strike.get(float(atm + sign * step * STRIKE_STEP))
        if r is not None:
            cands.append({"symbol": r.symbol, "token": r.token, "strike": atm + sign * step * STRIKE_STEP,
                          "type": opt_type, "step": step})
    if not cands or cands[0]["step"] != 0:
        raise RuntimeError("ATM strike nathi malyo")
    r = throttled(smart.getMarketData, "LTP", {"NFO": [c["token"] for c in cands]}, retries=1)
    rows = ((r or {}).get("data") or {}).get("fetched") if isinstance(r, dict) else None
    if not rows:
        raise RuntimeError(f"batch LTP nathi malyo: {r}")
    px = {str(x.get("symbolToken")): float(x["ltp"]) for x in rows if x.get("ltp") is not None}
    sell = cands[0]
    if sell["token"] not in px or px[sell["token"]] <= 0:
        raise RuntimeError("ATM price nathi malyo")
    sell_px = px[sell["token"]]
    target = sell_px * HEDGE_PCT
    pool = [c for c in cands[1:] if px.get(c["token"], 0) > 0]
    if not pool:
        raise RuntimeError("Hedge strike nathi malyo")
    hedge = min(pool, key=lambda c: abs(px[c["token"]] - target))
    strip = lambda o: {k: o[k] for k in ("symbol", "token", "strike", "type")}
    return strip(sell), sell_px, strip(hedge), px[hedge["token"]]


pick_hedge = _locked(pick_hedge)
pick_legs_batch = _locked(pick_legs_batch)
current_signal = _locked(current_signal)


async def open_position(direction: int, app: Application):
    if state["day_halt"]:
        return                              # divas limit hit -> navi entry nahi
    mt = max_trades_reason()
    if mt:
        return await set_day_halt(app, mt)  # divas na max trades puri thaya
    opt_type = "PE" if direction == 1 else "CE"
    t0 = time.time()
    spot = await spot_cached()                       # WS / poller thi fast price
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    sell_strike = sell_strike_for(opt_type, atm)      # ATM / OTM-n / ITM-n (setting mujab)
    sel_lbl = sell_label(SELL_OFFSET)
    expiry = nearest_expiry()
    qty = LOT_SIZE * LOTS

    try:
        # ek j API call ma ATM + hedge candidates (junu 6-8 calls no rasto hatu)
        sell_opt, sell_px, hedge_opt, hedge_px = await asyncio.to_thread(pick_legs_batch, opt_type, sell_strike, expiry)
    except Exception as e:
        log.warning("batch leg pick fail (%s) -> junu dhimu rasto", e)
        sell_opt = find_option(sell_strike, opt_type, expiry)
        sell_px = await asyncio.to_thread(ltp, "NFO", sell_opt["symbol"], sell_opt["token"])
        hedge_opt, hedge_px = await asyncio.to_thread(pick_hedge, opt_type, sell_strike, sell_px, expiry)

    credit = sell_px - hedge_px  # per unit
    if credit <= 0:
        await notify(app, "Net credit <= 0, entry skip.")
        return

    orders_info = None
    ltp_ref = (sell_px, hedge_px)
    if state["mode"] == "live":
        # Angel One ma sacha orders: fill confirm karine j position banave, fill price vapre
        r = await asyncio.to_thread(live_entry_orders, hedge_opt, sell_opt, qty)
        if not r["ok"]:
            state["last_event"] = f"{datetime.now():%H:%M} ENTRY FAIL"
            save_state()
            return await notify_trade(app, r["msg"], state["mode"], loud=True)
        sell_px = r["sell_px"] or sell_px
        hedge_px = r["hedge_px"] or hedge_px
        credit = sell_px - hedge_px
        orders_info = r["orders"]

    state["live_msg_id"] = None
    state["live_msg_t"] = 0.0
    state["position"] = {
        "dir": direction, "type": opt_type, "qty": qty, "expiry": str(expiry.date()),
        "sell": sell_opt, "sell_entry": sell_px,
        "hedge": hedge_opt, "hedge_entry": hedge_px,
        "credit": credit,
        "orders": orders_info, "ltp_ref": list(ltp_ref), "sell_sel": sel_lbl,
        "sl": -calc_limit(SL_MODE, SL_VALUE, credit, qty),
        "tp": calc_limit(TP_MODE, TP_VALUE, credit, qty),
        "sl_txt": limit_text(SL_MODE, SL_VALUE), "tp_txt": limit_text(TP_MODE, TP_VALUE),
        "time": datetime.now().strftime("%H:%M:%S"), "mode": state["mode"], "spot": spot,
        "lots": LOTS, "strategy": strategy_label(), "tf": CANDLE_MINUTES,
        "entry_dt": datetime.now().isoformat(timespec="seconds"),
    }
    ws_subscribe(2, [sell_opt["token"], hedge_opt["token"]])
    entry_secs = time.time() - t0
    state["last_event"] = f"{datetime.now():%H:%M} ENTRY {opt_type} spread"
    save_state()
    n_tr = trades_count_today()
    await notify_trade(app,
        f"✅ {state['mode'].upper()} ENTRY • {opt_type} • SELL {_strike_txt(sell_opt['symbol'])} @{sell_px:.2f} / "
        f"BUY {_strike_txt(hedge_opt['symbol'])} @{hedge_px:.2f}\n"
        f"{strategy_label()} {'UP 🟢' if direction == 1 else 'DOWN 🔴'} • Trade {n_tr}" + (f"/{max_trades_limit()}" if max_trades_limit() else "") + "\n"
        f"SELL {sell_opt['symbol']} @ {sell_px:.2f}  ({sel_lbl}, Nifty {spot:,.0f})\n"
        f"BUY  {hedge_opt['symbol']} @ {hedge_px:.2f}  (hedge, sell strike thi bahar)\n"
        f"Expiry: {expiry:%d %b %Y} ({EXPIRY_MODES[EXPIRY_MODE].split(' (')[0]})\n"
        f"Lots: {LOTS} | Qty: {qty} | Net credit/unit: {credit:.2f}\n"
        f"🛡 SL: -₹{abs(state['position']['sl']):,.0f} ({state['position']['sl_txt']})\n"
        f"🎯 Target: +₹{state['position']['tp']:,.0f} ({state['position']['tp_txt']})\n"
        f"⏱ Entry prep + orders: {entry_secs:.1f}s"
        + (("\n" + fills_text(orders_info, ltp_ref)) if orders_info else ""), state["mode"])


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
    live_txt = ""
    exit_orders, est, charges = None, False, None
    if mode == "live":
        if time.time() < state.get("exit_block_t", 0):
            return                                   # pehla exit fail thayo, thodi vaar ruko
        r = await asyncio.to_thread(live_exit_orders, p)
        if not r["ok"]:
            state["exit_block_t"] = time.time() + 30
            return await notify_trade(app, r["msg"], mode, loud=True)
        # Angel One na sacha fill price thi PnL
        s, h = r["s"], r["h"]
        exit_orders, est = r["orders"], bool(r["warn"])
        pnl = ((p["sell_entry"] - s) + (h - p["hedge_entry"])) * p["qty"]
        live_txt = "\n" + fills_text(r["orders"], exit_=True) + (("\n" + r["warn"]) if r["warn"] else "")
        # ---- charges Angel One (estimateCharges) thi: 4 executed orders na fill price par ----
        try:
            eo_, xo_ = p.get("orders") or {}, r["orders"]
            q_ = p["qty"]
            olist = []
            for side_, leg_, info_, fb_ in (("BUY", p["hedge"], eo_.get("hedge"), p["hedge_entry"]),
                                            ("SELL", p["sell"], eo_.get("sell"), p["sell_entry"]),
                                            ("BUY", p["sell"], xo_.get("sell_exit"), s),
                                            ("SELL", p["hedge"], xo_.get("hedge_exit"), h)):
                if info_ is not None or side_:
                    px_ = (info_ or {}).get("avg") or fb_
                    if px_:
                        olist.append({"side": side_, "symbol": leg_["symbol"], "token": leg_["token"], "qty": q_, "price": px_})
            if eo_:
                ch = await asyncio.wait_for(asyncio.to_thread(fetch_charges, olist), timeout=8)
                if ch[0] is not None:
                    charges = ch[0]
                    live_txt += f"\n💸 Charges (Angel est.): ₹{charges:,.2f} → Net {money(pnl - charges)}"
                else:
                    live_txt += f"\n💸 Charges: Angel thi nathi malya ({ch[1]})"
        except Exception as e:
            log.warning("charges fetch fail: %s", e)
    state["realized_pnl"] += pnl
    try:
        exit_spot = await spot_cached(5)
    except Exception:
        exit_spot = None
    trade = build_trade_record(p, s, h, pnl, reason, mode, exit_spot, exit_orders, est, charges)
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
    await notify_trade(app, f"🔴 {mode.upper()} EXIT • {reason} • {money(pnl)}\n{trade_block(trade)}\n"
                            f"Day PnL ({mode}): {money(mode_realized())}"
                            + (f" • Trade {len(mode_trades())}/{max_trades_limit()}" if max_trades_limit() else "")
                            + f"{extra}{live_txt}", mode)
    lim = day_limit_reason(mode_realized()) or max_trades_reason(after_close=True)
    if lim:
        await set_day_halt(app, lim)


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
def day_limit_reason(total_pnl: float):
    """Divas nu limit hit thayu hoy to reason aape, nahi to None."""
    if not DAY_LIMIT_ON or state["limit_override"]:
        return None
    if total_pnl <= -DAY_LOSS_LIMIT:
        return f"DAY LOSS LIMIT (₹{total_pnl:,.0f} / -₹{DAY_LOSS_LIMIT:,.0f})"
    if total_pnl >= DAY_PROFIT_LIMIT:
        return f"DAY PROFIT TARGET (₹{total_pnl:,.0f} / ₹{DAY_PROFIT_LIMIT:,.0f})"
    return None


def trades_count_today() -> int:
    """Aaj na (atyaar na mode na) entry thayela trades: band thayela + open."""
    return len(mode_trades()) + (1 if state.get("position") else 0)


def max_trades_limit() -> int:
    """ON hoy to limit (count), OFF hoy to 0."""
    return MAX_TRADES_PER_DAY if MAX_TRADES_ON else 0


def max_trades_reason(after_close: bool = False):
    lim = max_trades_limit()
    if not lim or state["limit_override"]:
        return None
    n = len(mode_trades()) if after_close else trades_count_today()
    if n >= lim:
        return f"MAX TRADES/DAY ({n}/{lim})"
    return None


def alert_chat(mode: str) -> int:
    return (PAPER_ALERT_CHAT_ID if mode == "paper" else LIVE_ALERT_CHAT_ID) or ALLOWED_CHAT_ID


async def notify_trade(app: Application, text: str, mode: str, loud: bool = False):
    """Entry/exit alert: mode no alag chat (hoy to), sound ON/OFF setting mujab. loud=True -> hamesha sound (error)."""
    silent = not (ALERT_SOUND or loud)
    chat = alert_chat(mode)
    if not chat:
        return
    try:
        await app.bot.send_message(chat, text, disable_notification=silent)
    except Exception as e:
        log.warning("alert chat %s ma na gayu (%s) -> main chat", chat, e)
        if ALLOWED_CHAT_ID and chat != ALLOWED_CHAT_ID:
            await app.bot.send_message(ALLOWED_CHAT_ID, text, disable_notification=silent)


async def set_day_halt(app: Application, reason: str):
    if state["day_halt"]:
        return
    state["day_halt"] = reason
    save_state()
    await notify_trade(app, f"🛑 {reason}\nAaj nu navu trading band thayu.\n"
                            f"Chalu rakhvu hoy to: Settings ma ▶️ Resume today (athva /daylimit resume).",
                       state["mode"], loud=True)


def roll_day():
    """Navo divas hoy to trades ane realized PnL reset."""
    today = datetime.now().date()
    if state["day"] != today:
        state["day"] = today
        state["trades"] = []
        state["realized_pnl"] = 0.0
        state["blocked_dir"] = None          # navo divas -> trend block reset
        state["day_halt"] = None             # navo divas -> limit halt reset
        state["limit_override"] = False


async def tick(context: ContextTypes.DEFAULT_TYPE):
    roll_day()
    if time.time() - _last_save[0] > 60:      # koi change save thatu rahi gayu hoy to pan 1 min ma save
        save_state()
    # Angel JWT roughly 1 divas ma expire thay -> har 6 kalak ma fresh login (market time pehla pan)
    if time.time() - _last_login[0] > 6 * 3600:
        try:
            await asyncio.to_thread(relogin, True)
        except Exception as e:
            log.warning("periodic relogin failed: %s", e)
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
            lim = day_limit_reason(mode_realized() + pnl)
            if lim:
                await close_position(app, lim)
                return await set_day_halt(app, lim)
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


# ----------------------------- BOTTOM BUTTONS (type karvu nahi pade) -----------------
BOTTOM_ROWS = [["📺 Dashboard", "🏠 Menu"], ["▶️ Run", "⏸ Stop"], ["📒 Orders", "⚙️ Settings"]]


def bottom_kb():
    try:
        return ReplyKeyboardMarkup(BOTTOM_ROWS, resize_keyboard=True, is_persistent=True,
                                   input_field_placeholder="Neeche na buttons vapro 👇")
    except TypeError:                                   # juni telegram library
        return ReplyKeyboardMarkup(BOTTOM_ROWS, resize_keyboard=True)


async def cmd_settings_btn(update: Update, ctx):
    text_, kb_ = settings_hub()
    await update.message.reply_text(text_, reply_markup=kb_)


async def on_bottom_text(update: Update, ctx):
    """Neeche na button no text -> te action. Bija koi text (chat) ne ignore kare."""
    if not update.message or not authorized(update):
        return
    actions = {"📺 Dashboard": cmd_dashboard, "🏠 Menu": cmd_menu, "▶️ Run": cmd_run, "⏸ Stop": cmd_stop,
               "📒 Orders": cmd_orders, "⚙️ Settings": cmd_settings_btn}
    txt = (update.message.text or "").strip()
    fn = actions.get(txt)
    if fn:
        ctx.user_data.pop("await_strat", None)
        await fn(update, ctx)
        return
    if ctx.user_data.get("await_strat"):                  # navi strategy no text vaanchvo
        try:
            key, label, spec = add_custom_strategy(txt)
        except ValueError as e:
            return await update.message.reply_text(f"❌ {e}\n\nSudhari ne fari moklo. (Cancel: /menu → Strategy → ➕ → Cancel)")
        except Exception as e:
            return await update.message.reply_text(f"❌ Strategy banti nathi: {e}")
        ctx.user_data.pop("await_strat", None)
        note = " (ulti condition aapo-aap)" if spec["auto_mirror"] else ""
        await update.message.reply_text(
            f"✅ Navi strategy save thai: {label}\n"
            f"BULLISH (PE spread): " + " AND ".join(_cond_text(c) for c in spec["bull"]) + "\n"
            f"BEARISH (CE spread){note}: " + " AND ".join(_cond_text(c) for c in spec["bear"]),
            reply_markup=Markup([[Btn("✅ Aa strategy select karo", callback_data=f"strat_{key}")],
                                 [Btn("🧠 Strategy menu", callback_data="strategy")]]))


async def cmd_newstrategy(update: Update, ctx):
    if not authorized(update):
        return
    ctx.user_data["await_strat"] = True
    await update.message.reply_text(CUSTOM_HELP)


async def cmd_keyboard(update: Update, ctx):
    if not authorized(update):
        return
    if ctx.args and ctx.args[0].lower() == "off":
        return await update.message.reply_text("⌨️ Neeche na buttons band karya.", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("⌨️ Neeche na buttons chalu. Type karvani jaroor nathi 👇", reply_markup=bottom_kb())


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    await update.message.reply_text(f"Your chat id: {update.effective_chat.id}")
    await update.message.reply_text("⌨️ Neeche na buttons vapro, type karvani jaroor nathi 👇", reply_markup=bottom_kb())
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
    save_state()
    await update.message.reply_text(f"▶️ Bot running in {state['mode'].upper()} mode")


async def cmd_stop(update: Update, ctx):
    if not authorized(update):
        return
    state["running"] = False
    save_state()
    await update.message.reply_text("⏸ Bot stopped (open position hoy to /exit vapro)")


async def cmd_mode(update: Update, ctx):
    if not authorized(update):
        return
    if state["position"]:
        return await update.message.reply_text("Position open che, pehla exit karo.")
    if not ctx.args or ctx.args[0] not in ("paper", "live"):
        return await update.message.reply_text("Use: /mode paper  or  /mode live")
    switch_mode(ctx.args[0])
    await update.message.reply_text(f"Mode: {state['mode'].upper()}\n{state['mode'].upper()} ni alag setting lagu thai. "
                                    f"⏸ Bot pause thayo, ▶️ Run dabavo.\n\n" + settings_text())


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
    "Commands: /menu /dashboard /signal /summary /status /pnl /live /run /stop /exit /mode")


def main_menu_text() -> str:
    run = "🟢 RUNNING" if state["running"] else "🔴 STOPPED"
    mode = "⚡ LIVE" if state["mode"] == "live" else "📝 PAPER"
    pos = "📌 Open" if state["position"] else "➖ None"
    return (f"━━━ 🤖 NIFTY SPREAD BOT ━━━\n"
            f"Bot: {run}   Mode: {mode}\n"
            f"Strategy: 🧠 {strategy_label()} • ⏱{CANDLE_MINUTES}m\n"
            f"Position: {pos}\n"
            f"Day realized: ₹{mode_realized():,.0f}\n"
            f"{('🛑 Daily limit hit: aaj trading band' + chr(10)) if state['day_halt'] else ''}"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Option select karo 👇")


def main_menu_kb() -> Markup:
    return Markup([
        [Btn("📺 Dashboard", callback_data="dashboard")],
        [Btn("🟢 Run Bot", callback_data="run"), Btn("🔴 Stop Bot", callback_data="stop")],
        [Btn("📡 Signal", callback_data="signal"), Btn("🧠 Strategy", callback_data="strategy")],
        [Btn("📅 Today's Summary", callback_data="summary"), Btn("💹 Live PnL", callback_data="live")],
        [Btn("📊 Status", callback_data="status"), Btn("💰 PnL", callback_data="pnl")],
        [Btn("📜 Today Trades", callback_data="trades"), Btn("🗂 History", callback_data="history")],
        [Btn("🚪 Exit Position", callback_data="exit")],
        [Btn("📝 Paper Mode", callback_data="mode_paper"), Btn("⚡ Live Mode", callback_data="mode_live")],
        [Btn("⚙️ Settings", callback_data="settings"), Btn("🧪 Angel Test", callback_data="angeltest")],
        [Btn("🔄 Refresh", callback_data="menu"), Btn("📒 Angel Orders", callback_data="angelorders")],
        [Btn("❓ Help", callback_data="help"), Btn("❌ Close", callback_data="close")],
    ])


_BUILTIN_DESC = {
    "supertrend": "• Supertrend: Green → PE spread, Red → CE spread",
    "ema": f"• EMA {EMA_FAST}/{EMA_SLOW}: EMA{EMA_FAST} > EMA{EMA_SLOW} → PE spread, niche → CE spread",
    "rsi_wma": (f"• RSI{RSI_PERIOD} x WMA{RSI_WMA_LEN} + SMA{SMA_LEN}: RSI > WMA ane close > SMA → PE spread; "
                f"RSI < WMA ane close < SMA → CE spread (candle close par)"),
}


def strategy_text() -> str:
    lines = [_BUILTIN_DESC[k] for k, v in STRATEGIES.items() if not v.get("custom") and k in _BUILTIN_DESC]
    lines += [strategy_detail(k) for k, v in STRATEGIES.items() if v.get("custom")]
    return ("🧠 Strategy select karo\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"Active: {strategy_label()}  •  ⏱ {CANDLE_MINUTES} min\n\n"
            + "\n".join(lines) + "\n\n"
            "➕ Navi strategy (potani condition sathe) umervā ane 🗑 juni remove karva niche buttons che.\n"
            "⏱ Timeframe niche thi select karo (badha strategy ma lagu).\n"
            "⚠️ Strategy/timeframe badalta bot pause thase (Run pharithi dabavo).\n"
            "Position open hoy tyare badli nahi shakay.")


def strategy_kb() -> Markup:
    rows = [[Btn(("✅ " if k == state["strategy"] else "") + v["label"], callback_data=f"strat_{k}")]
            for k, v in STRATEGIES.items()]
    rows.append([Btn("➕ Navi Strategy", callback_data="sx_new"), Btn("🗑 Strategy Remove", callback_data="sx_del")])
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


_view = {"inactive": False}      # settings page je mode ni jove che te atyaar no active mode nathi


def day_limit_text() -> str:
    if not DAY_LIMIT_ON:
        return "🛑 Daily limit: OFF"
    extra = ""
    if _view["inactive"]:
        extra = ""
    elif state["day_halt"]:
        extra = " • HIT, aaj band"
    elif state["limit_override"]:
        extra = " • aaj mate ignore"
    return f"🛑 Daily limit: ON (loss -₹{DAY_LOSS_LIMIT:,.0f} / profit +₹{DAY_PROFIT_LIMIT:,.0f}){extra}"


def settings_text(prof=None) -> str:
    prof = prof or state["mode"]
    tag = "✅ chalu mode" if prof == state["mode"] else "chalu nathi"
    return (f"{'⚡ LIVE' if prof == 'live' else '📝 PAPER'} settings ({tag}) • navi trade thi lagu\n"
            f"🧠 Strategy: {strategy_label()} • ⏱ {CANDLE_MINUTES}m\n"
            f"Lots: {LOTS} (qty {LOT_SIZE * LOTS})\n"
            f"Sell strike: {sell_label(SELL_OFFSET)}\n"
            f"Hedge premium: {HEDGE_PCT * 100:.0f}% of sold option\n"
            f"🛡 Stop loss: {limit_text(SL_MODE, SL_VALUE)}\n"
            f"🎯 Target: {limit_text(TP_MODE, TP_VALUE)}\n"
            f"📅 Expiry: {expiry_label()}\n"
            f"Signal flip exit: {'ON' if EXIT_ON_FLIP else 'OFF'}\n"
            f"{day_limit_text()}\n"
            f"🔢 Max trades/day: {('ON (' + str(MAX_TRADES_PER_DAY) + ')') if MAX_TRADES_ON else 'OFF'}\n"
            f"🕯 Candle breakout: {'ON (close > pichhli High / < pichhli Low)' if CANDLE_BREAKOUT else 'OFF'}\n"
            f"🔔 Alert sound: {'ON' if ALERT_SOUND else 'OFF (silent)'} • chat: "
            f"{'alag' if alert_chat(prof) != ALLOWED_CHAT_ID else 'main'}\n"
            f"🕒 Time: {ENTRY_START:%H:%M} - {LAST_ENTRY:%H:%M} (exit {FORCE_EXIT:%H:%M}) • "
            f"1 trade/trend: {'ON' if ONE_TRADE_PER_TREND else 'OFF'}")


def settings_kb(prof=None) -> Markup:
    prof = prof or state["mode"]
    dl_rows = [[Btn(f"🛑 Daily limit: {'ON' if DAY_LIMIT_ON else 'OFF'} 🔄", callback_data="set_dlim_0")]]
    if DAY_LIMIT_ON:
        dl_rows += [
            [Btn("➖", callback_data="set_dll_-1"), Btn(f"Loss -₹{DAY_LOSS_LIMIT:,.0f}", callback_data="noop"),
             Btn("➕", callback_data="set_dll_1")],
            [Btn("➖", callback_data="set_dlp_-1"), Btn(f"Profit +₹{DAY_PROFIT_LIMIT:,.0f}", callback_data="noop"),
             Btn("➕", callback_data="set_dlp_1")],
        ]
    if state["day_halt"] and not _view["inactive"]:
        dl_rows.append([Btn("▶️ Resume today (limit ignore)", callback_data="set_dres_0")])
    return Markup([
        [Btn("➖ Lots", callback_data="set_lots_-1"), Btn(f"{LOTS}", callback_data="noop"),
         Btn("➕ Lots", callback_data="set_lots_1")],
        [Btn("◀ ITM", callback_data="set_soff_-1"), Btn(f"Sell: {sell_label(SELL_OFFSET)}", callback_data="noop"),
         Btn("OTM ▶", callback_data="set_soff_1")],
        [Btn("➖ Hedge%", callback_data="set_hedge_-5"), Btn(f"{HEDGE_PCT * 100:.0f}%", callback_data="noop"),
         Btn("➕ Hedge%", callback_data="set_hedge_5")],
        [Btn(f"🛡 SL type: {MODE_INFO[SL_MODE]['name']} 🔄", callback_data="set_slm_0")],
        [Btn("➖", callback_data="set_slv_-1"), Btn(limit_short(SL_MODE, SL_VALUE), callback_data="noop"),
         Btn("➕", callback_data="set_slv_1")],
        [Btn(f"🎯 Target type: {MODE_INFO[TP_MODE]['name']} 🔄", callback_data="set_tpm_0")],
        [Btn("➖", callback_data="set_tpv_-1"), Btn(limit_short(TP_MODE, TP_VALUE), callback_data="noop"),
         Btn("➕", callback_data="set_tpv_1")],
        [Btn("🧠 Strategy & Timeframe", callback_data="strategy")],
        [Btn("📅 Expiry select", callback_data="expiry"), Btn("🕒 Time & Rules", callback_data="rules")],
        [Btn(f"🔁 Flip exit: {'ON' if EXIT_ON_FLIP else 'OFF'}", callback_data="set_flip_0")],
        [Btn(f"🔢 Max trades/day: {'ON' if MAX_TRADES_ON else 'OFF'} 🔄", callback_data="set_mton_0")],
        *([[Btn("➖", callback_data="set_mtd_-1"), Btn(f"Trades: {MAX_TRADES_PER_DAY}", callback_data="noop"),
            Btn("➕", callback_data="set_mtd_1")]] if MAX_TRADES_ON else []),
        [Btn(f"🕯 Candle breakout: {'ON' if CANDLE_BREAKOUT else 'OFF'} 🔄", callback_data="set_cbo_0")],
        [Btn(f"🔔 Alert sound: {'ON' if ALERT_SOUND else 'OFF'} 🔄", callback_data="set_snd_0")],
        *dl_rows,
        [Btn(f"📋 {'LIVE' if prof == 'paper' else 'PAPER'} ni setting ahi copy karo", callback_data="set_copy_0")],
        [Btn("⬅️ Back", callback_data="menu")],
    ])


async def _ans(q, *a, **k):
    """Callback answer fail thay (query too old / network) to pan button no action na atke."""
    try:
        await q.answer(*a, **k)
    except Exception as e:
        log.debug("callback answer fail: %s", e)


class _ShimMsg:
    def __init__(self, m):
        self._m = m

    async def reply_text(self, *a, **k):
        return await self._m.reply_text(*a, **k)

    async def reply_document(self, *a, **k):
        return await self._m.reply_document(*a, **k)

    def __getattr__(self, n):
        return getattr(self._m, n)


class _ShimUpdate:
    """Button dabavo = command chalavo: command function ne j call kare (je barobar chale che)."""
    def __init__(self, q, upd):
        self.message = _ShimMsg(q.message)
        self.effective_chat = upd.effective_chat
        self.effective_user = upd.effective_user
        self.callback_query = q


async def on_button_inner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global DAY_LIMIT_ON, DAY_LOSS_LIMIT, DAY_PROFIT_LIMIT, SELL_OFFSET
    global LOTS, HEDGE_PCT, SL_MODE, SL_VALUE, TP_MODE, TP_VALUE, EXIT_ON_FLIP, ONE_TRADE_PER_TREND, HISTORY_DAYS, EXPIRY_MODE, EXPIRY_SKIP_TODAY
    q = update.callback_query
    if ALLOWED_CHAT_ID and q.message.chat_id != ALLOWED_CHAT_ID:
        return await _ans(q, "Not allowed", show_alert=True)
    d = q.data
    app = ctx.application

    async def show(text, kb):
        try:
            await q.edit_message_text(text, reply_markup=kb)
        except Exception as e:
            if "not modified" in str(e).lower():
                return
            log.warning("menu edit failed (%s) -> navo message moklu chhu", e)
            try:
                await q.message.reply_text(text, reply_markup=kb)
            except Exception as e2:
                log.warning("fallback message pan na gayo: %s", e2)

    async def show_html(text, kb):
        try:
            await q.edit_message_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception as e:
            if "not modified" in str(e).lower():
                return
            log.warning("menu edit failed (%s) -> navo message moklu chhu", e)
            try:
                await q.message.reply_text(text, reply_markup=kb, parse_mode="HTML")
            except Exception as e2:
                log.warning("fallback message pan na gayo: %s", e2)

    def _test_kb(extra=None):
        rows = [[Btn("🔍 Connection check (surakshit)", callback_data="test_check")],
                [Btn("📨 Test order (sacho order)", callback_data="test_ask")]]
        if extra:
            rows = extra
        return Markup(rows + [[Btn("⬅️ Back", callback_data="menu")]])

    TEST_HOME = ("🧪 Angel One test\n\n"
                 "🔍 Connection check: login, funds ane order IP check. Koi order nathi jatu.\n"
                 "📨 Test order: 1 lot BUY LIMIT ₹0.05 sacho order moklse ane turant cancel karse.")

    async def _handle_cfg(prof, inner):
        if inner == "set_copy_0":
            err = copy_profile(prof)
            if err:
                return await _ans(q, err, show_alert=True)
            toast, alert, text, kb = cfg_handle(prof, "settings")
            toast = "Copy thayu ✅" + (" • Bot pause" if prof == state["mode"] else "")
        else:
            toast, alert, text, kb = cfg_handle(prof, inner)
        await _ans(q, toast or None, show_alert=alert) if toast else await _ans(q)
        await show(text, kb)

    async def _handle_sx(inner, prefix):
        """Navi strategy add / juni strategy remove na buttons. prefix = 'cfg|<prof>|' athva ''."""
        prof = prefix.split("|")[1] if prefix else state["mode"]
        back = prefix + "strategy"

        async def del_page():
            rows = [[Btn(f"🗑 {v['label']}{' (select che)' if strategy_in_use(k) else ''}",
                         callback_data=f"{prefix}sx_ask_{k}")] for k, v in STRATEGIES.items()]
            rows.append([Btn("♻️ Built-in strategy pacha lavo", callback_data=prefix + "sx_restore")])
            rows.append([Btn("⬅️ Back", callback_data=back)])
            await show("🗑 Kai strategy remove karvi che?\n━━━━━━━━━━━━━━━━━━\n"
                       "Paper/Live ma select hoy te remove nahi thay.", Markup(rows))

        if inner == "sx_new":
            ctx.user_data["await_strat"] = True
            await _ans(q)
            return await show(CUSTOM_HELP, Markup([[Btn("❌ Cancel", callback_data=prefix + "sx_cancel")]]))
        if inner == "sx_cancel":
            ctx.user_data.pop("await_strat", None)
            await _ans(q, "Cancel")
            return await _handle_cfg(prof, "strategy")
        if inner == "sx_del":
            await _ans(q)
            return await del_page()
        if inner.startswith("sx_ask_"):
            key = inner[7:]
            if key not in STRATEGIES:
                await _ans(q, "Aa strategy nathi", show_alert=True)
                return await del_page()
            if strategy_in_use(key):
                return await _ans(q, "Aa strategy Paper/Live ma select che. Pehla biji select karo.", show_alert=True)
            await _ans(q)
            v = STRATEGIES[key]
            body = strategy_detail(key) if v.get("custom") else _BUILTIN_DESC.get(key, v["label"])
            return await show(f"⚠️ Pakka remove karvi?\n\n{body}",
                              Markup([[Btn("✅ Haa, remove", callback_data=f"{prefix}sx_ok_{key}"),
                                       Btn("❌ Na", callback_data=prefix + "sx_del")]]))
        if inner.startswith("sx_ok_"):
            err = remove_strategy(inner[6:])
            await _ans(q, err or "Remove thayu ✅", show_alert=bool(err))
            return await del_page()
        if inner == "sx_restore":
            restore_builtin_strategies()
            await _ans(q, "Built-in strategy pacha aavi ✅")
            return await del_page()
        await _ans(q)

    try:
        if d.startswith("sx_"):
            return await _handle_sx(d, "")
        if d.startswith("cfg|") and d.split("|", 2)[2].startswith("sx_"):
            _, prof_x, inner_x = d.split("|", 2)
            return await _handle_sx(inner_x, f"cfg|{prof_x}|")
        if d == "settings":
            await _ans(q)
            text_, kb_ = settings_hub()
            return await show(text_, kb_)
        if d.startswith("cfg|"):
            _, prof_, inner_ = d.split("|", 2)
            return await _handle_cfg(prof_, inner_)
        if d.startswith(("set_", "exp_", "rule_", "tf_", "strat_")) or d in ("expiry", "rules", "strategy"):
            return await _handle_cfg(state["mode"], d)          # juna messages na buttons pan chale
        DIRECT = {"run": (cmd_run, "▶️ Bot chalu"), "stop": (cmd_stop, "⏸ Bot band"), "status": (cmd_status, "📊 Status..."),
                  "pnl": (cmd_pnl, "💰 PnL..."), "signal": (cmd_signal, "📡 Signal..."),
                  "summary": (cmd_summary, "📅 Summary..."), "trades": (cmd_trades, "📜 Trades..."),
                  "help": (cmd_help, "❓ Help"), "live": (cmd_live, "💹 Live PnL...")}
        if d in DIRECT:
            fn_, toast = DIRECT[d]
            await _ans(q, toast)
            await fn_(_ShimUpdate(q, update), ctx)           # command j chale che (jem /run, /status)
            if d in ("run", "stop"):
                await show(main_menu_text(), main_menu_kb())
            return
        if d == "angelorders":
            await _ans(q, "Angel thi orders lai rahyo...")
            pages = await asyncio.to_thread(_orders_pages)
            kb_ = Markup([[Btn("🔄 Refresh", callback_data="angelorders"), Btn("📥 Trades sync", callback_data="angelsync")],
                          [Btn("⬅️ Back", callback_data="menu")]])
            if len(pages) == 1:
                return await show(pages[0], kb_)
            await show(pages[0], None)
            for i_, pg in enumerate(pages[1:], 1):
                await q.message.reply_text(pg, reply_markup=kb_ if i_ == len(pages) - 1 else None)
            return
        if d == "angelsync":
            await _ans(q, "Sync chalu...")
            return await q.message.reply_text(await apply_angel_sync())
        if d == "angeltest":
            await _ans(q, )
            return await show(TEST_HOME, _test_kb())
        if d == "test_check":
            await _ans(q, "Check chalu...")
            await show("⏳ Check chalu...", _test_kb())
            txt = await asyncio.to_thread(_test_checks)
            return await show(txt, _test_kb())
        if d == "test_ask":
            await _ans(q, )
            return await show("⚠️ Aa SACHO order Angel One ma jase.\n"
                              "ATM CE, 1 lot, BUY LIMIT ₹0.05 (fill thavani sambhavna nathi), turant cancel thase.\n"
                              "Market band hoy to reject thase, pan connection/IP no error dekhase.\n\nChalu karvu che?",
                              Markup([[Btn("✅ Haa, test order moklo", callback_data="test_go")],
                                      [Btn("❌ Na", callback_data="angeltest")]]))
        if d == "test_go":
            await _ans(q, "Test order chalu...")
            await show("⏳ Test order chalu...", _test_kb())
            txt = await asyncio.to_thread(_test_order_roundtrip)
            return await show(txt, _test_kb())
        if d == "noop":
            return await _ans(q, )
        if d == "close":
            await _ans(q, )
            return await q.message.delete()
        if d == "menu":
            await _ans(q, "🔄 Refreshed")
            return await show(main_menu_text(), main_menu_kb())
        if d == "run":
            state["running"] = True
            state["last_signal_candle"] = None
            await _ans(q, "Bot chalu thayo ▶️")
            return await show(main_menu_text(), main_menu_kb())
        if d == "stop":
            state["running"] = False
            await _ans(q, "Bot band ⏸")
            return await show(main_menu_text(), main_menu_kb())
        if d == "dashboard":
            await _ans(q, "Dashboard khuli rahyu chhe...")
            return await start_dashboard(app, q.message.chat_id)
        if d == "dash_mode":
            state["dash_compact"] = not state["dash_compact"]
            state["dash_msg_id"], state["dash_chat"] = q.message.message_id, q.message.chat_id
            save_state()
            await _ans(q, "Compact view" if state["dash_compact"] else "Full view")
            return await update_dashboard(app, force=True)
        if d == "dash_refresh":
            await _ans(q, "Refreshed")
            state["dash_msg_id"], state["dash_chat"] = q.message.message_id, q.message.chat_id
            save_state()
            return await update_dashboard(app, force=True)
        if d == "dash_stop":
            state["dash_msg_id"] = None
            save_state()
            await _ans(q, "Auto-update band")
            return await show("⏹ Dashboard auto-update band.\n/dashboard thi pharithi chalu karo.",
                              Markup([[Btn("📺 Dashboard", callback_data="dashboard"),
                                       Btn("🏠 Menu", callback_data="menu_new")]]))
        if d == "menu_new":
            await _ans(q, )
            return await q.message.reply_text(main_menu_text(), reply_markup=main_menu_kb())
        if d == "history":
            await _ans(q, )
            return await q.message.reply_text(history_menu_text(), reply_markup=history_kb())
        if d == "hmenu":
            await _ans(q, )
            return await show(history_menu_text(), history_kb())
        if d.startswith("h_"):
            parts = d.split("_")
            if parts[1] == "sum":
                await _ans(q, )
                return await show(
                    stats_text(load_trades(parts[2]), scope_label(parts[2]), daywise=parts[2] != "today"),
                    Markup([[Btn("🧾 Trade details", callback_data=f"h_tr_{parts[2]}_0"),
                             Btn("🗂 History", callback_data="hmenu")]]))
            if parts[1] == "tr":
                await _ans(q, )
                text, kb = trade_page(parts[2], int(parts[3]))
                return await show(text, kb)
            if parts[1] == "csv":
                data = history_csv_bytes()
                if not data:
                    return await _ans(q, "History khali che", show_alert=True)
                await _ans(q, "File moklu chhu...")
                return await q.message.reply_document(document=data, filename="trades_history.csv",
                                                      caption="📥 Full trade history (Excel ma khuli shake)")
        if d == "expiry":
            await _ans(q, )
            return await show(expiry_text(), expiry_kb())
        if d.startswith("exp_"):
            key = d[4:]
            if key == "skip":
                EXPIRY_SKIP_TODAY = not EXPIRY_SKIP_TODAY
            elif key in EXPIRY_MODES:
                EXPIRY_MODE = key
            await _ans(q, f"Expiry: {expiry_label()}")
            return await show(expiry_text(), expiry_kb())
        if d == "rules":
            await _ans(q, )
            return await show(rules_text(), rules_kb())
        if d.startswith("rule_"):
            _, key, val = d.split("_", 2)
            v = int(val)
            if key in ("tstart", "tlast", "tend"):
                which = {"tstart": "start", "tlast": "last", "tend": "end"}[key]
                cur = {"start": ENTRY_START, "last": LAST_ENTRY, "end": FORCE_EXIT}[which]
                err = set_time_value(which, _shift_time(cur, v * 5))
                if err:
                    return await _ans(q, err, show_alert=True)
            elif key == "otpt":
                ONE_TRADE_PER_TREND = not ONE_TRADE_PER_TREND
                if not ONE_TRADE_PER_TREND:
                    state["blocked_dir"] = None
            elif key == "hist":
                if state["position"]:
                    return await _ans(q, "Position open che, pehla exit karo", show_alert=True)
                HISTORY_DAYS = step_history(v)
                _sig_cache["val"], _sig_cache["bucket"] = None, None
                state["last_signal_candle"] = None
                state["running"] = False            # signal badlay to surprise entry na thay
            await _ans(q, "Updated")
            return await show(rules_text(), rules_kb())
        if d == "strategy":
            await _ans(q, )
            return await show(strategy_text(), strategy_kb())
        if d.startswith("tf_"):
            if state["position"]:
                return await _ans(q, "Position open che, pehla exit karo", show_alert=True)
            m = int(d[3:])
            if m in TIMEFRAMES:
                set_timeframe(m)
                state["last_signal_candle"] = None
                state["running"] = False        # surprise entry na thay etle pause
            await _ans(q, f"Timeframe: {CANDLE_MINUTES} min • Bot paused")
            return await show(strategy_text(), strategy_kb())
        if d.startswith("strat_"):
            if state["position"]:
                return await _ans(q, "Position open che, pehla exit karo", show_alert=True)
            key = d[6:]
            if key in STRATEGIES:
                state["strategy"] = key
                _sig_cache["val"], _sig_cache["bucket"] = None, None
                state["last_signal_candle"] = None
                state["running"] = False        # surprise entry na thay etle pause
            await _ans(q, f"Strategy: {strategy_label()} • Bot paused")
            return await show(strategy_text(), strategy_kb())
        if d == "signal":
            await _ans(q, "Signal check thai rahyo chhe...")
            return await q.message.reply_text(await signal_text())
        if d == "summary":
            await _ans(q, )
            return await q.message.reply_text(await summary_text())
        if d == "status":
            await _ans(q, "Status mangavi rahyo chhu...")
            return await q.message.reply_text(await status_text())
        if d == "pnl":
            await _ans(q, )
            return await q.message.reply_text(await pnl_text())
        if d == "trades":
            await _ans(q, )
            text, kb = trade_page("today", 0)
            return await q.message.reply_text(text, reply_markup=kb)
        if d == "help":
            await _ans(q, )
            return await q.message.reply_text(HELP_TEXT)
        if d == "live":
            if not state["position"]:
                return await _ans(q, "Koi open position nathi", show_alert=True)
            await _ans(q, )
            pnl, s_, h_ = await position_pnl()
            state["live_msg_id"] = None
            return await update_live(app, pnl, s_, h_, force=True)

        # ---- mode
        if d in ("mode_paper", "mode_live", "mode_live_yes"):
            if state["position"]:
                return await _ans(q, "Position open che, pehla exit karo", show_alert=True)
            if d == "mode_live":
                await _ans(q, )
                return await show("⚠️ LIVE mode ma Angel One ma REAL orders jashe.\nPakka live karvu che?",
                                  Markup([[Btn("✅ Yes, LIVE", callback_data="mode_live_yes"),
                                           Btn("↩️ Cancel", callback_data="menu")]]))
            switch_mode("live" if d == "mode_live_yes" else "paper")
            await _ans(q, f"Mode: {state['mode'].upper()} • alag setting • Bot pause")
            return await show(main_menu_text(), main_menu_kb())

        # ---- exit position (confirm)
        if d == "exit":
            if not state["position"]:
                return await _ans(q, "Koi open position nathi", show_alert=True)
            await _ans(q, )
            return await show("🚪 Position abhi exit karvi che?",
                              Markup([[Btn("✅ Yes, Exit", callback_data="exit_yes"),
                                       Btn("↩️ Cancel", callback_data="menu")]]))
        if d == "exit_yes":
            await _ans(q, "Exit thai rahyu chhe...")
            if state["position"]:
                await close_position(app, "Manual exit (button)")
            return await show(main_menu_text(), main_menu_kb())

        # ---- settings
        if d == "settings":
            await _ans(q, )
            return await show(settings_text(), settings_kb())
        if d.startswith("set_"):
            _, key, val = d.split("_", 2)
            v = int(val)
            if key == "lots":
                LOTS = max(1, min(50, LOTS + v))
            elif key == "copy":
                other = "live" if state["mode"] == "paper" else "paper"
                if state["position"]:
                    return await _ans(q, "Position open che, pehla exit karo", show_alert=True)
                if not PROFILES.get(other):
                    return await _ans(q, f"{other.upper()} ni setting haju save nathi", show_alert=True)
                apply_settings(PROFILES[other])
                state["running"] = False
                state["last_signal_candle"] = None
            elif key == "soff":
                SELL_OFFSET = max(-5, min(5, SELL_OFFSET + v))
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
            elif key == "dlim":
                DAY_LIMIT_ON = not DAY_LIMIT_ON
                if not DAY_LIMIT_ON:
                    state["day_halt"] = None
                state["limit_override"] = False
            elif key == "dll":
                DAY_LOSS_LIMIT = max(DAY_LIMIT_STEP, min(200000.0, DAY_LOSS_LIMIT + v * DAY_LIMIT_STEP))
            elif key == "dlp":
                DAY_PROFIT_LIMIT = max(DAY_LIMIT_STEP, min(200000.0, DAY_PROFIT_LIMIT + v * DAY_LIMIT_STEP))
            elif key == "dres":
                state["day_halt"] = None
                state["limit_override"] = True
                state["last_signal_candle"] = None
            await _ans(q, "Updated")
            return await show(settings_text(), settings_kb())
        await _ans(q, )
    except Exception as e:
        log.exception("button error")
        await _ans(q, f"Error: {str(e)[:150]}", show_alert=True)


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


def _fsize(path) -> str:
    try:
        b = os.path.getsize(path)
    except OSError:
        return "nathi"
    return f"{b / 1024:,.1f} KB" if b < 1024 * 1024 else f"{b / 1024 / 1024:,.2f} MB"


def storage_text() -> str:
    """Bot ni saved files ketli jagya roke che ane ketla divas no data che."""
    vol = os.path.abspath(DATA_DIR) != os.path.abspath(BASE_DIR)
    L = ["💾 Storage",
         f"Folder: {DATA_DIR}",
         ("✅ Alag DATA_DIR (Volume hoy to deploy par data nahi jay)" if vol else
          "⚠️ DATA_DIR set nathi: Railway par darek deploy/restart par file delete thase"),
         "",
         f"bot_state.json: {_fsize(STATE_FILE)}",
         f"trades_history.csv: {_fsize(HISTORY_FILE)}"]
    try:
        rows = read_history()
        if rows:
            dates = sorted({str(r.get("date") or "") for r in rows if r.get("date")})
            L.append(f"History: {len(rows)} trades • {len(dates)} divas ({dates[0]} thi {dates[-1]})")
            L.append(f"Roj no average: {len(rows) / max(1, len(dates)):.1f} trades")
        else:
            L.append("History: haju koi trade nathi")
    except Exception as e:
        L.append(f"History vanchay nahi: {e}")
    L.append(f"Aaj na trades (state ma): {len(state['trades'])}")
    try:
        du = shutil.disk_usage(DATA_DIR)
        L.append(f"Disk: {du.used / 1024 ** 3:,.2f} GB vaparyu / {du.total / 1024 ** 3:,.1f} GB (free {du.free / 1024 ** 3:,.2f} GB)")
    except Exception:
        pass
    return "\n".join(L)


async def cmd_storage(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text(await asyncio.to_thread(storage_text))


async def cmd_strike(update: Update, ctx):
    global SELL_OFFSET
    if not authorized(update):
        return
    usage = (f"🎯 Sell strike: {sell_label(SELL_OFFSET)}\n\n/strike atm\n/strike otm1 ... otm5\n/strike itm1 ... itm5\n"
             f"Hedge sold option na premium na {HEDGE_PCT * 100:.0f}% mujab, sell strike thi bahar select thase.\n"
             f"(Navi trade thi lagu)")
    if not ctx.args:
        return await update.message.reply_text(usage)
    a = ctx.args[0].lower().replace("-", "").replace("_", "")
    m = re.fullmatch(r"(atm|otm([1-5])|itm([1-5]))", a)
    if not m:
        return await update.message.reply_text("❌ Samajyu nahi.\n\n" + usage)
    SELL_OFFSET = 0 if a == "atm" else (int(m.group(2)) if m.group(2) else -int(m.group(3)))
    save_state()
    await update.message.reply_text(f"✅ Sell strike: {sell_label(SELL_OFFSET)} (navi trade thi lagu)")


async def cmd_daylimit(update: Update, ctx):
    global DAY_LIMIT_ON, DAY_LOSS_LIMIT, DAY_PROFIT_LIMIT
    if not authorized(update):
        return
    usage = (f"{day_limit_text()}\n\n/daylimit on | off\n/daylimit loss 2000\n/daylimit profit 3000\n"
             f"/daylimit resume  (limit hit thayu hoy to aaj mate chalu rakho)")
    a = [x.lower() for x in ctx.args]
    if not a:
        return await update.message.reply_text(usage)
    if a[0] in ("on", "off"):
        DAY_LIMIT_ON = a[0] == "on"
        if not DAY_LIMIT_ON:
            state["day_halt"] = None
        state["limit_override"] = False
    elif a[0] == "resume":
        state["day_halt"] = None
        state["limit_override"] = True
        state["last_signal_candle"] = None
    elif a[0] in ("loss", "profit") and len(a) == 2:
        try:
            v = float(a[1].replace(",", "").replace("₹", ""))
        except ValueError:
            return await update.message.reply_text("❌ Amount samajyo nahi.\n\n" + usage)
        if not 100 <= v <= 500000:
            return await update.message.reply_text("❌ Amount 100 thi 5,00,000 vache joiye.")
        if a[0] == "loss":
            DAY_LOSS_LIMIT = v
        else:
            DAY_PROFIT_LIMIT = v
    else:
        return await update.message.reply_text("❌ Samajyu nahi.\n\n" + usage)
    save_state()
    await update.message.reply_text("✅ " + day_limit_text())


def _test_checks() -> str:
    """Read-only check: login, funds, bahar nu IP. Koi order nathi jatu."""
    out = []
    out.append("✅ Angel login: chalu" if AUTH.get("jwt") else "❌ Angel login: token nathi")
    try:
        r = throttled(smart.rmsLimit)
        if isinstance(r, dict) and r.get("status") and r.get("data"):
            d = r["data"]
            out.append(f"💰 Available cash: ₹{float(d.get('availablecash', 0)):,.0f}")
        else:
            out.append(f"❌ Funds check fail: {r}")
    except Exception as e:
        out.append(f"❌ Funds check fail: {e}")
    try:
        px = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
        ip = requests.get("https://api.ipify.org", proxies=px, timeout=10).text.strip()
        out.append(f"🌐 Orders aa IP thi jase: {ip}" + (" (proxy)" if PROXY_URL else " (proxy nathi)"))
        out.append("   Aa IP Angel One SmartAPI ma register hovo joie.")
    except Exception as e:
        out.append(f"❌ IP check fail: {e}")
    return "\n".join(out)


def _test_order_roundtrip() -> str:
    """Sacho order test: ATM CE nu 1 lot BUY LIMIT @ ₹0.05 (fill nahi thay), pachhi turant cancel."""
    spot = nifty_spot()
    atm = round(spot / STRIKE_STEP) * STRIKE_STEP
    opt = find_option(atm, "CE", nearest_expiry())
    params = {"variety": "NORMAL", "tradingsymbol": opt["symbol"], "symboltoken": opt["token"],
              "transactiontype": "BUY", "exchange": "NFO", "ordertype": "LIMIT",
              "producttype": PRODUCT_TYPE, "duration": "DAY", "price": "0.05", "squareoff": "0",
              "stoploss": "0", "quantity": str(LOT_SIZE)}
    lines = [f"Test order: BUY {opt['symbol']} x{LOT_SIZE} LIMIT ₹0.05"]
    try:
        oid = throttled(smart.placeOrder, params, retries=0)
    except Exception as e:
        lines.append(f"❌ Order place fail: {e}")
        lines.append("(IP/token/margin/market-closed jevo error hoy to upar dekhase)")
        return "\n".join(lines)
    lines.append(f"✅ Angel One e order swikaryo. Order ID: {oid}")
    time.sleep(2)
    status = "?"
    try:
        ob = throttled(smart.orderBook)
        for o in (ob.get("data") or []):
            if str(o.get("orderid")) == str(oid):
                status = f"{o.get('orderstatus')} {o.get('text') or ''}".strip()
    except Exception as e:
        status = f"orderbook error: {e}"
    lines.append(f"📋 Status: {status}")
    try:
        c = throttled(smart.cancelOrder, str(oid), "NORMAL")
        lines.append(f"🧹 Cancel: {c}")
    except Exception as e:
        lines.append(f"⚠️ Cancel na thayu ({e}). Angel One app ma jai ne order {oid} cancel karo!")
    return "\n".join(lines)


async def cmd_maxtrades(update: Update, ctx):
    global MAX_TRADES_PER_DAY, MAX_TRADES_ON
    if not authorized(update):
        return
    cur = f"ON ({MAX_TRADES_PER_DAY})" if MAX_TRADES_ON else f"OFF (value {MAX_TRADES_PER_DAY})"
    usage = f"🔢 Max trades/day ({state['mode'].upper()}): {cur}\n\n/maxtrades on | off\n/maxtrades 8   (8 set karine ON)"
    if not ctx.args:
        return await update.message.reply_text(usage)
    a = ctx.args[0].lower()
    if a == "off":
        MAX_TRADES_ON = False
    elif a == "on":
        MAX_TRADES_ON = True
    elif a.isdigit() and 1 <= int(a) <= 100:
        MAX_TRADES_PER_DAY, MAX_TRADES_ON = int(a), True
    else:
        return await update.message.reply_text("❌ Samajyu nahi.\n\n" + usage)
    if str(state.get("day_halt") or "").startswith("MAX TRADES") and max_trades_reason(after_close=True) is None:
        state["day_halt"] = None
    save_state()
    await update.message.reply_text("✅ Max trades/day: " + (f"ON ({MAX_TRADES_PER_DAY})" if MAX_TRADES_ON else "OFF"))


async def cmd_breakout(update: Update, ctx):
    global CANDLE_BREAKOUT
    if not authorized(update):
        return
    if ctx.args and ctx.args[0].lower() in ("on", "off"):
        CANDLE_BREAKOUT = ctx.args[0].lower() == "on"
        _sig_cache["val"], _sig_cache["bucket"] = None, None
        save_state()
    await update.message.reply_text(
        f"🕯 Candle breakout ({state['mode'].upper()}): {'ON' if CANDLE_BREAKOUT else 'OFF'}\n"
        "Bullish: close > pichhli candle High\nBearish: close < pichhli candle Low\n/breakout on | /breakout off")


async def cmd_sound(update: Update, ctx):
    global ALERT_SOUND
    if not authorized(update):
        return
    if ctx.args and ctx.args[0].lower() in ("on", "off"):
        ALERT_SOUND = ctx.args[0].lower() == "on"
        save_state()
    await update.message.reply_text(f"🔔 Alert sound ({state['mode'].upper()}): {'ON' if ALERT_SOUND else 'OFF (silent)'}\n"
                                    f"/sound on | /sound off")


async def cmd_chatid(update: Update, ctx):
    """Koi pan chat/group ma chalavo: te chat nu id batave (alert chat set karva mate)."""
    await update.message.reply_text(f"Chat id: {update.effective_chat.id}")


async def cmd_sync(update: Update, ctx):
    if not authorized(update):
        return
    await update.message.reply_text("⏳ Angel orders thi trades sync...")
    await update.message.reply_text(await apply_angel_sync())


async def cmd_orders(update: Update, ctx):
    if not authorized(update):
        return
    for pg in await asyncio.to_thread(_orders_pages):
        await update.message.reply_text(pg)


async def cmd_testorder(update: Update, ctx):
    if not authorized(update):
        return
    arg = ctx.args[0].lower() if ctx.args else ""
    if arg == "order":
        if not ctx.args[1:] or ctx.args[1].lower() != "yes":
            return await update.message.reply_text(
                "⚠️ Aa SACHO order Angel One ma jase (1 lot BUY LIMIT ₹0.05, fill thavani sambhavna nathi, "
                "turant cancel thase).\nMarket band hoy to reject thase, pan connection/IP no error dekhase.\n\n"
                "Chalu karva: /testorder order yes")
        await update.message.reply_text("⏳ Test order chalu...")
        return await update.message.reply_text(await asyncio.to_thread(_test_order_roundtrip))
    await update.message.reply_text("⏳ Check chalu...")
    txt = await asyncio.to_thread(_test_checks)
    await update.message.reply_text(txt + "\n\nSacho order test mate: /testorder order")


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


async def on_error(update, ctx: ContextTypes.DEFAULT_TYPE):
    """Koi handler ma error aave to log kare ane button dabavnar ne jaan kare (silent fail nahi)."""
    log.error("handler error: %s", ctx.error, exc_info=ctx.error)
    try:
        if isinstance(update, Update) and update.callback_query:
            await _ans(update.callback_query, "⚠️ Error thayo, fari dabavo", show_alert=False)
    except Exception:
        pass


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        await on_button_inner(update, ctx)
    finally:
        save_state()      # settings / mode / run badlay to save


async def post_init(app: Application):
    """Bot ni default Menu button (message box ni baju ma) ma commands batave."""
    await app.bot.set_my_commands([
        BotCommand("menu", "🏠 Main menu (buttons)"),
        BotCommand("dashboard", "📺 Dashboard (live auto refresh)"),
        BotCommand("signal", "📡 Strategy signal"),
        BotCommand("summary", "📅 Today summary (full detail)"),
        BotCommand("trades", "🧾 Today trades (lot, rate, PnL)"),
        BotCommand("export", "📥 History CSV file"),
        BotCommand("expiry", "📅 Expiry select (weekly/monthly)"),
        BotCommand("daylimit", "🛑 Daily profit/loss limit"),
        BotCommand("testorder", "🧪 Angel One connection / order test"),
        BotCommand("orders", "📒 Angel One aaj na orders"),
        BotCommand("strike", "🎯 Sell strike (ATM / OTM / ITM)"),
        BotCommand("storage", "💾 Saved data / storage"),
        BotCommand("keyboard", "⌨️ Neeche na buttons (/keyboard off = band)"),
        BotCommand("sync", "📥 Angel orders thi trades bot ma umero"),
        BotCommand("maxtrades", "🔢 Max trades per day"),
        BotCommand("sound", "🔔 Alert sound ON/OFF"),
        BotCommand("breakout", "🕯 Candle breakout condition ON/OFF"),
        BotCommand("newstrategy", "➕ Navi strategy + condition add"),
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
    if ALLOWED_CHAT_ID:
        try:
            if restore_notes:
                tail = ("\n\nBot RUNNING che." if state["running"]
                        else "\n\nBot STOPPED che. Navi entry mate neeche ▶️ Run dabavo.")
                msg = "♻️ Bot restart thayo, juno data restore karyo:\n" + "\n".join(restore_notes) + tail
            else:
                msg = "🟢 Bot online. Neeche na buttons vapro 👇"
            await app.bot.send_message(ALLOWED_CHAT_ID, msg, reply_markup=bottom_kb())
        except Exception as e:
            log.warning("startup message fail: %s", e)
        if state["mode"] == "live":
            try:                                   # restart/deploy ma state gayu hoy to Angel thi trades pacha lavo
                txt = await apply_angel_sync()
                if "navu bot ma umeryu" in txt and " 0 navu" not in txt:
                    await app.bot.send_message(ALLOWED_CHAT_ID, txt)
            except Exception as e:
                log.warning("startup sync fail: %s", e)


# ----------------------------- DASHBOARD ------------------------------
def pbar(pnl, lo, hi, n=10) -> str:
    frac = 0.5 if hi == lo else (pnl - lo) / (hi - lo)
    frac = max(0.0, min(1.0, frac))
    f = round(frac * n)
    return "▓" * f + "░" * (n - f)


async def spot_cached(max_age=3):
    w = ws_ltp(NIFTY_TOKEN)
    if w is not None:
        return w
    c = state["spot_cache"]
    if c and time.time() - c[1] < max_age:
        return c[0]
    v = await asyncio.to_thread(nifty_spot)
    state["spot_cache"] = (v, time.time())
    return v


async def pnl_cached(max_age=3):
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
        [Btn("📋 Full view" if state["dash_compact"] else "📏 Compact view", callback_data="dash_mode"),
         Btn("🏠 Menu", callback_data="menu_new")],
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


# ---------------- ANGEL ACCOUNT INFO (dashboard mate cache) ----------------
acct = {"funds": None, "ok": None, "err": "", "t": 0.0, "ip": None, "ip_t": 0.0, "busy": False}


def _acct_fetch():
    """Funds + bahar nu IP lave (background thread). Dashboard ne block nathi karto."""
    try:
        try:
            r = throttled(smart.rmsLimit)
            if isinstance(r, dict) and r.get("status") and r.get("data"):
                acct["funds"], acct["ok"], acct["err"] = r["data"], True, ""
            else:
                acct["ok"] = False
                acct["err"] = str((r or {}).get("message") or r)[:70] if isinstance(r, dict) else str(r)[:70]
        except Exception as e:
            acct["ok"], acct["err"] = False, str(e)[:70]
        acct["t"] = time.time()
        if time.time() - acct["ip_t"] > 600:
            try:
                px = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
                acct["ip"] = requests.get("https://api.ipify.org", proxies=px, timeout=6).text.strip()
            except Exception:
                acct["ip"] = None
            acct["ip_t"] = time.time()
    finally:
        acct["busy"] = False


def kick_acct(max_age=60):
    if acct["busy"] or time.time() - acct["t"] < max_age:
        return
    acct["busy"] = True
    asyncio.get_running_loop().run_in_executor(None, _acct_fetch)


def _fnum(d, *keys) -> float:
    for k in keys:
        try:
            if d.get(k) not in (None, ""):
                return float(d[k])
        except Exception:
            pass
    return 0.0


def angel_conn_status(feed_ok: bool, mkt_open: bool) -> str:
    if not AUTH.get("jwt"):
        return "🔴 Login nathi"
    if acct["ok"] is False:
        return f"🔴 API error: {acct['err']}"
    if acct["ok"] is None:
        return "🟡 Check thay che..."
    age = time.time() - acct["t"]
    if age > 180:
        return f"🟡 Data juno ({age / 60:.0f} min)"
    if mkt_open and not feed_ok:
        return "🟡 Connected (live feed nathi, REST)"
    return "🟢 Connected"


def angel_block(feed_ok: bool, mkt_open: bool) -> list:
    """Dashboard no Angel One section. Live ma pura details, paper ma ek line."""
    esc = html.escape
    if state["mode"] != "live":
        ok = "🟢 Login chalu" if AUTH.get("jwt") else "🔴 Login nathi"
        return [f"<b>🏦 ANGEL</b>  {ok} • <code>{esc(ANGEL_CLIENT_ID or '-')}</code> • "
                f"<i>Paper mode (sacha orders nathi jata)</i>", ""]
    kick_acct()
    f = acct["funds"] or {}
    login_age = (time.time() - _last_login[0]) / 60 if _last_login[0] else None
    rows = [("User", ANGEL_CLIENT_ID or "-")]
    if f:
        rows += [("Funds", f"₹{_fnum(f, 'availablecash', 'net'):,.0f} available"),
                 ("Margin", f"₹{_fnum(f, 'utilisedamount', 'utiliseddebits'):,.0f} used")]
    else:
        rows.append(("Funds", "load thay che..." if acct["ok"] is None else "n/a"))
    rows.append(("Feed", "⚡ WS live" if feed_ok else "🌐 REST polling"))
    if login_age is not None:
        rows.append(("Login", f"{login_age:.0f} min pehla"))
    if acct["ip"]:
        rows.append(("Order IP", acct["ip"] + (" (proxy)" if PROXY_URL else "")))
    return [f"<b>🏦 ANGEL ONE</b>  {angel_conn_status(feed_ok, mkt_open)}", kv(rows), ""]


def _chg_row(tr, total) -> list:
    """Aaj na live trades na Angel charges (hoy to) - dashboard TODAY ma."""
    cs = [float(t.get("charges")) for t in tr if t.get("charges") not in (None, "")]
    if not cs:
        return []
    return [("Charges", f"₹{sum(cs):,.0f} • Net {money(total - sum(cs))}")]


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
    L += angel_block(feed_ok, mkt_open)

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
            if p.get("orders"):
                o_ = p["orders"]
                L.append("📒 Angel: " + " • ".join(
                    f"{lab} #{(o_.get(k) or {}).get('oid')} {'✅' if (o_.get(k) or {}).get('status') == 'complete' else '❌'}"
                    for k, lab in (("sell", "SELL"), ("hedge", "BUY")) if o_.get(k)))
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
    tr = mode_trades()
    wins = sum(1 for t in tr if t["pnl"] > 0)
    total = mode_realized() + open_pnl
    wr = wins / len(tr) if tr else 0
    best = max((t["pnl"] for t in tr), default=0)
    worst = min((t["pnl"] for t in tr), default=0)
    L.append(f"<b>📅 TODAY</b>  {'🟢' if total >= 0 else '🔴'} <b>{money(total)}</b>")
    L.append(kv([
        ("Trades", f"{len(tr)}{'/' + str(max_trades_limit()) if max_trades_limit() else ''}   ✅{wins} ❌{len(tr) - wins}"),
        ("Win", f"{bar(wr)} {wr * 100:.0f}%" if tr else "-"),
        ("Realized", money(mode_realized())),
        ("Open", money(open_pnl)),
        ("Best", money(best)),
        ("Worst", money(worst)),
        ("Last", state["last_event"]),
    ] + _chg_row(tr, total)))
    L.append(f"<i>🛡 SL {limit_short(SL_MODE, SL_VALUE)} • 🎯 TP {limit_short(TP_MODE, TP_VALUE)} • Hedge {HEDGE_PCT * 100:.0f}% • "
             f"Lots {LOTS} • 🕒 {ENTRY_START:%H:%M}-{LAST_ENTRY:%H:%M} (exit {FORCE_EXIT:%H:%M}) • ♻️ {DASH_REFRESH}s</i>")
    return "\n".join(L)


async def dashboard_compact_text() -> str:
    """Nano dashboard: 6-7 lines, mobile ma ek screen ma."""
    esc = html.escape
    now = datetime.now()
    mkt_open = now.weekday() < 5 and dtime(9, 15) <= now.time() <= dtime(15, 30)
    feed_ok = ws_ltp(NIFTY_TOKEN) is not None
    live = state["mode"] == "live"
    L = [f"<b>📺 Dashboard</b> {'🟢' if mkt_open else '🔴'}{'⚡' if feed_ok else '🌐'} {now:%H:%M:%S} • "
         f"{'⚡LIVE' if live else '📝PAPER'} • {'▶️Run' if state['running'] else '⏸Stop'}"]
    if live:
        kick_acct()
        f = acct["funds"] or {}
        bal = f"₹{_fnum(f, 'availablecash', 'net'):,.0f}" if f else "..."
        L.append(f"🏦 {esc(ANGEL_CLIENT_ID or '-')} • {bal} • {angel_conn_status(feed_ok, mkt_open)}")
    try:
        d, ct, st = await asyncio.to_thread(current_signal)
        spot = await spot_cached()
        L.append(f"📡 {esc(strategy_label())} {CANDLE_MINUTES}m {trend_name(d)} • Nifty {spot:,.0f} "
                 f"({'▲' if spot - st >= 0 else '▼'}{abs(spot - st):,.0f})")
    except Exception as e:
        L.append(f"📡 n/a ({esc(str(e)[:40])})")
    open_pnl = 0.0
    p = state["position"]
    if p:
        try:
            pnl, s_, h_ = await pnl_cached()
            open_pnl = pnl
            L.append(f"📌 {p['type']} • S {p['sell_entry']:.1f}→{s_:.1f} • B {p['hedge_entry']:.1f}→{h_:.1f}")
            L.append(f"{'🟢' if pnl >= 0 else '🔴'} <b>Open {money(pnl)}</b> • SL {p['sl']:+,.0f} / TP {p['tp']:+,.0f}")
        except Exception as e:
            L.append(f"📌 open (PnL n/a: {esc(str(e)[:30])})")
    else:
        L.append("📌 No position")
    tr = mode_trades()
    wins = sum(1 for t in tr if t["pnl"] > 0)
    total = mode_realized() + open_pnl
    L.append(f"📅 {'🟢' if total >= 0 else '🔴'} <b>{money(total)}</b> • {len(tr)}{'/' + str(max_trades_limit()) if max_trades_limit() else ''} trades ✅{wins} ❌{len(tr) - wins}")
    if state["day_halt"]:
        L.append("🛑 Daily limit hit: aaj trading band")
    elif DAY_LIMIT_ON:
        L.append(f"🛑 Limit -₹{DAY_LOSS_LIMIT:,.0f} / +₹{DAY_PROFIT_LIMIT:,.0f}")
    return "\n".join(L)


async def dashboard_any() -> str:
    return await (dashboard_compact_text() if state["dash_compact"] else dashboard_text())


async def start_dashboard(app: Application, chat_id: int):
    try:
        text = await asyncio.wait_for(dashboard_any(), timeout=15)
    except Exception as e:
        text = f"📺 Dashboard error: {html.escape(str(e))}"
    m = await app.bot.send_message(chat_id, text, reply_markup=dashboard_kb(), parse_mode="HTML")
    state["dash_msg_id"], state["dash_chat"], state["dash_t"] = m.message_id, chat_id, time.time()
    state["dash_last"] = text
    _dash["fails"] = 0
    save_state()


_dash = {"iv": float(DASH_REFRESH), "ok": 0, "fails": 0, "warn_t": 0.0}


def _retry_after_secs(e) -> float:
    ra = getattr(e, "retry_after", None)
    if ra is None:
        return 0.0
    return ra.total_seconds() if hasattr(ra, "total_seconds") else float(ra)


async def update_dashboard(app: Application, force=False):
    """Auto refresh. Kyarey band nathi thatu: flood control ma slow thay, message jatu rahe to navo moklay."""
    if not state["dash_msg_id"] or not state["dash_chat"]:
        return
    if not force and time.time() - state["dash_t"] < _dash["iv"]:
        return
    state["dash_t"] = time.time()
    try:
        text = await asyncio.wait_for(dashboard_any(), timeout=12)
    except asyncio.TimeoutError:
        _dash["fails"] += 1
        if time.time() - _dash["warn_t"] > 60:
            _dash["warn_t"] = time.time()
            log.warning("dashboard data 12s ma nathi aavyo (API atki?) - fari try karish")
        return
    except Exception as e:
        text = f"📺 Dashboard error: {html.escape(str(e)[:80])}\n(auto fari try karse)"
    if text == state["dash_last"] and not force:
        return                                       # kai badlyu nathi -> Telegram call nathi
    try:
        await app.bot.edit_message_text(text, chat_id=state["dash_chat"], message_id=state["dash_msg_id"],
                                        reply_markup=dashboard_kb(), parse_mode="HTML")
        state["dash_last"] = text
        _dash["fails"] = 0
        _dash["ok"] += 1
        if _dash["ok"] >= 60 and _dash["iv"] > DASH_REFRESH:      # flood pachhi dhire dhire pacho fast
            _dash["iv"] = max(float(DASH_REFRESH), _dash["iv"] - 1)
            _dash["ok"] = 0
    except Exception as e:
        msg = str(e).lower()
        if hasattr(e, "retry_after"):                              # Telegram flood control
            wait = _retry_after_secs(e)
            state["dash_t"] = time.time() + wait
            _dash["iv"] = min(8.0, _dash["iv"] + 1)                # thodu dhimu karo, pan band nahi
            _dash["ok"] = 0
        elif "not modified" in msg:
            state["dash_last"] = text
        elif "not found" in msg or "can't be edited" in msg or "message to edit" in msg:
            log.warning("dashboard message gayo (%s) -> navo moklu chhu", e)
            try:
                await start_dashboard(app, state["dash_chat"])
            except Exception as e2:
                log.warning("navo dashboard na moklayo: %s", e2)
        else:                                                      # timeout/network: chalu j rakho
            _dash["fails"] += 1
            if _dash["fails"] in (1, 10) or _dash["fails"] % 60 == 0:
                log.warning("dashboard update failed (%d): %s", _dash["fails"], e)


async def dash_loop(context: ContextTypes.DEFAULT_TYPE):
    """Alag job: exit/orders slow hoy to pan dashboard refresh na atke."""
    try:
        await asyncio.wait_for(update_dashboard(context.application), timeout=30)
    except Exception as e:
        log.warning("dash_loop error: %s", e)


async def cmd_dashboard(update: Update, ctx):
    if not authorized(update):
        return
    if ctx.args and ctx.args[0].lower() in ("compact", "full"):
        state["dash_compact"] = ctx.args[0].lower() == "compact"
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
    lim = day_limit_reason(mode_realized() + pnl)
    if lim:
        await close_position(app, lim)
        return await set_day_halt(app, lim)
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
    if time.time() - _last_save[0] > 10:
        save_state()


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


def build_trade_record(p: dict, s: float, h: float, pnl: float, reason: str, mode: str, exit_spot,
                       exit_orders=None, est=False, charges=None) -> dict:
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
    eo, xo = (p.get("orders") or {}), (exit_orders or {})
    oid = lambda d_, k: (d_.get(k) or {}).get("oid") or ""
    angel = mode == "live" and bool(eo)
    return {
        "charges": round(charges, 2) if charges is not None else "",
        "net_pnl": round(pnl - charges, 2) if charges is not None else "",
        "price_src": ("angel+est" if est else "angel") if angel else "ltp",
        "sell_entry_oid": oid(eo, "sell"), "hedge_entry_oid": oid(eo, "hedge"),
        "sell_exit_oid": oid(xo, "sell_exit"), "hedge_exit_oid": oid(xo, "hedge_exit"),
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
    src = str(r.get("price_src") or "")
    if src.startswith("angel"):
        ids = " ".join(f"{lab}#{r[k]}" for k, lab in (("hedge_entry_oid", "B"), ("sell_entry_oid", "S"),
                                                        ("sell_exit_oid", "S-exit"), ("hedge_exit_oid", "B-exit")) if r.get(k))
        L.append("📒 Angel fill prices" + (" (hedge exit andaje)" if src == "angel+est" else "") + (f" | {ids}" if ids else ""))
    L.append(f"💰 Net PnL: {money(pnl)}  [{r.get('reason', '')}]")
    ch_ = fnum(r, "charges")
    if ch_ is not None:
        L.append(f"💸 Charges (Angel est.): ₹{ch_:,.2f} → After charges {money(pnl - ch_)}")
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
    chs = [c for c in (fnum(r, "charges") for r in rows) if c is not None]
    if chs:
        L.append(f"💸 Charges (Angel est.): ₹{sum(chs):,.2f}  ({len(chs)} live trades)")
        L.append(f"💰 Net after charges: {money(net - sum(chs))}")

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
    load_custom_strategies()      # custom strategy + remove kareli built-in (load_state pehla)
    load_state()
    ensure_valid_strategy()
    angel_login()
    load_scrips()
    start_ws()
    threading.Thread(target=_price_poller, daemon=True).start()   # WS na chale to pan prices fast rahe
    app = (Application.builder().token(TELEGRAM_TOKEN).post_init(post_init)
           .concurrent_updates(True)                 # ek slow handler bija buttons/commands ne na atkave
           .connection_pool_size(32).pool_timeout(15).connect_timeout(15)
           .read_timeout(25).write_timeout(25)
           .build())
    app.add_error_handler(on_error)
    for name, fn in [("start", cmd_start), ("run", cmd_run), ("stop", cmd_stop), ("mode", cmd_mode),
                     ("status", cmd_status), ("pnl", cmd_pnl), ("live", cmd_live), ("exit", cmd_exit),
                     ("signal", cmd_signal), ("summary", cmd_summary), ("help", cmd_help),
                     ("dashboard", cmd_dashboard), ("history", cmd_history),
                     ("sl", cmd_sl), ("target", cmd_target), ("tp", cmd_target), ("time", cmd_time), ("trades", cmd_trades), ("export", cmd_export), ("expiry", cmd_expiry), ("daylimit", cmd_daylimit), ("testorder", cmd_testorder), ("orders", cmd_orders), ("strike", cmd_strike), ("storage", cmd_storage), ("keyboard", cmd_keyboard), ("sync", cmd_sync), ("maxtrades", cmd_maxtrades), ("sound", cmd_sound), ("chatid", cmd_chatid), ("breakout", cmd_breakout), ("newstrategy", cmd_newstrategy)]:
        app.add_handler(CommandHandler(name, fn))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_bottom_text))
    app.add_handler(CallbackQueryHandler(on_button))
    app.job_queue.run_repeating(tick, interval=LOOP_SECONDS, first=5)
    app.job_queue.run_repeating(fast_loop, interval=DASH_REFRESH, first=3)
    app.job_queue.run_repeating(dash_loop, interval=1, first=4)      # dashboard auto refresh (alag job)
    app.run_polling(drop_pending_updates=True)        # restart par juna (late) button/command na chale


if __name__ == "__main__":
    main()
