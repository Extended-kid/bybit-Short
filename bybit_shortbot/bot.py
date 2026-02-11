#!/usr/bin/env python3
import os, json, time
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from pybit.unified_trading import HTTP


# =========================
# CONFIG
# =========================
@dataclass
class BotConfig:
    category: str = "linear"   # USDT perp
    tf: str = "15"             # 15m
    wake_seconds: int = 5
    only_on_15m_close: bool = True

    # pump filters (ticker)
    quote: str = "USDT"
    min_pump_from_low24_pct: float = 35.0
    near_high_ratio: float = 0.88
    min_turnover_usdt: float = 5_000_000.0

    # stall logic
    stall_candles: int = 2     # 2 свечи без обновления локального хая

    # watchlist TTL
    watch_ttl_seconds: int = 24 * 60 * 60  # удалить из watch через сутки

    # paper trade rules
    notional_usdt: float = 20.0
    tp_from_high_pct: float = 0.30         # TP = local_high*(1-0.30)
    sl_mult: float = 2.0                   # SL = entry*2

    # funding guard
    enable_funding_guard: bool = True
    funding_guard_ratio: float = 1.0       # exit if expected funding loss >= remaining profit to TP

    # persistence
    state_file: str = "state.json"
    trades_file: str = "trades.json"

    # avoid spam
    symbol_cooldown_minutes: int = 60


CFG = BotConfig()


# =========================
# UTIL
# =========================
def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def current_15m_close_ms() -> int:
    now = int(time.time())
    close = (now // (15 * 60)) * (15 * 60)
    return close * 1000


# =========================
# JSON HELPERS
# =========================
def load_json(path: str, default_obj: Any) -> Any:
    if not os.path.exists(path):
        return default_obj
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                return default_obj
            return json.loads(raw)
    except Exception:
        return default_obj

def save_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def default_state() -> Dict[str, Any]:
    return {
        "last_bar_close_ms": None,
        "cooldowns": {},   # symbol -> unix seconds
        "watch": {},       # symbol -> {local_high, stall, blocked, updated_ts, created_ts}
        "last_events": []  # small log
    }

def in_cooldown(state: Dict[str, Any], symbol: str, now_ts: int) -> bool:
    last = state.get("cooldowns", {}).get(symbol)
    if not last:
        return False
    return (now_ts - int(last)) < CFG.symbol_cooldown_minutes * 60

def set_cooldown(state: Dict[str, Any], symbol: str, now_ts: int) -> None:
    state.setdefault("cooldowns", {})[symbol] = int(now_ts)


# =========================
# BYBIT
# =========================
def make_auth_session() -> HTTP:
    key = os.getenv("BYBIT_API_KEY", "").strip()
    sec = os.getenv("BYBIT_API_SECRET", "").strip()
    if not key or not sec:
        raise RuntimeError("Set env vars BYBIT_API_KEY and BYBIT_API_SECRET")
    return HTTP(testnet=False, api_key=key, api_secret=sec)

def public_session() -> HTTP:
    return HTTP(testnet=False)

def get_all_tickers(sess: HTTP) -> List[Dict[str, Any]]:
    r = sess.get_tickers(category=CFG.category)
    if r.get("retCode") != 0:
        raise RuntimeError(f"tickers error: {r.get('retMsg')}")
    return r["result"]["list"]

def get_last_closed_kline(sess: HTTP, symbol: str) -> Optional[List[str]]:
    r = sess.get_kline(category=CFG.category, symbol=symbol, interval=CFG.tf, limit=5)
    if r.get("retCode") != 0:
        return None
    data = r["result"]["list"] or []
    if not data:
        return None
    data = list(reversed(data))  # oldest->newest
    return data[-1]


# =========================
# FILTERS
# =========================
def candidate_from_ticker(t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sym = t.get("symbol", "")
    if not sym.endswith(CFG.quote):
        return None

    last = safe_float(t.get("lastPrice"))
    high24 = safe_float(t.get("highPrice24h"))
    low24 = safe_float(t.get("lowPrice24h"))
    turnover = safe_float(t.get("turnover24h"))

    funding_rate = safe_float(t.get("fundingRate", 0.0))
    next_funding_ms = int(safe_float(t.get("nextFundingTime", 0), 0))

    if last <= 0 or high24 <= 0 or low24 <= 0:
        return None

    pump_from_low24 = (last - low24) / low24 * 100.0
    near_high = last / high24

    if turnover < CFG.min_turnover_usdt:
        return None
    if pump_from_low24 < CFG.min_pump_from_low24_pct:
        return None
    if near_high < CFG.near_high_ratio:
        return None

    return {
        "symbol": sym,
        "last": last,
        "high24": high24,
        "low24": low24,
        "turnover": turnover,
        "pump_from_low24_pct": pump_from_low24,
        "near_high": near_high,
        "funding_rate": funding_rate,
        "next_funding_ms": next_funding_ms,
    }


# =========================
# PAPER TRADES
# =========================
def make_trade(symbol: str, entry: float, local_high: float, now_ts: int, ticker: Dict[str, Any]) -> Dict[str, Any]:
    tp = local_high * (1.0 - CFG.tp_from_high_pct)  # TP from high
    sl = entry * CFG.sl_mult
    qty = CFG.notional_usdt / entry

    return {
        "id": f"{symbol}:{now_ts}",
        "symbol": symbol,
        "status": "OPEN",
        "created_ts": now_ts,
        "open_ts": now_ts,

        "side": "SHORT",
        "notional_usdt": CFG.notional_usdt,
        "qty": qty,

        "entry": entry,
        "tp": tp,
        "sl": sl,
        "local_high": local_high,

        "funding_rate_at_open": safe_float(ticker.get("funding_rate", 0.0)),
        "next_funding_ms_at_open": int(safe_float(ticker.get("next_funding_ms", 0), 0)),

        "close_ts": None,
        "close_reason": None,
        "close_price": None,
    }

def trade_pnl(tr: Dict[str, Any]) -> Optional[float]:
    if tr["status"] != "CLOSED" or tr["close_price"] is None:
        return None
    entry = float(tr["entry"])
    close = float(tr["close_price"])
    qty = float(tr["qty"])
    return (entry - close) * qty  # short

def remaining_profit_to_tp(tr: Dict[str, Any], mark: float) -> float:
    tp = float(tr["tp"])
    qty = float(tr["qty"])
    if mark <= tp:
        return 0.0
    return (mark - tp) * qty

def funding_pnl_short(notional: float, rate: float) -> float:
    # short funding pnl = notional * rate (rate<0 => short pays => negative)
    return notional * rate

def funding_guard(tr: Dict[str, Any], ticker: Dict[str, Any], mark: float) -> bool:
    if not CFG.enable_funding_guard:
        return False
    rate = safe_float(ticker.get("funding_rate", 0.0), 0.0)
    if rate >= 0:
        return False  # shorts receive or neutral

    rem = remaining_profit_to_tp(tr, mark)
    if rem <= 0:
        return False

    expected = funding_pnl_short(float(tr["notional_usdt"]), rate)  # negative if rate<0
    return abs(expected) >= rem * CFG.funding_guard_ratio

def update_open_trades(trades: List[Dict[str, Any]], tick_map: Dict[str, Dict[str, Any]], now_ts: int) -> None:
    pub = public_session()
    for tr in trades:
        if tr["status"] != "OPEN":
            continue
        sym = tr["symbol"]

        last_candle = get_last_closed_kline(pub, sym)
        if not last_candle:
            continue
        h = safe_float(last_candle[2])
        l = safe_float(last_candle[3])
        c = safe_float(last_candle[4])

        tp = float(tr["tp"])
        sl = float(tr["sl"])

        ticker = tick_map.get(sym, {"funding_rate": 0.0, "last": c})
        mark = safe_float(ticker.get("last", c), c)

        # funding early exit
        if funding_guard(tr, ticker, mark):
            tr["status"] = "CLOSED"
            tr["close_ts"] = now_ts
            tr["close_reason"] = "FUNDING_GUARD"
            tr["close_price"] = mark
            continue

        hit_tp = (l <= tp)
        hit_sl = (h >= sl)

        if hit_tp and hit_sl:
            # worst-case
            tr["status"] = "CLOSED"
            tr["close_ts"] = now_ts
            tr["close_reason"] = "SL"
            tr["close_price"] = sl
        elif hit_sl:
            tr["status"] = "CLOSED"
            tr["close_ts"] = now_ts
            tr["close_reason"] = "SL"
            tr["close_price"] = sl
        elif hit_tp:
            tr["status"] = "CLOSED"
            tr["close_ts"] = now_ts
            tr["close_reason"] = "TP"
            tr["close_price"] = tp

def print_stats(trades: List[Dict[str, Any]]) -> None:
    closed = [t for t in trades if t["status"] == "CLOSED"]
    if not closed:
        print("[STATS] closed=0")
        return
    pnls = [trade_pnl(t) for t in closed]
    pnls = [p for p in pnls if p is not None]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    winrate = (len(wins) / len(pnls)) * 100.0 if pnls else 0.0
    total = sum(pnls)
    avg = total / len(pnls) if pnls else 0.0
    pf = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else float("inf")
    print(f"[STATS] closed={len(pnls)} winrate={winrate:.1f}% totalPnL={total:.2f} avgPnL={avg:.2f} PF={pf:.2f}")


# =========================
# MAIN LOOP
# =========================
def main():
    # Just to ensure env vars exist (you can keep keys restricted: no withdraw)
    _ = make_auth_session()
    pub = public_session()

    state = load_json(CFG.state_file, default_state())
    trades = load_json(CFG.trades_file, [])

    print(f"Loaded trades={len(trades)} open={sum(1 for t in trades if t['status']=='OPEN')} watch={len(state.get('watch', {}))}")

    while True:
        try:
            now_ts = int(time.time())

            bar_close = current_15m_close_ms() if CFG.only_on_15m_close else int(time.time() * 1000)
            if CFG.only_on_15m_close and state.get("last_bar_close_ms") == bar_close:
                time.sleep(CFG.wake_seconds)
                continue
            state["last_bar_close_ms"] = bar_close

            tickers = get_all_tickers(pub)

            # Build ticker map + pump candidates
            tick_map: Dict[str, Dict[str, Any]] = {}
            candidates: List[Dict[str, Any]] = []

            for t in tickers:
                sym = t.get("symbol", "")
                last = safe_float(t.get("lastPrice"))
                tick_map[sym] = {
                    "symbol": sym,
                    "last": last,
                    "funding_rate": safe_float(t.get("fundingRate", 0.0)),
                    "next_funding_ms": int(safe_float(t.get("nextFundingTime", 0), 0)),
                }
                c = candidate_from_ticker(t)
                if c:
                    candidates.append(c)

            # 1) Update open trades
            update_open_trades(trades, tick_map, now_ts)
            save_json(CFG.trades_file, trades)

            watch = state.setdefault("watch", {})

            print(f"[SCAN] {utc_now_str()} cand={len(candidates)} watch={len(watch)} open={sum(1 for t in trades if t['status']=='OPEN')} closed={sum(1 for t in trades if t['status']=='CLOSED')}")
            print_stats(trades)

            # 2) Clean watchlist by TTL (24h since created_ts)
            removed_ttl = 0
            for sym in list(watch.keys()):
                w = watch[sym]
                created_ts = int(w.get("created_ts", w.get("updated_ts", now_ts)))
                if now_ts - created_ts >= CFG.watch_ttl_seconds:
                    watch.pop(sym, None)
                    removed_ttl += 1
            if removed_ttl:
                print(f"[WATCH] removed_by_ttl={removed_ttl}")

            # 3) Add new pump candidates to watchlist
            added = 0
            for c in candidates:
                sym = c["symbol"]
                if sym in watch:
                    continue
                if in_cooldown(state, sym, now_ts):
                    continue
                if any(t["symbol"] == sym and t["status"] == "OPEN" for t in trades):
                    continue

                last_candle = get_last_closed_kline(pub, sym)
                if not last_candle:
                    continue
                high = safe_float(last_candle[2])

                watch[sym] = {
                    "local_high": high,
                    "stall": 0,
                    "blocked": False,     # if price already below TP when stalled
                    "updated_ts": now_ts,
                    "created_ts": now_ts
                }
                added += 1

            # 4) Update watchlist stall logic; trigger market short when stalled
            triggered = 0
            skipped_below_tp = 0

            for sym in list(watch.keys()):
                w = watch.get(sym)
                if not w:
                    continue

                last_candle = get_last_closed_kline(pub, sym)
                if not last_candle:
                    continue

                high = safe_float(last_candle[2])
                close = safe_float(last_candle[4])

                local_high = float(w["local_high"])
                stall = int(w["stall"])
                blocked = bool(w.get("blocked", False))

                # New high resets stall and unblocks
                if high > local_high:
                    w["local_high"] = high
                    w["stall"] = 0
                    w["blocked"] = False
                else:
                    w["stall"] = stall + 1

                w["updated_ts"] = now_ts

                # Trigger condition
                if int(w["stall"]) >= CFG.stall_candles and not blocked:
                    tinfo = tick_map.get(sym, {"last": close, "funding_rate": 0.0, "next_funding_ms": 0})
                    entry = safe_float(tinfo.get("last", close), close)

                    tp_from_high = float(w["local_high"]) * (1.0 - CFG.tp_from_high_pct)

                    # Rule: do NOT open if market <= TP (already too late). Keep watching.
                    if entry <= tp_from_high:
                        w["blocked"] = True  # block until new high appears
                        skipped_below_tp += 1
                        state.setdefault("last_events", []).append({
                            "ts": now_ts,
                            "type": "SKIP_BELOW_TP",
                            "symbol": sym,
                            "entry": entry,
                            "tp": tp_from_high,
                            "local_high": float(w["local_high"])
                        })
                        state["last_events"] = state["last_events"][-500:]
                        print(f"[SKIP] {sym} entry={entry:.6f} <= TP(fromHigh)={tp_from_high:.6f} -> keep watching (blocked until new high)")
                        continue

                    # Open paper trade (market entry)
                    tr = make_trade(sym, entry=entry, local_high=float(w["local_high"]), now_ts=now_ts, ticker=tinfo)
                    trades.append(tr)
                    set_cooldown(state, sym, now_ts)
                    triggered += 1

                    state.setdefault("last_events", []).append({
                        "ts": now_ts,
                        "type": "TRIGGER",
                        "symbol": sym,
                        "entry": tr["entry"],
                        "tp": tr["tp"],
                        "sl": tr["sl"],
                        "local_high": tr["local_high"],
                        "funding_rate": tr["funding_rate_at_open"]
                    })
                    state["last_events"] = state["last_events"][-500:]

                    # Remove from watch after opening (you can re-add later on a new pump)
                    watch.pop(sym, None)

                    print(f"[PAPER] TRIGGER {sym} entry(MKT)={tr['entry']:.6f} TP(fromHigh)={tr['tp']:.6f} SL={tr['sl']:.6f} localHigh={tr['local_high']:.6f} funding={tr['funding_rate_at_open']:.6f}")

            # Save state & trades
            save_json(CFG.state_file, state)
            save_json(CFG.trades_file, trades)

            if added:
                print(f"[WATCH] added={added}")
            if skipped_below_tp:
                print(f"[WATCH] skipped_below_tp={skipped_below_tp}")
            if triggered == 0:
                print("[INFO] no triggers this bar")

            time.sleep(CFG.wake_seconds)

        except KeyboardInterrupt:
            print("\n[EXIT] KeyboardInterrupt")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(3)


if __name__ == "__main__":
    main()
