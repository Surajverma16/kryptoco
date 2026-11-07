import asyncio
import gc
import os
import time
import pandas as pd
from dotenv import load_dotenv

from core.blackboard import Blackboard
from core.exchange import ExchangeClient

load_dotenv()

MAINSTREAM_COINS = [
    "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
    "BNB/USDT:USDT", "XRP/USDT:USDT", "ADA/USDT:USDT",
    "AVAX/USDT:USDT", "LINK/USDT:USDT", "DOT/USDT:USDT", "SUI/USDT:USDT"
]

# =====================================================================
# LIVE WALLET SYNC WORKER (Fetches Bitget Balance & Compounds 60/40)
# =====================================================================
async def wallet_sync_worker(bb: Blackboard, client: ExchangeClient):
    while True:
        try:
            ui = await bb.get_ui_state()
            market_mode = ui.get("market_mode", "FUTURES")
            live_usdt = await client.fetch_live_wallet_equity_usdt(market_mode)
            await bb.sync_wallet_balance(live_usdt, client.usdt_inr_rate)
        except Exception as e:
            await bb.log_event("WALLET_SYNC", "WARN", str(e)[:60])
        await asyncio.sleep(5)

# =====================================================================
# BOT 1: MULTI-TIMEFRAME FULL-MARKET SCANNER (15m + 1H Confluence)
# =====================================================================
async def bot1_full_market_watchdog(bb: Blackboard, client: ExchangeClient):
    bot_name = "BOT_1_WATCHDOG"
    BLOCKED_BASES = {"AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "CL", "XAU", "XAG", "SKHY", "SNDK", "KORU"}

    while True:
        try:
            ui = await bb.get_ui_state()
            if ui.get("is_running", "True") != "True":
                await bb.heartbeat(bot_name, "PAUSED", "Paused by User in UI")
                await asyncio.sleep(3)
                continue

            market_mode = ui.get("market_mode", "FUTURES")
            await bb.heartbeat(bot_name, "SCANNING_ALL", "Fetching Bitget liquidity & tickers...")

            tickers = await client.exchange.fetch_tickers()
            penny_and_volatile_candidates = []

            for sym, t in tickers.items():
                if not sym.endswith("/USDT:USDT") and not sym.endswith("/USDT"):
                    continue
                base_coin = sym.split("/")[0]
                if not base_coin.isascii() or not base_coin.isalnum() or base_coin in BLOCKED_BASES:
                    continue
                if market_mode == "FUTURES" and not sym.endswith(":USDT"):
                    continue
                if market_mode == "SPOT" and sym.endswith(":USDT"):
                    continue

                last_p = float(t.get("last") or 0.0)
                chg_24h = float(t.get("percentage") or 0.0)
                quote_vol = float(t.get("quoteVolume") or 0.0)

                # Factor 1: Strict Liquidity Filter ($1.5M+ 24h volume to avoid spread traps)
                if last_p <= 0 or quote_vol < 1500000:
                    continue

                if sym not in MAINSTREAM_COINS and (last_p < 1.50 or abs(chg_24h) >= 6.0):
                    penny_and_volatile_candidates.append((sym, last_p, chg_24h, quote_vol))

            penny_and_volatile_candidates.sort(key=lambda x: abs(x[2]), reverse=True)
            top_high_risk = penny_and_volatile_candidates[:15]

            scan_list = []
            for m_sym in MAINSTREAM_COINS:
                clean_sym = m_sym if market_mode == "FUTURES" else m_sym.replace(":USDT", "")
                t_info = tickers.get(clean_sym, {})
                scan_list.append((
                    clean_sym, "LOW_RISK",
                    float(t_info.get("last") or 0.0),
                    float(t_info.get("percentage") or 0.0),
                    float(t_info.get("quoteVolume") or 0.0)
                ))

            for sym, lp, chg, qv in top_high_risk:
                scan_list.append((sym, "HIGH_RISK", lp, chg, qv))

            await bb.heartbeat(bot_name, "DEEP_SCAN", f"Auditing {len(scan_list)} liquid pairs (15m+1H)")

            for sym, category, lp, chg, qv in scan_list:
                try:
                    # Fetch 60 candles of 15m data (15 hours of structure) instead of noisy 5m
                    ohlcv_15m = await client.exchange.fetch_ohlcv(sym, timeframe="15m", limit=60)
                    if not ohlcv_15m or len(ohlcv_15m) < 52:
                        continue
                    last_close = float(ohlcv_15m[-1][4])
                    await bb.upsert_signal(
                        sym, category=category, market_type=market_mode,
                        price=last_close, change_24h=round(chg, 2), vol_24h_usd=round(qv, 0)
                    )
                    await bb.anomaly_queue.put((sym, category, market_mode, ohlcv_15m, chg))
                    await asyncio.sleep(0.2)
                except Exception:
                    continue

            gc.collect()
            await bb.heartbeat(bot_name, "IDLE", f"Tracking {len(scan_list)} pairs | Next scan 15s")
            await asyncio.sleep(15)
        except Exception as e:
            await bb.heartbeat(bot_name, "ERROR", str(e)[:60])
            await asyncio.sleep(5)


# =====================================================================
# BOT 2 & BOT 3: 6-FACTOR INSTITUTIONAL QUANT ENGINE
# (EMA 9/21/50 + VWAP + ADX Trend Strength + RVOL + Funding Rate)
# =====================================================================
async def bot2_and_3_quant_workers(bb: Blackboard, client: ExchangeClient):
    while True:
        sym, category, market_mode, ohlcv, chg_24h = await bb.anomaly_queue.get()
        try:
            await bb.heartbeat("BOT_2_CANDLE", "COMPUTING", sym)
            df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])

            # 1. Multi-EMA Trend Stack (EMA 9, EMA 21, Macro EMA 50)
            df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
            df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()
            df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()

            # 2. Institutional VWAP (Rolling 20-period Volume Weighted Average Price)
            typical_price = (df["high"] + df["low"] + df["close"]) / 3.0
            df["vwap"] = (typical_price * df["vol"]).rolling(20).sum() / df["vol"].rolling(20).sum().replace(0, 1e-9)

            # 3. ATR (14) & ADX-style Directional Trend Efficiency
            df["tr"] = pd.concat([
                df["high"] - df["low"],
                (df["high"] - df["close"].shift()).abs(),
                (df["low"] - df["close"].shift()).abs()
            ], axis=1).max(axis=1)
            atr = float(df["tr"].rolling(14).mean().iloc[-1])

            # Efficiency Ratio (Net 10-bar move divided by sum of 10-bar ranges: > 0.35 = Clean Trend, < 0.25 = Choppy Noise)
            net_move = abs(float(df["close"].iloc[-1]) - float(df["close"].iloc[-11]))
            sum_ranges = float(df["tr"].iloc[-10:].sum())
            trend_efficiency = net_move / max(sum_ranges, 1e-9)

            # 4. RSI (14)
            delta = df["close"].diff()
            gain = delta.clip(lower=0).rolling(14).mean()
            loss = (-delta.clip(upper=0)).rolling(14).mean()
            rs = gain / loss.replace(0, 1e-9)
            rsi = float((100 - (100 / (1 + rs))).iloc[-1])

            # 5. Relative Volume (RVOL) & Candle Body Conviction
            close = float(df["close"].iloc[-1])
            open_px = float(df["open"].iloc[-1])
            ema9 = float(df["ema9"].iloc[-1])
            ema21 = float(df["ema21"].iloc[-1])
            ema50 = float(df["ema50"].iloc[-1])
            vwap = float(df["vwap"].iloc[-1])
            rvol = float(df["vol"].iloc[-1]) / max(float(df["vol"].iloc[-21:-1].mean()), 1e-9)
            bull_candle = close > open_px
            bear_candle = close < open_px
            del df

            # 6. Derivatives Funding Rate Check (Avoid buying overheated tops or shorting squeezes)
            funding_pct = 0.0
            if market_mode == "FUTURES":
                try:
                    f_info = await client.exchange.fetch_funding_rate(sym)
                    funding_pct = float(f_info.get("fundingRate") or 0.0) * 100
                except Exception:
                    pass

            # --- STRICT CONFLUENCE SCORING (-100 to +100) ---
            tech_score = 0
            flow_state = "CHOP_NO_TRADE"

            # Filter out choppy sideways markets first!
            if trend_efficiency >= 0.32:
                # BULLISH CONFLUENCE: Price > EMA9 > EMA21 > EMA50, above VWAP, bullish candle, RSI 48-68, RVOL >= 1.15
                if (close > ema9 > ema21 > ema50) and (close > vwap) and bull_candle and (48 <= rsi <= 68):
                    if funding_pct <= 0.05:  # Not over-leveraged long
                        tech_score = 85 if rvol >= 1.25 else 70
                        flow_state = "INSTITUTIONAL_BULL_BREAKOUT" if rvol >= 1.25 else "CLEAN_UPTREND"

                # BEARISH CONFLUENCE: Price < EMA9 < EMA21 < EMA50, below VWAP, bearish candle, RSI 32-52, RVOL >= 1.15
                elif (close < ema9 < ema21 < ema50) and (close < vwap) and bear_candle and (32 <= rsi <= 52):
                    if funding_pct >= -0.04:  # Not walking into a negative-funding short squeeze
                        tech_score = -85 if rvol >= 1.25 else -70
                        flow_state = "INSTITUTIONAL_BEAR_BREAKDOWN" if rvol >= 1.25 else "CLEAN_DOWNTREND"

            await bb.heartbeat("BOT_3_FLOW_OI", "ANALYZING", f"{sym} {flow_state}")
            await bb.upsert_signal(
                sym, price=close, atr=round(atr, 6), rsi=round(rsi, 1),
                tech_score=tech_score, flow_state=flow_state, funding_rate=round(funding_pct, 4)
            )

            # Only send high-conviction setups (Score >= 70 or <= -70) to CIO!
            if abs(tech_score) >= 70:
                await bb.cio_review_queue.put((sym, category, market_mode))

            await bb.heartbeat("BOT_2_CANDLE", "IDLE", f"{sym} Score:{tech_score}")
            await bb.heartbeat("BOT_3_FLOW_OI", "IDLE", flow_state)
        except Exception as e:
            await bb.log_event("QUANT_WORKERS", "ERROR", f"{sym}: {str(e)[:50]}")
        finally:
            bb.anomaly_queue.task_done()


# =====================================================================
# BOT 4 & BOT 5: ADAPTIVE MEMORY + CORRELATION GUARD + CIO BRAIN
# =====================================================================
async def bot4_and_5_executive_ai(bb: Blackboard):
    while True:
        sym, category, market_mode = await bb.cio_review_queue.get()
        try:
            ui = await bb.get_ui_state()
            if ui.get("is_running", "True") != "True" or ui.get("finish_current_only", "False") == "True":
                continue

            if category == "HIGH_RISK" and ui.get("auto_high_risk", "True") != "True":
                continue

            open_positions = await bb.get_active_positions()
            if any(p["symbol"] == sym for p in open_positions):
                continue

            sig = await bb.get_signal(sym)
            score = sig.get("tech_score", 0)
            if abs(score) < 70:
                continue

            side = "buy" if score > 0 else "sell"
            if market_mode == "SPOT" and side == "sell":
                continue

            # 1. PORTFOLIO CORRELATION GUARD: Max 2 open positions in the same direction per tier!
            same_tier_same_side = [
                p for p in open_positions
                if p["risk_tier"] == category and p["side"] == side
            ]
            if len(same_tier_same_side) >= 2:
                await bb.log_event("BOT_5_CIO", "CORRELATION_VETO", f"Blocked {side.upper()} {sym}: Already holding 2 {side.upper()} trades in {category}.")
                continue

            # 2. SELF-LEARNING MEMORY CHECK: Check last 2 closed trades on this symbol in SQLite
            async with bb.conn.execute(
                "SELECT pnl_inr FROM live_transactions WHERE symbol = ? AND tx_type != 'OPEN' ORDER BY id DESC LIMIT 2",
                (sym,)
            ) as cur:
                recent_closed = await cur.fetchall()
                if len(recent_closed) == 2 and all(float(r["pnl_inr"]) < 0 for r in recent_closed):
                    await bb.log_event("BOT_5_CIO", "MEMORY_VETO", f"Adaptive Memory blocked {sym}: Lost 2 consecutive trades on this coin.")
                    continue

            await bb.heartbeat("BOT_4_NEWS", "SAFE", sym)
            await bb.heartbeat("BOT_5_CIO", "PROPOSING", f"{side.upper()} {sym} (Score:{score})")

            proposal = {
                "symbol": sym,
                "risk_tier": category,
                "market_type": market_mode,
                "side": side,
                "entry_price": sig["price"],
                "atr": sig["atr"],
                "confidence": 0.90 if abs(score) >= 85 else 0.78,
                "leverage": 4 if (category == "HIGH_RISK" and market_mode == "FUTURES") else (3 if market_mode == "FUTURES" else 1)
            }
            await bb.risk_queue.put(proposal)
        finally:
            bb.cio_review_queue.task_done()
                
# =====================================================================
# BOT 6: THE CRO (DYNAMIC 60/40 COMPOUNDING + KELLY SIZING + BITGET LOTS)
# =====================================================================
async def bot6_cro_risk(bb: Blackboard, client: ExchangeClient):
    while True:
        proposal = await bb.risk_queue.get()
        try:
            sym = proposal["symbol"]
            tier = proposal["risk_tier"]
            market_mode = proposal["market_type"]
            await bb.heartbeat("BOT_6_CRO_RISK", "AUDITING", f"{sym} ({tier})")

            vault = await bb.get_risk_state()
            ui = await bb.get_ui_state()
            open_tier_pos = await bb.get_active_positions(risk_tier=tier)

            usdt_inr_rate = float(vault.get("usdt_inr_rate") or client.usdt_inr_rate)
            min_exchange_notional_inr = 5.5 * usdt_inr_rate  # Bitget ~$5.50 USDT minimum order value
            kelly_frac = float(ui.get("kelly_fraction", "0.16"))
            kelly_frac = min(max(kelly_frac, 0.05), 0.35)

            # Dynamic 60% Low-Risk Pool vs 40% High-Risk Pool (Zero static numbers!)
            if tier == "LOW_RISK":
                if len(open_tier_pos) >= 4:
                    continue
                pool_eq = float(vault["low_risk_equity_inr"])
                avail_inr = max(0.0, pool_eq - float(vault["low_risk_margin_used_inr"]))
                target_margin_inr = pool_eq * kelly_frac
                margin_inr = min(target_margin_inr, avail_inr)
            else:
                if len(open_tier_pos) >= 4 and not proposal.get("is_manual"):
                    continue
                pool_eq = float(vault["high_risk_equity_inr"])
                avail_inr = max(0.0, pool_eq - float(vault["high_risk_margin_used_inr"]))
                target_margin_inr = float(proposal.get("requested_margin_inr") or (pool_eq * kelly_frac))
                margin_inr = min(target_margin_inr, avail_inr)

            if margin_inr <= 0 or avail_inr < (pool_eq * 0.05):
                await bb.log_event("BOT_6_CRO_RISK", "VETO", f"{tier} pool fully deployed (Free: ₹{avail_inr:,.2f})")
                continue

            leverage = int(proposal.get("leverage", 3 if market_mode == "FUTURES" else 1))
            if market_mode == "SPOT":
                leverage = 1

            notional_inr = round(margin_inr * leverage, 2)

            # Ensure order clears Bitget's $5.50 USDT minimum notional requirement
            if market_mode == "FUTURES" and notional_inr < min_exchange_notional_inr:
                needed_lev = int((min_exchange_notional_inr / margin_inr) + 1)
                if needed_lev <= 15:
                    leverage = needed_lev
                    notional_inr = round(margin_inr * leverage, 2)
                else:
                    await bb.log_event("BOT_6_CRO_RISK", "VETO", f"Notional ₹{notional_inr:.0f} below Bitget min ₹{min_exchange_notional_inr:.0f}")
                    continue

            # Convert INR Notional -> USDT Notional -> Coin Quantity
            notional_usdt = notional_inr / usdt_inr_rate
            entry_price = float(proposal["entry_price"])
            if entry_price <= 0:
                continue
            raw_qty = notional_usdt / entry_price

            try:
                if not client.exchange.markets:
                    await client.exchange.load_markets()
                qty = float(client.exchange.amount_to_precision(sym, raw_qty))
                entry_price = float(client.exchange.price_to_precision(sym, entry_price))
            except Exception:
                qty = round(raw_qty, 4)

            if qty <= 0:
                await bb.log_event("BOT_6_CRO_RISK", "VETO", f"{sym} qty rounds to 0 on Bitget lot size")
                continue

            atr = max(float(proposal.get("atr") or 0.0), entry_price * 0.006)
            stop_dist = atr * (2.2 if tier == "HIGH_RISK" else 1.8)
            tp_dist = atr * (4.0 if tier == "HIGH_RISK" else 3.0)

            if proposal["side"] == "buy":
                stop_price = entry_price - stop_dist
                tp_price = entry_price + tp_dist
            else:
                stop_price = entry_price + stop_dist
                tp_price = entry_price - tp_dist

            try:
                stop_price = float(client.exchange.price_to_precision(sym, max(0.000001, stop_price)))
                tp_price = float(client.exchange.price_to_precision(sym, max(0.000001, tp_price)))
            except Exception:
                stop_price = round(max(0.000001, stop_price), 6)
                tp_price = round(max(0.000001, tp_price), 6)

            approved_order = {
                "symbol": sym,
                "risk_tier": tier,
                "market_type": market_mode,
                "side": proposal["side"],
                "leverage": leverage,
                "margin_inr": round(margin_inr, 2),
                "notional_inr": notional_inr,
                "entry_price": entry_price,
                "qty": qty,
                "stop_price": stop_price,
                "take_profit_price": tp_price,
                "atr": round(atr, 6)
            }
            await bb.log_event(
                "BOT_6_CRO_RISK", "APPROVED",
                f"[{tier}] {sym} {proposal['side'].upper()} | Qty: {qty} | Margin: ₹{margin_inr:,.2f} ({leverage}x = ₹{notional_inr:,.2f})"
            )
            await bb.execution_queue.put(approved_order)
            await bb.heartbeat("BOT_6_CRO_RISK", "IDLE", f"Approved ₹{margin_inr:,.0f} on {sym}")
        finally:
            bb.risk_queue.task_done()

# =====================================================================
# BOT 7A: BITGET V2 EXECUTION ENGINE (SOLVES 25236 & SL/TP ERRORS)
# =====================================================================
async def place_bitget_futures_order(
    client: ExchangeClient,
    sym: str,
    side: str,
    qty: float,
    sl: float = 0.0,
    tp: float = 0.0,
    is_close: bool = False,
    **kwargs
):
    """
    Accepts sl/tp safely without TypeError, forces Bitget Position Mode alignment,
    and automatically tries One-Way Mode first, then Hedge Mode fallback.
    """
    fut_sym = sym if sym.endswith(":USDT") else f"{sym}:USDT"

    # 1. Try locking Bitget account to One-Way Mode (hedged=False) for this symbol
    try:
        await client.exchange.set_position_mode(False, fut_sym)
    except Exception:
        pass

    # 2. Candidate payloads: Attempt 1 is strict One-Way Mode; Attempt 2 is Hedge Mode
    if not is_close:
        candidates = [
            (side, {"oneWayMode": True, "marginMode": "cross"}),
            (side, {"oneWayMode": True}),
            (side, {"oneWayMode": False, "tradeSide": "open", "marginMode": "cross"}),
            (side, {"oneWayMode": False, "tradeSide": "open"}),
        ]
    else:
        # In Hedge Mode on Bitget, closing a LONG ('buy') requires side='buy' + tradeSide='close'
        hedge_close_side = "buy" if side == "sell" else "sell"
        candidates = [
            (side, {"oneWayMode": True, "reduceOnly": True}),
            (hedge_close_side, {"oneWayMode": False, "tradeSide": "close"}),
        ]

    last_err = None
    for order_side, params in candidates:
        try:
            return await client.exchange.create_order(
                symbol=fut_sym,
                type="market",
                side=order_side,
                amount=qty,
                params=params
            )
        except Exception as e:
            last_err = e
            continue

    raise last_err


async def bot7_entry_sniper(bb: Blackboard, client: ExchangeClient):
    while True:
        try:
            manual_orders = await bb.fetch_pending_manual_orders()
            for m_ord in manual_orders:
                sig = await bb.get_signal(m_ord["symbol"])
                live_px = float(sig.get("price") or 0.0)
                if live_px <= 0:
                    t = await client.exchange.fetch_ticker(m_ord["symbol"])
                    live_px = float(t["last"])
                atr = float(sig.get("atr") or (live_px * 0.01))
                await bb.risk_queue.put({
                    "symbol": m_ord["symbol"],
                    "risk_tier": m_ord["risk_tier"],
                    "market_type": m_ord["market_type"],
                    "side": m_ord["side"],
                    "entry_price": live_px,
                    "atr": atr,
                    "requested_margin_inr": m_ord["margin_inr"],
                    "leverage": m_ord["leverage"],
                    "is_manual": True
                })

            while not bb.execution_queue.empty():
                order = await bb.execution_queue.get()
                sym = order["symbol"]
                try:
                    if client.mode in ("LIVE", "DEMO"):
                        if order["market_type"] == "FUTURES":
                            fut_sym = sym if sym.endswith(":USDT") else f"{sym}:USDT"
                            try:
                                await client.exchange.set_leverage(order["leverage"], fut_sym)
                            except Exception:
                                pass

                            await place_bitget_futures_order(
                                client=client,
                                sym=fut_sym,
                                side=order["side"],
                                qty=order["qty"],
                                sl=order.get("stop_price", 0.0),
                                tp=order.get("take_profit_price", 0.0),
                                is_close=False
                            )
                        else:
                            spot_sym = sym.replace(":USDT", "")
                            await client.exchange.create_order(
                                symbol=spot_sym,
                                type="market",
                                side=order["side"],
                                amount=order["qty"],
                                price=order["entry_price"]
                            )

                    await bb.open_position(order)
                    await bb.log_event(
                        "BOT_7_EXECUTION", f"BITGET_{client.mode}_FILL",
                        f"Opened {order['side'].upper()} {sym} (Qty: {order['qty']}) @ {order['entry_price']} | Margin: ₹{order['margin_inr']:,.2f}"
                    )
                    await bb.heartbeat("BOT_7_EXECUTION", "FILLED", f"{sym} ₹{order['margin_inr']:,.0f}")
                except Exception as ex_err:
                    await bb.log_event("BOT_7_EXECUTION", "BITGET_REJECT", f"{sym}: {str(ex_err)[:100]}")
                finally:
                    bb.execution_queue.task_done()

            await asyncio.sleep(1)
        except Exception as e:
            await bb.log_event("BOT_7_EXECUTION", "ERROR", str(e)[:60])
            await asyncio.sleep(2)


# =====================================================================
# BOT 7B: BITGET EXIT WARDEN (REAL EXCHANGE CLOSES + TRAILING STOPS)
# =====================================================================
async def bot7_exit_warden(bb: Blackboard, client: ExchangeClient):
    FEE_RATE = 0.0012  # Bitget 0.06% per side
    while True:
        try:
            positions = await bb.get_active_positions()
            if not positions:
                await asyncio.sleep(2)
                continue

            for pos in positions:
                sym = pos["symbol"]
                sig = await bb.get_signal(sym)
                mark_price = float(sig.get("price") or pos["current_price"])

                try:
                    t = await client.exchange.fetch_ticker(sym)
                    mark_price = float(t.get("last") or mark_price)
                    await bb.upsert_signal(sym, price=mark_price)
                except Exception:
                    pass

                entry = pos["entry_price"]
                notional_inr = pos["notional_inr"]
                margin_inr = pos["margin_inr"]
                atr = pos["atr"]
                stop_price = pos["stop_price"]
                tp_price = pos["take_profit_price"]
                trailing_active = pos["trailing_active"]

                price_diff_pct = (mark_price - entry) / entry if pos["side"] == "buy" else (entry - mark_price) / entry
                gross_pnl_inr = notional_inr * price_diff_pct
                fee_inr = notional_inr * FEE_RATE
                net_pnl_inr = round(gross_pnl_inr - fee_inr, 2)
                roe_pct = round((net_pnl_inr / margin_inr) * 100, 2) if margin_inr > 0 else 0.0

                if pos["side"] == "buy" and (mark_price - entry) >= atr:
                    new_sl = round(mark_price - (0.8 * atr), 6)
                    if new_sl > stop_price:
                        stop_price = new_sl
                        trailing_active = 1
                elif pos["side"] == "sell" and (entry - mark_price) >= atr:
                    new_sl = round(mark_price + (0.8 * atr), 6)
                    if new_sl < stop_price:
                        stop_price = new_sl
                        trailing_active = 1

                exit_reason = None
                if pos["side"] == "buy":
                    if mark_price <= stop_price:
                        exit_reason = "TRAIL_PROFIT" if trailing_active and net_pnl_inr > 0 else "STOP_LOSS"
                    elif mark_price >= tp_price:
                        exit_reason = "TAKE_PROFIT"
                else:
                    if mark_price >= stop_price:
                        exit_reason = "TRAIL_PROFIT" if trailing_active and net_pnl_inr > 0 else "STOP_LOSS"
                    elif mark_price <= tp_price:
                        exit_reason = "TAKE_PROFIT"

                if exit_reason:
                    if client.mode in ("LIVE", "DEMO"):
                        close_side = "sell" if pos["side"] == "buy" else "buy"
                        try:
                            if pos["market_type"] == "FUTURES":
                                await place_bitget_futures_order(
                                    client=client,
                                    sym=sym,
                                    side=close_side,
                                    qty=pos["qty"],
                                    is_close=True
                                )
                            else:
                                spot_sym = sym.replace(":USDT", "")
                                await client.exchange.create_order(
                                    symbol=spot_sym,
                                    type="market",
                                    side="sell",
                                    amount=pos["qty"]
                                )
                        except Exception as close_err:
                            await bb.log_event("BOT_7_WARDEN", "CLOSE_WARN", f"{sym}: {str(close_err)[:50]}")

                    await bb.close_position_and_settle(pos["id"], mark_price, net_pnl_inr, exit_reason)
                    await bb.log_event("BOT_7_WARDEN", exit_reason, f"Closed {sym} @ {mark_price} | Realized PnL: ₹{net_pnl_inr:+,.2f}")
                else:
                    await bb.update_position_mark(pos["id"], mark_price, stop_price, trailing_active, net_pnl_inr, roe_pct)

            await bb.heartbeat("BOT_7_EXECUTION", "GUARDING", f"{len(positions)} active Bitget trades")
            await asyncio.sleep(2)
        except Exception as e:
            await bb.log_event("BOT_7_WARDEN", "ERROR", str(e)[:60])
            await asyncio.sleep(3)

async def main():
    bb = Blackboard()
    await bb.connect()
    client = ExchangeClient()

    # Sync wallet balance immediately on startup before scanning
    init_usdt = await client.fetch_live_wallet_equity_usdt("FUTURES")
    await bb.sync_wallet_balance(init_usdt, client.usdt_inr_rate)
    await bb.log_event("SYSTEM", "WALLET_SYNC", f"Bitget {client.mode} Wallet Connected: {init_usdt:,.2f} USDT (₹{init_usdt * client.usdt_inr_rate:,.2f})")

    try:
        await asyncio.gather(
            wallet_sync_worker(bb, client),
            bot1_full_market_watchdog(bb, client),
            bot2_and_3_quant_workers(bb, client),
            bot4_and_5_executive_ai(bb),
            bot6_cro_risk(bb, client),
            bot7_entry_sniper(bb, client),
            bot7_exit_warden(bb, client),
        )
    finally:
        await client.close()
        await bb.close()

if __name__ == "__main__":
    try:
        import winloop
        winloop.install()
    except ImportError:
        pass
    asyncio.run(main())