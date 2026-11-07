import streamlit as st
import sqlite3
import subprocess
import sys
import os
import time
import pandas as pd
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "company_state.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# --- AUTO-LAUNCH main.py IN BACKGROUND FOR CLOUD / LINUX HOSTING ---
@st.cache_resource
def ensure_bots_running():
    """Starts main.py once as a background worker inside the cloud container."""
    bot_proc = subprocess.Popen(
        [sys.executable, "main.py"],
        cwd=str(Path(__file__).parent),
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    return bot_proc

ensure_bots_running()

st.set_page_config(
    page_title="Kryptoco Dynamic AI Terminal",
    layout="wide",
    page_icon="⚡",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .vault-card {
        background: linear-gradient(135deg, #111827 0%, #1f2937 100%);
        border: 1px solid #374151;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .vault-title { color: #9ca3af; font-size: 0.82rem; font-weight: 600; text-transform: uppercase; }
    .vault-value { color: #f9fafb; font-size: 1.55rem; font-weight: 800; margin: 4px 0; }
    .vault-sub { color: #10b981; font-size: 0.80rem; }
</style>
""", unsafe_allow_html=True)

DB_PATH = Path(__file__).parent / "data" / "company_state.db"

def get_conn():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH, timeout=3.0)
    conn.row_factory = sqlite3.Row
    return conn

def set_ui_state(key: str, val: str):
    conn = get_conn()
    if conn:
        conn.execute("INSERT OR REPLACE INTO terminal_state (key, value) VALUES (?, ?)", (key, str(val)))
        conn.commit()
        conn.close()

def queue_manual_order(symbol: str, side: str, risk_tier: str, market_type: str, margin_inr: float, leverage: int):
    conn = get_conn()
    if conn:
        conn.execute("""
            INSERT INTO manual_orders (symbol, side, risk_tier, market_type, margin_inr, leverage, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?)
        """, (symbol, side, risk_tier, market_type, margin_inr, leverage, time.time()))
        conn.commit()
        conn.close()

def force_close_position(pos_id: int, current_price: float, net_pnl_inr: float):
    conn = get_conn()
    if conn:
        pos = conn.execute("SELECT * FROM active_positions WHERE id = ?", (pos_id,)).fetchone()
        if pos:
            pos = dict(pos)
            conn.execute("DELETE FROM active_positions WHERE id = ?", (pos_id,))
            conn.execute("""
                INSERT INTO live_transactions (timestamp, tx_type, risk_tier, market_type, symbol, side, price, margin_inr, leverage, pnl_inr, details)
                VALUES (?, 'MANUAL_CLOSE', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (time.time(), pos["risk_tier"], pos["market_type"], pos["symbol"], pos["side"].upper(), current_price, pos["margin_inr"], pos["leverage"], net_pnl_inr, f"Manual Close @ {current_price} | PnL: ₹{net_pnl_inr:+,.2f}"))
            conn.execute("""
                UPDATE risk_vault SET
                    total_live_equity_inr = total_live_equity_inr + ?,
                    low_risk_equity_inr = (total_live_equity_inr + ?) * 0.60,
                    high_risk_equity_inr = (total_live_equity_inr + ?) * 0.40,
                    realized_pnl_inr = realized_pnl_inr + ?,
                    total_trades = total_trades + 1
                WHERE id = 1
            """, (net_pnl_inr, net_pnl_inr, net_pnl_inr, net_pnl_inr))
            conn.commit()
        conn.close()

def clear_local_history():
    conn = get_conn()
    if conn:
        conn.execute("DELETE FROM active_positions")
        conn.execute("DELETE FROM live_transactions")
        conn.execute("DELETE FROM manual_orders")
        conn.execute("UPDATE risk_vault SET realized_pnl_inr = 0.0, total_trades = 0, winning_trades = 0, consecutive_losses = 0 WHERE id = 1")
        conn.commit()
        conn.close()

conn = get_conn()
if not conn:
    st.warning("⏳ Waiting for `main.py` to initialize `data/company_state.db` and sync your Bitget wallet...")
    time.sleep(2)
    st.rerun()

try:
    vault = dict(conn.execute("SELECT * FROM risk_vault WHERE id = 1").fetchone() or {})
    ui_state = {r["key"]: r["value"] for r in conn.execute("SELECT key, value FROM terminal_state").fetchall()}
    positions = [dict(r) for r in conn.execute("SELECT * FROM active_positions ORDER BY opened_at DESC").fetchall()]
    signals = [dict(r) for r in conn.execute("SELECT * FROM market_signals ORDER BY updated_at DESC").fetchall()]
    txs = [dict(r) for r in conn.execute("SELECT * FROM live_transactions ORDER BY id DESC LIMIT 30").fetchall()]
    bots = [dict(r) for r in conn.execute("SELECT * FROM bot_heartbeat ORDER BY bot_name").fetchall()]
    logs = [dict(r) for r in conn.execute("SELECT * FROM boardroom_log ORDER BY id DESC LIMIT 20").fetchall()]
finally:
    conn.close()

# --- EXTRACT DYNAMIC WALLET & 60/40 COMPOUNDING VALUES FROM DATABASE ---
wallet_usdt = float(vault.get("wallet_balance_usdt") or 0.0)
usdt_rate = float(vault.get("usdt_inr_rate") or 87.0)
low_eq = float(vault.get("low_risk_equity_inr") or 0.0)
high_eq = float(vault.get("high_risk_equity_inr") or 0.0)
low_used = sum(float(p["margin_inr"]) for p in positions if p["risk_tier"] == "LOW_RISK")
high_used = sum(float(p["margin_inr"]) for p in positions if p["risk_tier"] == "HIGH_RISK")
low_free = max(0.0, low_eq - low_used)
high_free = max(0.0, high_eq - high_used)
total_u_pnl = sum(float(p["unrealized_pnl_inr"]) for p in positions)
realized_pnl = float(vault.get("realized_pnl_inr") or 0.0)
total_live_eq = low_eq + high_eq + total_u_pnl

# --- SIDEBAR CONTROLS & DYNAMIC KELLY FORMULA ---
st.sidebar.title("🎛️ Kryptoco Live Control")

is_running = st.sidebar.toggle("🟢 Master Bot Running", value=(ui_state.get("is_running", "True") == "True"))
set_ui_state("is_running", str(is_running))

finish_current = st.sidebar.toggle("🛑 Finish Current & Stop New", value=(ui_state.get("finish_current_only", "False") == "True"))
set_ui_state("finish_current_only", str(finish_current))

auto_high_risk = st.sidebar.toggle("🤖 Auto-Trade High-Risk Memes", value=(ui_state.get("auto_high_risk", "True") == "True"))
set_ui_state("auto_high_risk", str(auto_high_risk))

st.sidebar.markdown("---")
st.sidebar.subheader("📐 Dynamic Kelly Compounding")
w_rate = st.sidebar.slider("Win Probability (W)", 0.35, 0.85, 0.55, 0.01)
r_ratio = st.sidebar.slider("Reward-to-Risk (R)", 1.0, 4.0, 1.8, 0.1)
kelly_pct = max(0.05, (w_rate - ((1.0 - w_rate) / r_ratio)) * 0.5)
set_ui_state("kelly_fraction", f"{kelly_pct:.4f}")

low_kelly_bet = low_eq * kelly_pct
high_kelly_bet = high_eq * kelly_pct
st.sidebar.info(
    f"**Half-Kelly Fraction:** `{kelly_pct*100:.1f}%` of Pool\n\n"
    f"• Low-Risk Trade Size: **₹{low_kelly_bet:,.2f}**\n"
    f"• High-Risk Trade Size: **₹{high_kelly_bet:,.2f}**\n"
    f"*(Automatically compounds as equity grows)*"
)

st.sidebar.markdown("---")
if st.sidebar.button("🧹 Clear Local Trade History & Re-Sync Wallet", use_container_width=True):
    clear_local_history()
    st.rerun()

# --- TOP NAVIGATION BAR ---
nav_col1, nav_col2 = st.columns([1, 2.2])
with nav_col1:
    market_mode_sel = st.radio(
        "1️⃣ Select Exchange Market",
        ["🚀 FUTURES (Perpetual)", "💰 SPOT Market"],
        horizontal=True,
        index=0 if ui_state.get("market_mode", "FUTURES") == "FUTURES" else 1
    )
    active_market = "FUTURES" if "FUTURES" in market_mode_sel else "SPOT"
    set_ui_state("market_mode", active_market)

with nav_col2:
    active_view = st.radio(
        "2️⃣ Navigation Bar — Switch Trading Desk",
        [
            f"🛡️ Low-Risk Desk (60% = ₹{low_eq:,.0f})",
            f"🔥 High-Risk Sniper (40% = ₹{high_eq:,.0f})",
            "⚡ Live Transactions & Boardroom Logs"
        ],
        horizontal=True
    )

# --- TOP 4 DYNAMIC METRIC CARDS ---
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="vault-card">
        <div class="vault-title">🏦 Live Bitget Wallet Equity</div>
        <div class="vault-value">₹{total_live_eq:,.2f}</div>
        <div class="vault-sub">{wallet_usdt:,.2f} USDT (@ ₹{usdt_rate:.0f}) | Realized: ₹{realized_pnl:+,.2f}</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="vault-card">
        <div class="vault-title">🛡️ Low-Risk Pool (60% Dynamic)</div>
        <div class="vault-value">₹{low_eq:,.2f}</div>
        <div class="vault-sub">Locked: ₹{low_used:,.0f} | Free Cash: ₹{low_free:,.0f}</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="vault-card">
        <div class="vault-title">🔥 High-Risk Pool (40% Dynamic)</div>
        <div class="vault-value">₹{high_eq:,.2f}</div>
        <div class="vault-sub">Locked: ₹{high_used:,.0f} | Free Cash: ₹{high_free:,.0f}</div>
    </div>""", unsafe_allow_html=True)
with c4:
    trades_cnt = vault.get("total_trades", 0)
    wins_cnt = vault.get("winning_trades", 0)
    wr = (wins_cnt / trades_cnt * 100) if trades_cnt > 0 else 0.0
    st.markdown(f"""<div class="vault-card">
        <div class="vault-title">📊 Performance ({active_market})</div>
        <div class="vault-value">{wr:.1f}% Win Rate</div>
        <div class="vault-sub">Closed: {trades_cnt} ({wins_cnt}W) | Active Open: {len(positions)}</div>
    </div>""", unsafe_allow_html=True)

# =====================================================================
# VIEW 1: LOW-RISK AUTONOMOUS DESK (60% OF LIVE WALLET)
# =====================================================================
if "Low-Risk" in active_view:
    st.subheader(f"🛡️ Autonomous Low-Risk Desk — [{active_market}] (60% Pool: ₹{low_eq:,.2f})")
    st.caption(f"Bots automatically compound 60% of your live Bitget wallet, sizing each trade at {kelly_pct*100:.1f}% (₹{low_kelly_bet:,.2f} margin).")

    low_positions = [p for p in positions if p["risk_tier"] == "LOW_RISK"]
    if low_positions:
        for p in low_positions:
            pcol1, pcol2, pcol3, pcol4, pcol5, pcol6 = st.columns([2, 1.8, 2, 2, 1.5, 1])
            pcol1.markdown(f"**{p['symbol']}** (`{p['side'].upper()}` {p['leverage']}x)")
            pcol2.markdown(f"Margin: **₹{p['margin_inr']:,.0f}**\n*(Notional: ₹{p['notional_inr']:,.0f})*")
            pcol3.markdown(f"Entry: `{p['entry_price']}` → Live: **`{p['current_price']}`**")
            pcol4.markdown(f"SL: `{p['stop_price']}` | TP: `{p['take_profit_price']}`")
            pnl_color = "🟢" if p["unrealized_pnl_inr"] >= 0 else "🔴"
            pcol5.markdown(f"{pnl_color} **₹{p['unrealized_pnl_inr']:+,.2f}** (`{p['pnl_pct']:+.2f}%`)")
            if pcol6.button("Close", key=f"close_low_{p['id']}"):
                force_close_position(p["id"], p["current_price"], p["unrealized_pnl_inr"])
                st.rerun()
    else:
        st.info("⏳ No open Low-Risk positions right now. Bots are scanning for the next high-probability entry...")

    st.markdown("#### 📡 Live Mainstream Radar")
    low_sigs = [s for s in signals if s["category"] == "LOW_RISK"]
    if low_sigs:
        st.dataframe(pd.DataFrame(low_sigs)[["symbol", "price", "change_24h", "rsi", "tech_score", "flow_state"]], use_container_width=True, hide_index=True)

# =====================================================================
# VIEW 2: HIGH-RISK PENNY & MEME SNIPER DESK (40% OF LIVE WALLET)
# =====================================================================
elif "High-Risk" in active_view:
    st.subheader(f"🔥 High-Risk Penny, Meme & Volatile Sniper — [{active_market}] (40% Pool: ₹{high_eq:,.2f})")
    st.caption(f"Dedicated 40% high-beta pool (Free Cash: ₹{high_free:,.2f}). Execute trades **instantly** below or let the Auto-Sniper compound your gains!")

    high_sigs = [s for s in signals if s["category"] == "HIGH_RISK"]
    all_available_syms = [s["symbol"] for s in high_sigs] if high_sigs else ([s["symbol"] for s in signals] or ["DOGE/USDT:USDT", "PEPE/USDT:USDT", "SHIB/USDT:USDT"])

    default_margin = round(max(100.0, min(high_kelly_bet, high_free if high_free > 0 else 500.0)), 0)
    max_margin = round(max(100.0, high_free if high_free > 0 else 10000.0), 0)

    with st.container(border=True):
        st.markdown("#### ⚡ Instant High-Risk Order Launcher (Direct to Bitget Demo/Live)")
        oc1, oc2, oc3, oc4, oc5 = st.columns([2.5, 1.5, 1.5, 1.5, 1.5])
        target_sym = oc1.selectbox("Select Penny / Meme / Volatile Coin", all_available_syms)
        margin_to_use = oc2.number_input("Margin to Risk (₹ INR)", min_value=50.0, max_value=max_margin, value=default_margin, step=100.0)
        lev_to_use = oc3.slider("Futures Leverage (x)", 1, 20, 5 if active_market == "FUTURES" else 1, disabled=(active_market == "SPOT"))

        if oc4.button("🚀 INSTANT LONG (BUY)", use_container_width=True, type="primary"):
            if high_free >= margin_to_use:
                queue_manual_order(target_sym, "buy", "HIGH_RISK", active_market, margin_to_use, lev_to_use)
                st.toast(f"⚡ Sent Instant LONG on {target_sym} (Margin: ₹{margin_to_use:,.0f} @ {lev_to_use}x)")
                time.sleep(1)
                st.rerun()
            else:
                st.error(f"Insufficient free cash in High-Risk Pool (Free: ₹{high_free:,.2f})!")

        if oc5.button("🩸 INSTANT SHORT (SELL)", use_container_width=True, disabled=(active_market == "SPOT")):
            if high_free >= margin_to_use:
                queue_manual_order(target_sym, "sell", "HIGH_RISK", active_market, margin_to_use, lev_to_use)
                st.toast(f"⚡ Sent Instant SHORT on {target_sym} (Margin: ₹{margin_to_use:,.0f} @ {lev_to_use}x)")
                time.sleep(1)
                st.rerun()
            else:
                st.error(f"Insufficient free cash in High-Risk Pool (Free: ₹{high_free:,.2f})!")

    st.markdown("#### 🎯 Active High-Risk Positions")
    high_positions = [p for p in positions if p["risk_tier"] == "HIGH_RISK"]
    if high_positions:
        for p in high_positions:
            pcol1, pcol2, pcol3, pcol4, pcol5, pcol6 = st.columns([2, 1.8, 2, 2, 1.5, 1])
            pcol1.markdown(f"🔥 **{p['symbol']}** (`{p['side'].upper()}` {p['leverage']}x)")
            pcol2.markdown(f"Margin: **₹{p['margin_inr']:,.0f}**\n*(Notional: ₹{p['notional_inr']:,.0f})*")
            pcol3.markdown(f"Entry: `{p['entry_price']}` → Live: **`{p['current_price']}`**")
            pcol4.markdown(f"SL: `{p['stop_price']}` | TP: `{p['take_profit_price']}`")
            pnl_color = "🟢" if p["unrealized_pnl_inr"] >= 0 else "🔴"
            pcol5.markdown(f"{pnl_color} **₹{p['unrealized_pnl_inr']:+,.2f}** (`{p['pnl_pct']:+.2f}%`)")
            if pcol6.button("Exit Now", key=f"close_high_{p['id']}"):
                force_close_position(p["id"], p["current_price"], p["unrealized_pnl_inr"])
                st.rerun()
    else:
        st.info("No active High-Risk trades right now.")

    st.markdown("#### 🚀 Top Penny, Meme & Volatile Movers Scanner (Live)")
    if high_sigs:
        st.dataframe(pd.DataFrame(high_sigs)[["symbol", "price", "change_24h", "vol_24h_usd", "rsi", "tech_score", "flow_state"]], use_container_width=True, hide_index=True)

# =====================================================================
# VIEW 3: LIVE TRANSACTIONS TAPE, HEARTBEATS & BOARDROOM LOGS
# =====================================================================
else:
    col_tx, col_bots = st.columns([1.5, 1])
    with col_tx:
        st.subheader("⚡ Live Transaction Tape (Opens, Closes & Realized ₹ PnL)")
        if txs:
            tx_df = pd.DataFrame(txs)
            tx_df["Time"] = tx_df["timestamp"].apply(lambda x: datetime.fromtimestamp(x).strftime("%H:%M:%S"))
            st.dataframe(
                tx_df[["Time", "tx_type", "risk_tier", "market_type", "symbol", "side", "price", "margin_inr", "leverage", "pnl_inr", "details"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No transactions executed yet.")

    with col_bots:
        st.subheader("🤖 7-Bot Live Heartbeats")
        if bots:
            b_df = pd.DataFrame(bots)
            b_df["Age (s)"] = b_df["updated_at"].apply(lambda x: f"{int(time.time() - x)}s ago")
            st.dataframe(b_df[["bot_name", "status", "detail", "Age (s)"]], use_container_width=True, hide_index=True)

    st.markdown("#### 🧠 Executive Boardroom & Exchange Execution Logs")
    if logs:
        l_df = pd.DataFrame(logs)
        l_df["Time"] = l_df["timestamp"].apply(lambda x: datetime.fromtimestamp(x).strftime("%H:%M:%S"))
        st.dataframe(l_df[["Time", "bot_name", "action", "message"]], use_container_width=True, hide_index=True)

time.sleep(2)
st.rerun()
