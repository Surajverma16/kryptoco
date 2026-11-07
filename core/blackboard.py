import aiosqlite
import asyncio
import os
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "company_state.db"

class Blackboard:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

        self.anomaly_queue = asyncio.Queue()
        self.cio_review_queue = asyncio.Queue()
        self.risk_queue = asyncio.Queue()
        self.execution_queue = asyncio.Queue()

    async def connect(self):
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA synchronous=NORMAL;")
        await self.conn.execute("PRAGMA cache_size=-16000;")
        await self.conn.execute("PRAGMA temp_store=MEMORY;")
        await self._init_schema()
        await self.log_event("SYSTEM", "ONLINE", "Dynamic API-Driven 60/40 Compounding Vault Initialized.")

    async def _init_schema(self):
        async with self._write_lock:
            await self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS terminal_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS bot_heartbeat (
                    bot_name TEXT PRIMARY KEY,
                    status TEXT,
                    detail TEXT,
                    updated_at REAL
                );

                CREATE TABLE IF NOT EXISTS market_signals (
                    symbol TEXT PRIMARY KEY,
                    category TEXT DEFAULT 'LOW_RISK',
                    market_type TEXT DEFAULT 'FUTURES',
                    price REAL DEFAULT 0.0,
                    change_24h REAL DEFAULT 0.0,
                    vol_24h_usd REAL DEFAULT 0.0,
                    atr REAL DEFAULT 0.0,
                    rsi REAL DEFAULT 50.0,
                    tech_score INTEGER DEFAULT 0,
                    flow_state TEXT DEFAULT 'NEUTRAL',
                    funding_rate REAL DEFAULT 0.0,
                    updated_at REAL
                );

                CREATE TABLE IF NOT EXISTS risk_vault (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    wallet_balance_usdt REAL DEFAULT 0.0,
                    usdt_inr_rate REAL DEFAULT 87.0,
                    total_starting_inr REAL DEFAULT 0.0,
                    total_live_equity_inr REAL DEFAULT 0.0,
                    low_risk_equity_inr REAL DEFAULT 0.0,
                    high_risk_equity_inr REAL DEFAULT 0.0,
                    low_risk_margin_used_inr REAL DEFAULT 0.0,
                    high_risk_margin_used_inr REAL DEFAULT 0.0,
                    realized_pnl_inr REAL DEFAULT 0.0,
                    total_trades INTEGER DEFAULT 0,
                    winning_trades INTEGER DEFAULT 0,
                    consecutive_losses INTEGER DEFAULT 0,
                    last_wallet_sync REAL DEFAULT 0.0
                );

                CREATE TABLE IF NOT EXISTS active_positions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    risk_tier TEXT DEFAULT 'LOW_RISK',
                    market_type TEXT DEFAULT 'FUTURES',
                    side TEXT,
                    leverage INTEGER DEFAULT 1,
                    margin_inr REAL,
                    notional_inr REAL,
                    entry_price REAL,
                    current_price REAL,
                    qty REAL,
                    stop_price REAL,
                    take_profit_price REAL,
                    atr REAL,
                    unrealized_pnl_inr REAL DEFAULT 0.0,
                    pnl_pct REAL DEFAULT 0.0,
                    trailing_active INTEGER DEFAULT 0,
                    opened_at REAL
                );

                CREATE TABLE IF NOT EXISTS live_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    tx_type TEXT,
                    risk_tier TEXT,
                    market_type TEXT,
                    symbol TEXT,
                    side TEXT,
                    price REAL,
                    margin_inr REAL,
                    leverage INTEGER,
                    pnl_inr REAL DEFAULT 0.0,
                    details TEXT
                );

                CREATE TABLE IF NOT EXISTS manual_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    side TEXT,
                    risk_tier TEXT,
                    market_type TEXT,
                    margin_inr REAL,
                    leverage INTEGER,
                    status TEXT DEFAULT 'PENDING',
                    created_at REAL
                );

                CREATE TABLE IF NOT EXISTS boardroom_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    bot_name TEXT,
                    action TEXT,
                    message TEXT
                );
            """)

            await self.conn.execute("INSERT OR IGNORE INTO risk_vault (id) VALUES (1);")
            await self.conn.execute("INSERT OR IGNORE INTO terminal_state (key, value) VALUES ('is_running', 'True')")
            await self.conn.execute("INSERT OR IGNORE INTO terminal_state (key, value) VALUES ('finish_current_only', 'False')")
            await self.conn.execute("INSERT OR IGNORE INTO terminal_state (key, value) VALUES ('market_mode', 'FUTURES')")
            await self.conn.execute("INSERT OR IGNORE INTO terminal_state (key, value) VALUES ('auto_high_risk', 'True')")
            await self.conn.execute("INSERT OR IGNORE INTO terminal_state (key, value) VALUES ('kelly_fraction', '0.16')")
            await self.conn.commit()

    async def sync_wallet_balance(self, live_usdt: float, usdt_inr_rate: float):
        """
        Syncs live Bitget exchange wallet balance into SQLite and dynamically splits 60/40
        while compounding realized profits.
        """
        async with self._write_lock:
            async with self.conn.execute("SELECT * FROM risk_vault WHERE id = 1") as cur:
                vault = dict(await cur.fetchone())

            async with self.conn.execute(
                "SELECT COALESCE(SUM(margin_inr), 0.0) as m FROM active_positions WHERE risk_tier = 'LOW_RISK'"
            ) as cur:
                low_used = float((await cur.fetchone())["m"])

            async with self.conn.execute(
                "SELECT COALESCE(SUM(margin_inr), 0.0) as m FROM active_positions WHERE risk_tier = 'HIGH_RISK'"
            ) as cur:
                high_used = float((await cur.fetchone())["m"])

            if live_usdt > 0:
                live_total_inr = round(live_usdt * usdt_inr_rate, 2)
                starting_inr = vault["total_starting_inr"] if vault["total_starting_inr"] > 0 else live_total_inr
            else:
                # Fallback if in PAPER mode or before first API sync: compound base + realized_pnl_inr
                base_inr = vault["total_starting_inr"]
                if base_inr <= 0:
                    env_cap = float(os.getenv("STARTING_CAPITAL", "0.0"))
                    base_inr = env_cap if env_cap > 0 else 0.0
                starting_inr = base_inr
                live_total_inr = round(max(0.0, starting_inr + vault["realized_pnl_inr"]), 2)
                live_usdt = round(live_total_inr / usdt_inr_rate, 2) if usdt_inr_rate > 0 else 0.0

            # Dynamic 60 / 40 Compounding Split of Total Live Equity
            low_eq_inr = round(live_total_inr * 0.60, 2)
            high_eq_inr = round(live_total_inr * 0.40, 2)

            await self.conn.execute("""
                UPDATE risk_vault SET
                    wallet_balance_usdt = ?,
                    usdt_inr_rate = ?,
                    total_starting_inr = ?,
                    total_live_equity_inr = ?,
                    low_risk_equity_inr = ?,
                    high_risk_equity_inr = ?,
                    low_risk_margin_used_inr = ?,
                    high_risk_margin_used_inr = ?,
                    last_wallet_sync = ?
                WHERE id = 1
            """, (
                round(live_usdt, 4), usdt_inr_rate, starting_inr, live_total_inr,
                low_eq_inr, high_eq_inr, round(low_used, 2), round(high_used, 2), time.time()
            ))
            await self.conn.commit()

    async def get_ui_state(self) -> dict:
        async with self.conn.execute("SELECT key, value FROM terminal_state") as cur:
            rows = await cur.fetchall()
            return {r["key"]: r["value"] for r in rows}

    async def heartbeat(self, bot_name: str, status: str, detail: str = ""):
        async with self._write_lock:
            await self.conn.execute("""
                INSERT INTO bot_heartbeat (bot_name, status, detail, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(bot_name) DO UPDATE SET
                    status=excluded.status, detail=excluded.detail, updated_at=excluded.updated_at
            """, (bot_name, status, detail, time.time()))
            await self.conn.commit()

    async def upsert_signal(self, symbol: str, **kwargs):
        async with self._write_lock:
            await self.conn.execute(
                "INSERT OR IGNORE INTO market_signals (symbol, updated_at) VALUES (?, ?)",
                (symbol, time.time())
            )
            sets = ", ".join([f"{k} = ?" for k in kwargs.keys()])
            values = list(kwargs.values()) + [time.time(), symbol]
            await self.conn.execute(
                f"UPDATE market_signals SET {sets}, updated_at = ? WHERE symbol = ?",
                values
            )
            await self.conn.commit()

    async def get_signal(self, symbol: str) -> dict:
        async with self.conn.execute("SELECT * FROM market_signals WHERE symbol = ?", (symbol,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else {}

    async def get_risk_state(self) -> dict:
        async with self._write_lock:
            async with self.conn.execute(
                "SELECT COALESCE(SUM(margin_inr), 0.0) as m FROM active_positions WHERE risk_tier = 'LOW_RISK'"
            ) as cur:
                low_used = float((await cur.fetchone())["m"])

            async with self.conn.execute(
                "SELECT COALESCE(SUM(margin_inr), 0.0) as m FROM active_positions WHERE risk_tier = 'HIGH_RISK'"
            ) as cur:
                high_used = float((await cur.fetchone())["m"])

            await self.conn.execute("""
                UPDATE risk_vault 
                SET low_risk_margin_used_inr = ?, high_risk_margin_used_inr = ?
                WHERE id = 1
            """, (round(low_used, 2), round(high_used, 2)))
            await self.conn.commit()

            async with self.conn.execute("SELECT * FROM risk_vault WHERE id = 1") as cur:
                return dict(await cur.fetchone())

    async def get_active_positions(self, risk_tier: str | None = None) -> list[dict]:
        if risk_tier:
            async with self.conn.execute("SELECT * FROM active_positions WHERE risk_tier = ? ORDER BY opened_at DESC", (risk_tier,)) as cur:
                return [dict(r) for r in await cur.fetchall()]
        async with self.conn.execute("SELECT * FROM active_positions ORDER BY opened_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]

    async def open_position(self, pos: dict):
        async with self._write_lock:
            await self.conn.execute("""
                INSERT INTO active_positions
                (symbol, risk_tier, market_type, side, leverage, margin_inr, notional_inr,
                 entry_price, current_price, qty, stop_price, take_profit_price, atr,
                 unrealized_pnl_inr, pnl_pct, trailing_active, opened_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, 0.0, 0, ?)
            """, (
                pos["symbol"], pos["risk_tier"], pos["market_type"], pos["side"],
                pos["leverage"], pos["margin_inr"], pos["notional_inr"],
                pos["entry_price"], pos["entry_price"], pos["qty"],
                pos["stop_price"], pos["take_profit_price"], pos["atr"], time.time()
            ))
            await self.conn.execute("""
                INSERT INTO live_transactions
                (timestamp, tx_type, risk_tier, market_type, symbol, side, price, margin_inr, leverage, pnl_inr, details)
                VALUES (?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, 0.0, ?)
            """, (
                time.time(), pos["risk_tier"], pos["market_type"], pos["symbol"],
                pos["side"].upper(), pos["entry_price"], pos["margin_inr"], pos["leverage"],
                f"Opened {pos['market_type']} ({pos['leverage']}x) | Margin: ₹{pos['margin_inr']:,.2f} | Qty: {pos['qty']}"
            ))
            await self.conn.commit()

    async def update_position_mark(self, pos_id: int, current_price: float, stop_price: float, trailing_active: int, u_pnl_inr: float, pnl_pct: float):
        async with self._write_lock:
            await self.conn.execute("""
                UPDATE active_positions
                SET current_price = ?, stop_price = ?, trailing_active = ?, unrealized_pnl_inr = ?, pnl_pct = ?
                WHERE id = ?
            """, (current_price, stop_price, trailing_active, u_pnl_inr, pnl_pct, pos_id))
            await self.conn.commit()

    async def close_position_and_settle(self, pos_id: int, exit_price: float, net_pnl_inr: float, reason: str):
        async with self._write_lock:
            async with self.conn.execute("SELECT * FROM active_positions WHERE id = ?", (pos_id,)) as cur:
                row = await cur.fetchone()
                if not row:
                    return
                pos = dict(row)

            await self.conn.execute("DELETE FROM active_positions WHERE id = ?", (pos_id,))

            await self.conn.execute("""
                INSERT INTO live_transactions
                (timestamp, tx_type, risk_tier, market_type, symbol, side, price, margin_inr, leverage, pnl_inr, details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                time.time(), reason, pos["risk_tier"], pos["market_type"], pos["symbol"],
                pos["side"].upper(), exit_price, pos["margin_inr"], pos["leverage"], net_pnl_inr,
                f"Closed @ {exit_price} | Realized PnL: ₹{net_pnl_inr:+,.2f} ({reason})"
            ))

            async with self.conn.execute("SELECT * FROM risk_vault WHERE id = 1") as cur:
                vault = dict(await cur.fetchone())

            # Compounding: Add realized PnL to total equity and re-split 60/40 dynamically
            new_realized_pnl = round(vault["realized_pnl_inr"] + net_pnl_inr, 2)
            new_total_eq = round(max(0.0, vault["total_live_equity_inr"] + net_pnl_inr), 2)
            new_low_eq = round(new_total_eq * 0.60, 2)
            new_high_eq = round(new_total_eq * 0.40, 2)

            total_trades = vault["total_trades"] + 1
            wins = vault["winning_trades"] + (1 if net_pnl_inr > 0 else 0)
            consec_loss = 0 if net_pnl_inr > 0 else vault["consecutive_losses"] + 1

            await self.conn.execute("""
                UPDATE risk_vault SET
                    total_live_equity_inr = ?,
                    low_risk_equity_inr = ?,
                    high_risk_equity_inr = ?,
                    realized_pnl_inr = ?,
                    total_trades = ?,
                    winning_trades = ?,
                    consecutive_losses = ?
                WHERE id = 1
            """, (new_total_eq, new_low_eq, new_high_eq, new_realized_pnl, total_trades, wins, consec_loss))
            await self.conn.commit()

    async def fetch_pending_manual_orders(self) -> list[dict]:
        async with self._write_lock:
            async with self.conn.execute("SELECT * FROM manual_orders WHERE status = 'PENDING' ORDER BY id ASC") as cur:
                orders = [dict(r) for r in await cur.fetchall()]
            if orders:
                await self.conn.execute("UPDATE manual_orders SET status = 'PROCESSED' WHERE status = 'PENDING'")
                await self.conn.commit()
            return orders

    async def log_event(self, bot_name: str, action: str, message: str):
        async with self._write_lock:
            await self.conn.execute(
                "INSERT INTO boardroom_log (timestamp, bot_name, action, message) VALUES (?, ?, ?, ?)",
                (time.time(), bot_name, action, message)
            )
            await self.conn.execute("""
                DELETE FROM boardroom_log WHERE id NOT IN (
                    SELECT id FROM boardroom_log ORDER BY id DESC LIMIT 300
                )
            """)
            await self.conn.commit()

    async def close(self):
        if self.conn:
            await self.conn.close()