import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

load_dotenv()
DB_PATH = Path(__file__).parent / "data" / "company_state.db"
console = Console()

def fetch_snapshot():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2.0)
    conn.row_factory = sqlite3.Row
    try:
        vault = dict(conn.execute("SELECT * FROM risk_vault WHERE id = 1").fetchone() or {})
        bots = [dict(r) for r in conn.execute("SELECT * FROM bot_heartbeat ORDER BY bot_name").fetchall()]
        signals = [dict(r) for r in conn.execute("SELECT * FROM market_signals ORDER BY symbol").fetchall()]
        positions = [dict(r) for r in conn.execute("SELECT * FROM active_positions ORDER BY opened_at DESC").fetchall()]
        logs = [dict(r) for r in conn.execute("SELECT * FROM boardroom_log ORDER BY id DESC LIMIT 10").fetchall()]
        return {"vault": vault, "bots": bots, "signals": signals, "positions": positions, "logs": logs}
    except Exception:
        return None
    finally:
        conn.close()

def render_dashboard(data: dict | None) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=4),
        Layout(name="upper", size=11),
        Layout(name="middle", size=9),
        Layout(name="logs", size=14)
    )
    layout["upper"].split_row(
        Layout(name="bots", ratio=1),
        Layout(name="signals", ratio=1)
    )

    if not data or not data["vault"]:
        layout["header"].update(Panel("Waiting for main.py to initialize data/company_state.db...", style="yellow"))
        return layout

    v = data["vault"]
    mode = os.getenv("TRADING_MODE", "PAPER").upper()
    eq = v.get("current_equity", 1500.0)
    start_eq = v.get("starting_equity", 1500.0)
    total_pnl = eq - start_eq
    daily_pct = v.get("daily_pnl_pct", 0.0)
    trades = v.get("total_trades", 0)
    wins = v.get("winning_trades", 0)
    win_rate = (wins / trades * 100) if trades > 0 else 0.0
    cb_status = "LOCKED (RED)" if v.get("circuit_breaker_locked") else "ACTIVE (GREEN)"

    # 1. Header Panel
    header_text = Text.assemble(
        (" MODE: ", "bold white"), (f"{mode} ", "bold cyan"),
        ("| EQUITY: ", "bold white"), (f"${eq:,.2f} ({total_pnl:+.2f}) ", "bold green" if total_pnl >= 0 else "bold red"),
        ("| TODAY: ", "bold white"), (f"{daily_pct:+.2f}% ", "bold green" if daily_pct >= 0 else "bold red"),
        ("| WIN RATE: ", "bold white"), (f"{win_rate:.1f}% ({wins}/{trades}) ", "bold yellow"),
        ("| VAULT: ", "bold white"), (cb_status, "bold red" if v.get("circuit_breaker_locked") else "bold green")
    )
    layout["header"].update(Panel(header_text, title="[bold cyan]SAVIOUR MULTI-AGENT AI TRADING FIRM[/bold cyan]", border_style="cyan"))

    # 2. Bot Heartbeat Table
    bot_table = Table(expand=True, box=None)
    bot_table.add_column("Agent / Bot", style="bold cyan")
    bot_table.add_column("Status", style="bold")
    bot_table.add_column("Live Telemetry")
    bot_table.add_column("Age", justify="right")

    now = time.time()
    for b in data["bots"]:
        age = f"{int(now - b['updated_at'])}s"
        st = b["status"]
        color = "green" if st in ("SCANNING", "COMPUTING", "ENTERING", "GUARDING", "WARDEN_ACTIVE") else ("red" if "ERR" in st else "yellow")
        bot_table.add_row(b["bot_name"], f"[{color}]{st}[/{color}]", str(b["detail"])[:32], age)
    layout["bots"].update(Panel(bot_table, title="7-Bot Heartbeat Monitor", border_style="blue"))

    # 3. Market Signals Table
    sig_table = Table(expand=True, box=None)
    sig_table.add_column("Pair", style="bold white")
    sig_table.add_column("Price", justify="right")
    sig_table.add_column("RSI", justify="right")
    sig_table.add_column("Score", justify="right")
    sig_table.add_column("Flow / OI State")
    sig_table.add_column("Funding", justify="right")

    for s in data["signals"]:
        sc = s.get("tech_score") or 0
        sc_col = "green" if sc > 0 else ("red" if sc < 0 else "dim")
        sig_table.add_row(
            s["symbol"].replace(":USDT", ""),
            f"{s.get('price', 0):.4f}",
            f"{s.get('rsi', 50):.1f}",
            f"[{sc_col}]{sc:+d}[/{sc_col}]",
            str(s.get("flow_state", "NEUTRAL")),
            f"{s.get('funding_rate', 0):.4f}%"
        )
    layout["signals"].update(Panel(sig_table, title="Live Derivatives & Technical Radar", border_style="blue"))

    # 4. Active Open Positions
    pos_table = Table(expand=True, box=None)
    pos_table.add_column("Symbol", style="bold")
    pos_table.add_column("Side")
    pos_table.add_column("Notional", justify="right")
    pos_table.add_column("Entry", justify="right")
    pos_table.add_column("Mark Price", justify="right")
    pos_table.add_column("Stop Loss (Mark)", justify="right")
    pos_table.add_column("Take Profit", justify="right")
    pos_table.add_column("Net uPnL", justify="right")

    for p in data["positions"]:
        u_pnl = p.get("unrealized_pnl", 0.0)
        pnl_col = "bold green" if u_pnl >= 0 else "bold red"
        trail_tag = " [TRAIL]" if p.get("trailing_active") else ""
        pos_table.add_row(
            p["symbol"].replace(":USDT", ""),
            f"[green]LONG[/green]" if p["side"] == "buy" else "[red]SHORT[/red]",
            f"${p['notional_usd']:,.2f}",
            f"{p['entry_price']:.4f}",
            f"{p['current_price']:.4f}",
            f"{p['stop_price']:.4f}{trail_tag}",
            f"{p['take_profit_price']:.4f}",
            f"[{pnl_col}]${u_pnl:+.2f}[/{pnl_col}]"
        )
    layout["middle"].update(Panel(pos_table, title="Active Positions & Exit Warden (Bot 7)", border_style="magenta"))

    # 5. Boardroom Log
    log_table = Table(expand=True, box=None, show_header=False)
    log_table.add_column("Time", style="dim", width=10)
    log_table.add_column("Bot", style="bold cyan", width=16)
    log_table.add_column("Action", width=16)
    log_table.add_column("Message")

    for lg in data["logs"]:
        ts_str = datetime.fromtimestamp(lg["timestamp"]).strftime("%H:%M:%S")
        act = lg["action"]
        act_col = "green" if "APPROV" in act or "OPEN" in act or "WIN" in act else ("red" if "VETO" in act or "ERR" in act or "LOSS" in act else "yellow")
        log_table.add_row(ts_str, lg["bot_name"], f"[{act_col}]{act}[/{act_col}]", lg["message"])
    layout["logs"].update(Panel(log_table, title="AI Executive Boardroom Feed (Latest Decisions)", border_style="green"))

    return layout

if __name__ == "__main__":
    with Live(render_dashboard(fetch_snapshot()), refresh_per_second=1, screen=True) as live:
        try:
            while True:
                time.sleep(1)
                live.update(render_dashboard(fetch_snapshot()))
        except KeyboardInterrupt:
            pass