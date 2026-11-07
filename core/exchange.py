import os
import ccxt.async_support as ccxt

class ExchangeClient:
    def __init__(self):
        self.mode = os.getenv("TRADING_MODE", "DEMO").upper()
        self.usdt_inr_rate = float(os.getenv("USDT_INR_RATE", "87.0"))
        api_key = os.getenv("BITGET_API_KEY", "").strip()
        secret = os.getenv("BITGET_SECRET", "").strip()
        password = os.getenv("BITGET_PASSPHRASE", "").strip()

        config = {
            "enableRateLimit": True,
            "options": {
                "defaultType": "swap",
                "createMarketBuyOrderRequiresPrice": False,
                "oneWayMode": True,  # Forces CCXT Bitget V2 to use One-Way Mode by default
            },
        }
        if api_key and secret and password:
            config["apiKey"] = api_key
            config["secret"] = secret
            config["password"] = password

        self.exchange = ccxt.bitget(config)

        if self.mode == "DEMO":
            self.exchange.enable_demo_trading(True)

    async def fetch_live_wallet_equity_usdt(self, market_mode: str = "FUTURES") -> float:
        """
        Fetches actual live account equity from Bitget (supports DEMO SUSDT/USDT & LIVE USDT).
        """
        if self.mode == "PAPER":
            return 0.0

        # Try Futures product types first (Bitget Demo uses SUSDT-FUTURES or USDT-FUTURES)
        product_types = ["USDT-FUTURES", "SUSDT-FUTURES"] if market_mode == "FUTURES" else ["spot", "USDT-FUTURES"]

        for p_type in product_types:
            try:
                params = {} if p_type == "spot" else {"productType": p_type}
                bal = await self.exchange.fetch_balance(params)

                # 1. Check standard CCXT currency keys (USDT or Demo SUSDT)
                for coin in ("USDT", "SUSDT"):
                    if coin in bal and isinstance(bal[coin], dict):
                        tot = float(bal[coin].get("total") or bal[coin].get("free") or 0.0)
                        if tot > 0:
                            return tot

                # 2. Check Bitget raw V2 info payload (accountEquity / usdtEquity)
                info_data = bal.get("info", {}).get("data")
                if isinstance(info_data, dict):
                    eq = float(info_data.get("usdtEquity") or info_data.get("accountEquity") or 0.0)
                    if eq > 0:
                        return eq
                elif isinstance(info_data, list):
                    for item in info_data:
                        if isinstance(item, dict):
                            eq = float(item.get("usdtEquity") or item.get("accountEquity") or item.get("available") or 0.0)
                            if eq > 0:
                                return eq
            except Exception:
                continue

        return 0.0

    async def close(self):
        await self.exchange.close()