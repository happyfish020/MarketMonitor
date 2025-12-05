
class AshareProcessor:
    def build_from_daily(self, snapshot):
        df = snapshot.get("zh_spot")
        return {
            "features": {
                "pct": df["pct"].tolist() if df is not None else [],
                "volume": df["volume"].tolist() if df is not None else [],
                "amount": df["amount"].tolist() if df is not None else [],
                "price": df["price"].tolist() if df is not None else [],
                "pre_close": df["pre_close"].tolist() if df is not None else [],
                "change": df["change"].tolist() if df is not None else [],
            }
        }
