# data_combiner.py
import pandas as pd
import aiohttp
from datetime import datetime

class DataCombiner:
    def __init__(self, server_url="http://localhost:5000"):
        self.server_url = server_url
        self.combined_data = pd.DataFrame()

    def combine_data(self, upbit_data, binance_data):
        """Combines data from both exchanges"""
        if upbit_data:
            upbit_df = pd.DataFrame(upbit_data)
            upbit_df = upbit_df.rename(columns={
                "opening_price": "opening_price_upbit",
                "high_price": "high_price_upbit",
                "low_price": "low_price_upbit",
                "trade_price": "trade_price_upbit",
                "candle_acc_trade_price": "candle_acc_trade_price_upbit",
                "candle_acc_trade_volume": "candle_acc_trade_volume_upbit",
                "source": "source_upbit"
            })

        if binance_data:
            binance_df = pd.DataFrame(binance_data)
            binance_df = binance_df.rename(columns={
                "opening_price": "opening_price_binance",
                "high_price": "high_price_binance",
                "low_price": "low_price_binance",
                "trade_price": "trade_price_binance",
                "candle_acc_trade_price": "candle_acc_trade_price_binance",
                "candle_acc_trade_volume": "candle_acc_trade_volume_binance",
                "source": "source_binance"
            })

        # Merge data on market and timestamp
        self.combined_data = pd.merge(
            upbit_df, binance_df,
            on=["market", "candle_date_time_utc_x"],
            how="outer"
        )

    def save_combined_data(self):
        """Saves combined data to CSV"""
        if not self.combined_data.empty:
            try:
                # Try to read existing data
                existing_data = pd.read_csv("combined_candles.csv")
                combined = pd.concat([existing_data, self.combined_data])
                # Remove duplicates
                combined = combined.drop_duplicates(
                    subset=['market', 'candle_date_time_utc_x'],
                    keep='last'
                )
                combined.to_csv("combined_candles.csv", index=False)
                print(f"Saved combined data: {len(combined)} rows")
            except FileNotFoundError:
                self.combined_data.to_csv("combined_candles.csv", index=False)
                print(f"Created new file with {len(self.combined_data)} rows")

    async def send_to_web_service(self):
        """Sends combined data to web service"""
        if not self.combined_data.empty:
            try:
                data_dict = self.combined_data.to_dict('records')
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.server_url}/update_data",
                        json={"data": data_dict}
                    ) as response:
                        if response.status == 200:
                            print(f"Data successfully sent to web service: {datetime.now()}")
                        else:
                            print(f"Error sending data: {response.status}")
            except Exception as e:
                print(f"Error sending data to web service: {e}")