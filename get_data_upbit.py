import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import os

class UpbitDataFetcher:
    def __init__(self, batch_size=200, delay=1):  # Adjusted for Upbit's 200 requests/sec limit
        self.base_url = "https://api.upbit.com/v1/candles/minutes/5"
        self.headers = {"accept": "application/json"}
        self.exclude_pairs = ['KRW-USDT']
        self.filtered_pairs = []
        self.all_candles_data = []
        self.batch_size = batch_size
        self.delay = delay
        self.krw_usdt_rate = None
        self.data_directory = "data"
        self.historical_data_file = os.path.join(self.data_directory, "upbit_historical_data.csv")

    def ensure_data_directory(self):
        if not os.path.exists(self.data_directory):
            os.makedirs(self.data_directory)

    async def fetch_market_pairs(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.upbit.com/v1/market/all") as response:
                    if response.status == 200:
                        markets = await response.json()
                        self.filtered_pairs = [
                            market['market'] for market in markets
                            if market['market'].startswith('KRW-') 
                            and market['market'] not in self.exclude_pairs
                        ]
                        print(f"\nFetched {len(self.filtered_pairs)} market pairs")
                    else:
                        print(f"Error fetching market pairs: {response.status}")
                        self.filtered_pairs = []
        except Exception as e:
            print(f"Error fetching market pairs: {e}")
            self.filtered_pairs = []

    async def fetch_krw_usdt_rate(self, session):
        try:
            params = {'markets': 'KRW-USDT'}
            async with session.get("https://api.upbit.com/v1/ticker", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        self.krw_usdt_rate = data[0]['trade_price']
                        print(f"Current KRW-USDT rate: {self.krw_usdt_rate}")
                    else:
                        self.krw_usdt_rate = 1300
                        print("Using default KRW-USDT rate: 1300")
        except Exception as e:
            print(f"Error fetching KRW-USDT rate: {e}")
            self.krw_usdt_rate = 1300

    async def fetch_candle_data(self, pair, session):
        """Fetch single candle data for a pair"""
        to_date = (datetime.utcnow() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
        params = {
            'market': pair,
            'to': to_date,
            'count': 1
        }
        
        try:
            async with session.get(self.base_url, params=params, headers=self.headers) as response:
                if response.status == 200:
                    candles = await response.json()
                    if candles:
                        candle = candles[0]
                        processed_candle = {
                            'market': f"{pair.replace('KRW-', '')}/USDT",
                            'source': 'Upbit',
                            'candle_date_time_utc': candle['candle_date_time_utc'],
                            'opening_price': float(candle['opening_price']) / self.krw_usdt_rate,
                            'high_price': float(candle['high_price']) / self.krw_usdt_rate,
                            'low_price': float(candle['low_price']) / self.krw_usdt_rate,
                            'trade_price': float(candle['trade_price']) / self.krw_usdt_rate,
                            'candle_acc_trade_volume': float(candle['candle_acc_trade_volume']),
                            'candle_acc_trade_price': float(candle['candle_acc_trade_price']) / self.krw_usdt_rate,
                            'timestamp': pd.to_datetime(candle['candle_date_time_utc'])
                        }
                        return processed_candle
        except Exception as e:
            print(f"Error fetching candle for {pair}: {e}")
        return None

    async def fetch_candles_batch(self, pairs_batch, session):
        """Fetch candles for a batch of pairs"""
        tasks = []
        for pair in pairs_batch:
            tasks.append(self.fetch_candle_data(pair, session))
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    async def fetch_all_candles(self):
        """Fetch all candles with proper rate limiting"""
        await self.fetch_market_pairs()
        
        async with aiohttp.ClientSession() as session:
            await self.fetch_krw_usdt_rate(session)
            
            total_pairs = len(self.filtered_pairs)
            for i in range(0, total_pairs, self.batch_size):
                batch = self.filtered_pairs[i:i + self.batch_size]
                print(f"Processing batch {i//self.batch_size + 1}")
                results = await self.fetch_candles_batch(batch, session)
                self.all_candles_data.extend(results)
                await asyncio.sleep(self.delay)  # Rate limiting delay between batches

    def save_to_csv(self):
        """Save data to CSV with proper handling"""
        if not self.all_candles_data:
            print("No data to save")
            return

        self.ensure_data_directory()
        new_df = pd.DataFrame(self.all_candles_data)
        
        try:
            existing_df = pd.read_csv(self.historical_data_file)
            existing_df['timestamp'] = pd.to_datetime(existing_df['candle_date_time_utc'])
            
            combined_df = pd.concat([existing_df, new_df])
            combined_df = combined_df.drop_duplicates(
                subset=['market', 'timestamp'], 
                keep='last'
            )
            
            combined_df = combined_df.sort_values(['market', 'timestamp'])
            combined_df.to_csv(self.historical_data_file, index=False)
            print(f"Updated {self.historical_data_file} with {len(combined_df)} records")
            
        except FileNotFoundError:
            new_df = new_df.sort_values(['market', 'timestamp'])
            new_df.to_csv(self.historical_data_file, index=False)
            print(f"Created new file {self.historical_data_file} with {len(new_df)} records")

    async def run(self):
        """Main execution method"""
        await self.fetch_all_candles()
        self.save_to_csv()

if __name__ == "__main__":
    fetcher = UpbitDataFetcher()
    asyncio.run(fetcher.run())