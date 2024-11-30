# get_data_upbit.py
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta

class UpbitDataFetcher:
    def __init__(self, batch_size=10, delay=5):
        self.base_url = "https://api.upbit.com/v1/candles/minutes/5"
        self.headers = {"accept": "application/json"}
        self.shit_list = ['KRW-USDT']
        self.filtered_pairs = []
        self.all_candles_data = []
        self.batch_size = batch_size
        self.delay = delay
        self.krw_usdt_rate = None
        self.is_first_run = True

    async def fetch_market_pairs(self):
        """Fetch and filter market pairs from Upbit"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.upbit.com/v1/market/all") as response:
                    response.raise_for_status()
                    markets = await response.json()
                    self.filtered_pairs = [market['market'] for market in markets 
                                         if market['market'].startswith('KRW-') 
                                         and market['market'] not in self.shit_list]
                    print(f"\nFetched {len(self.filtered_pairs)} market pairs")
        except Exception as e:
            print(f"Error fetching market pairs: {e}")
            self.filtered_pairs = []

    async def fetch_krw_usdt_rate(self, session):
        """Fetch KRW-USDT exchange rate"""
        try:
            url = "https://api.upbit.com/v1/ticker"
            params = {'markets': 'KRW-USDT'}
            async with session.get(url, params=params, headers=self.headers) as response:
                response.raise_for_status()
                data = await response.json()
                if data:
                    self.krw_usdt_rate = data[0]['trade_price']
                    print(f"\nCurrent KRW-USDT rate: {self.krw_usdt_rate}")
                else:
                    self.krw_usdt_rate = 1300
                    print("\nWarning: Using default KRW-USDT rate: 1300")
        except Exception as e:
            print(f"Error fetching KRW-USDT rate: {e}")
            self.krw_usdt_rate = 1300

    async def fetch_historical_data(self, pair, session, pair_index, total_pairs):
        """Fetch historical data with detailed progress logging"""
        start_date = datetime(2024, 11, 1)
        current_date = datetime.utcnow()
        all_candles = []
        batch_count = 0
        
        print(f"\nProcessing pair {pair_index + 1}/{total_pairs}: {pair}")
        
        while current_date > start_date:
            batch_count += 1
            to_date = current_date.strftime('%Y-%m-%dT%H:%M:%S')
            params = {'market': pair, 'to': to_date, 'count': 200}
            
            try:
                async with session.get(self.base_url, params=params, headers=self.headers) as response:
                    response.raise_for_status()
                    candles = await response.json()
                    
                    if not candles:
                        break
                    
                    print(f"  ├── Batch {batch_count}: Got {len(candles)} candles "
                          f"(from {candles[-1]['candle_date_time_utc']} "
                          f"to {candles[0]['candle_date_time_utc']})")
                    
                    for candle in candles:
                        for key in ['opening_price', 'high_price', 'low_price', 'trade_price']:
                            candle[key] /= self.krw_usdt_rate
                        candle['market'] = f"{pair.replace('KRW-', '')}/USDT"
                        candle['source'] = 'Upbit'
                        all_candles.append(candle)
                    
                    current_date = datetime.strptime(candles[-1]['candle_date_time_utc'], 
                                                   '%Y-%m-%dT%H:%M:%S')
                    await asyncio.sleep(0.2)
                    
            except Exception as e:
                print(f"  ├── Error in batch {batch_count} for {pair}: {e}")
                break
        
        print(f"  └── Completed {pair}: Total {len(all_candles)} candles in {batch_count} batches")
        return all_candles

    async def fetch_candles_batch(self, batch, session):
        """Fetch current candles for a batch of pairs"""
        for pair in batch:
            params = {'market': pair, 'count': 1}
            try:
                async with session.get(self.base_url, params=params, headers=self.headers) as response:
                    response.raise_for_status()
                    candles = await response.json()
                    if candles:
                        print(f"  ├── Got current data for {pair}")
                        for candle in candles:
                            for key in ['opening_price', 'high_price', 'low_price', 'trade_price']:
                                candle[key] /= self.krw_usdt_rate
                            candle['market'] = f"{pair.replace('KRW-', '')}/USDT"
                            candle['source'] = 'Upbit'
                            self.all_candles_data.append(candle)
            except Exception as e:
                print(f"  ├── Error fetching candles for {pair}: {e}")

    async def fetch_all_candles(self):
        async with aiohttp.ClientSession() as session:
            await self.fetch_krw_usdt_rate(session)
            
            if self.is_first_run:
                print("\n=== Initial Run: Fetching Historical Data ===")
                print(f"Total pairs to process: {len(self.filtered_pairs)}")
                
                for idx, pair in enumerate(self.filtered_pairs):
                    historical_data = await self.fetch_historical_data(pair, session, idx, 
                                                                    len(self.filtered_pairs))
                    if historical_data:
                        self.all_candles_data.extend(historical_data)
                
                print("\n=== Historical Data Collection Complete ===")
                print(f"Total candles collected: {len(self.all_candles_data)}")
                self.is_first_run = False
                
            else:
                print("\n=== Subsequent Run: Fetching Current Data ===")
                for i in range(0, len(self.filtered_pairs), self.batch_size):
                    batch = self.filtered_pairs[i:i + self.batch_size]
                    print(f"\nProcessing batch {i//self.batch_size + 1}/{len(self.filtered_pairs)//self.batch_size + 1}")
                    await self.fetch_candles_batch(batch, session)
                    await asyncio.sleep(self.delay)

    def save_to_csv(self, filename="upbit_data.csv"):
        """Save the fetched data to CSV"""
        if self.all_candles_data:
            df = pd.DataFrame(self.all_candles_data)
            df.to_csv(filename, index=False)
            print(f"\nSaved {len(self.all_candles_data)} candles to {filename}")

