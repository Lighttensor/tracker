import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta

class BinanceDataFetcher:
    def __init__(self):
        self.base_url_spot = "https://api.binance.com/api/v3/klines"
        self.base_url_perp = "https://fapi.binance.com/fapi/v1/klines"
        self.all_candles_data = []
        self.is_first_run = True
        self.trading_pairs = self.load_pairs_from_file()

    def load_pairs_from_file(self):
        """Load trading pairs from names.txt"""
        try:
            with open('names.txt', 'r') as file:
                pairs = eval(file.read())
                print(f"Loaded {len(pairs)} pairs from names.txt")
                return pairs
        except Exception as e:
            print(f"Error loading pairs from names.txt: {e}")
            return []

    async def check_market_availability(self, session, pair):
        """Check if pair is available on spot or perpetual market"""
        pair_for_binance = pair.replace("/", "")
        
        # Check spot market first
        try:
            params = {"symbol": pair_for_binance, "interval": "5m", "limit": 1}
            async with session.get(self.base_url_spot, params=params) as response:
                if response.status == 200:
                    return "spot", self.base_url_spot
        except:
            pass

        # If not available on spot, check perpetual market
        try:
            params = {"symbol": pair_for_binance, "interval": "5m", "limit": 1}
            async with session.get(self.base_url_perp, params=params) as response:
                if response.status == 200:
                    return "perpetual", self.base_url_perp
        except:
            pass

        return None, None

    async def fetch_historical_candles(self, session, base_url, pair):
        """Fetch historical candles for a given pair"""
        start_date = datetime(2024, 11, 1)
        current_date = datetime.utcnow()
        all_candles = []
        batch_count = 0
        
        pair_for_binance = pair.replace("/", "")
        
        while current_date > start_date:
            batch_count += 1
            end_time = int(current_date.timestamp() * 1000)
            
            params = {
                "symbol": pair_for_binance,
                "interval": "5m",
                "limit": 1000,
                "endTime": end_time
            }
            
            try:
                async with session.get(base_url, params=params) as response:
                    if response.status == 200:
                        candles = await response.json()
                        
                        if not candles:
                            break
                        
                        print(f"  Batch {batch_count}: Got {len(candles)} candles "
                              f"(from {datetime.fromtimestamp(candles[-1][0]/1000)} "
                              f"to {datetime.fromtimestamp(candles[0][0]/1000)})")
                        
                        for candle in candles:
                            all_candles.append({
                                "market": pair,
                                "source": f"Binance_{base_url.split('.')[-2].split('/')[-1]}",
                                "candle_date_time_utc_x": datetime.utcfromtimestamp(candle[0] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                                "opening_price": float(candle[1]),
                                "high_price": float(candle[2]),
                                "low_price": float(candle[3]),
                                "trade_price": float(candle[4]),
                                "candle_acc_trade_price": float(candle[7]),
                                "candle_acc_trade_volume": float(candle[5])
                            })
                        
                        current_date = datetime.fromtimestamp(candles[-1][0]/1000)
                        await asyncio.sleep(0.2)
                        
                    else:
                        print(f"Error response {response.status} for {pair}")
                        break
                        
            except Exception as e:
                print(f"Error in batch {batch_count} for {pair}: {e}")
                break
        
        print(f"Completed {pair}: Total {len(all_candles)} candles in {batch_count} batches")
        return all_candles

    async def fetch_all_candles(self):
        """Fetch historical or current data for all pairs"""
        all_data = []
        
        async with aiohttp.ClientSession() as session:
            if self.is_first_run:
                print("\n=== Initial Run: Fetching Historical Data ===")
                for idx, pair in enumerate(self.trading_pairs):
                    market_type, base_url = await self.check_market_availability(session, pair)
                    if market_type:
                        print(f"Processing {pair} from {market_type} market ({idx + 1}/{len(self.trading_pairs)})")
                        historical_data = await self.fetch_historical_candles(session, base_url, pair)
                        if historical_data:
                            all_data.extend(historical_data)
                    else:
                        print(f"Pair {pair} not available on either market")
                self.is_first_run = False
            else:
                print("\n=== Subsequent Run: Fetching Current Data ===")
                batch_size = 10
                for i in range(0, len(self.trading_pairs), batch_size):
                    batch = self.trading_pairs[i:i + batch_size]
                    results = await self.fetch_candles_batch(batch, session)
                    all_data.extend(results)
        
        return all_data

    async def fetch_candles_batch(self, pairs, session):
        """Fetch current candles for a batch of pairs"""
        results = []
        for pair in pairs:
            market_type, base_url = await self.check_market_availability(session, pair)
            if not market_type:
                continue
                
            pair_for_binance = pair.replace("/", "")
            params = {
                "symbol": pair_for_binance,
                "interval": "5m",
                "limit": 1
            }
            
            try:
                async with session.get(base_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data:
                            candle = data[0]
                            results.append({
                                "market": pair,
                                "source": f"Binance_{market_type}",
                                "candle_date_time_utc_x": datetime.utcfromtimestamp(candle[0] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                                "opening_price": float(candle[1]),
                                "high_price": float(candle[2]),
                                "low_price": float(candle[3]),
                                "trade_price": float(candle[4]),
                                "candle_acc_trade_price": float(candle[7]),
                                "candle_acc_trade_volume": float(candle[5])
                            })
            except Exception as e:
                print(f"Error fetching candles for {pair}: {e}")
                
        return results