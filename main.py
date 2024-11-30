import asyncio
import pandas as pd
from get_data_upbit import UpbitDataFetcher
from get_data_binance import BinanceDataFetcher
from data_combiner import DataCombiner

async def main():
    upbit_fetcher = UpbitDataFetcher()
    binance_fetcher = BinanceDataFetcher()
    data_combiner = DataCombiner()
    
    while True:
        try:
            print("\n=== Starting New Data Collection Cycle ===")
            
            # Fetch market pairs
            await upbit_fetcher.fetch_market_pairs()
            
            # Fetch data from both exchanges
            upbit_data, binance_data = await asyncio.gather(
                upbit_fetcher.fetch_all_candles(),
                binance_fetcher.fetch_all_candles()
            )
            
            # Combine and save data
            data_combiner.combine_data(upbit_data, binance_data)
            data_combiner.save_combined_data()
            
            # Send to web service
            await data_combiner.send_to_web_service()
            
            await asyncio.sleep(300)  # 5 minutes pause
            
        except Exception as e:
            print(f"Error in main loop: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())