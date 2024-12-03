import asyncio
from get_data_upbit import UpbitDataFetcher
from get_data_binance import BinanceDataFetcher
from data_combiner import DataCombiner
import aiohttp
from datetime import datetime, timedelta

async def main():
    try:
        # Инициализация фетчеров
        upbit_fetcher = UpbitDataFetcher()
        binance_fetcher = BinanceDataFetcher()
        data_combiner = DataCombiner()

        # Получаем список пар с Upbit
        await upbit_fetcher.fetch_market_pairs()
        
        while True:
            try:
                print("\n=== Starting New Data Collection Cycle ===")
                
                # Синхронное получение данных с обеих бирж
                tasks = [
                    upbit_fetcher.fetch_all_candles(),
                    binance_fetcher.fetch_all_pairs(['BTC/USDT', 'NEO/USDT', 'ETC/USDT', 'QTUM/USDT', 'SNT/USDT', 'ETH/USDT', 'XRP/USDT', 'MTL/USDT', 'STEEM/USDT', 'XLM/USDT', 'ARDR/USDT', 'ARK/USDT', 'LSK/USDT', 'STORJ/USDT', 'ADA/USDT', 'POWR/USDT', 'ICX/USDT', 'EOS/USDT', 'TRX/USDT', 'SC/USDT', 'ZIL/USDT', 'ONT/USDT', 'POLYX/USDT', 'ZRX/USDT', 'BAT/USDT', 'BCH/USDT', 'IOST/USDT', 'CVC/USDT', 'IQ/USDT', 'IOTA/USDT', 'HIFI/USDT', 'ONG/USDT', 'GAS/USDT', 'ELF/USDT', 'KNC/USDT', 'BSV/USDT', 'TFUEL/USDT', 'THETA/USDT', 'QKC/USDT', 'MANA/USDT', 'ANKR/USDT', 'AERGO/USDT', 'ATOM/USDT', 'WAXP/USDT', 'HBAR/USDT', 'MBL/USDT', 'STPT/USDT', 'ORBS/USDT', 'VET/USDT', 'CHZ/USDT', 'STMX/USDT', 'HIVE/USDT', 'KAVA/USDT', 'LINK/USDT', 'XTZ/USDT', 'JST/USDT', 'TON/USDT', 'SXP/USDT', 'DOT/USDT', 'STRAX/USDT', 'GLM/USDT', 'SAND/USDT', 'DOGE/USDT', 'PUNDIX/USDT', 'FLOW/USDT', 'AXS/USDT', 'SOL/USDT', 'STX/USDT', 'POL/USDT', 'XEC/USDT', '1INCH/USDT', 'AAVE/USDT', 'ALGO/USDT', 'NEAR/USDT', 'AVAX/USDT', 'GMT/USDT', 'SHIB/USDT', 'CELO/USDT', 'T/USDT', 'ARB/USDT', 'EGLD/USDT', 'APT/USDT', 'MASK/USDT', 'GRT/USDT', 'SUI/USDT', 'SEI/USDT', 'MINA/USDT', 'BLUR/USDT', 'IMX/USDT', 'ID/USDT', 'PYTH/USDT', 'ASTR/USDT', 'AKT/USDT', 'ZETA/USDT', 'AUCTION/USDT', 'STG/USDT', 'ONDO/USDT', 'ZRO/USDT', 'JUP/USDT', 'ENS/USDT', 'G/USDT', 'PENDLE/USDT', 'USDC/USDT', 'UXLINK/USDT', 'BIGTIME/USDT', 'CKB/USDT', 'W/USDT', 'INJ/USDT', 'UNI/USDT', 'MEW/USDT', 'SAFE/USDT', 'DRIFT/USDT', 'AGLD/USDT', 'PEPE/USDT', 'BONK/USDT', 'WAVES/USDT', 'XEM/USDT', 'GRS/USDT', 'SBD/USDT', 'BTG/USDT', 'LOOM/USDT', 'BOUNTY/USDT', 'BTT/USDT', 'MOC/USDT', 'TT/USDT', 'GAME2/USDT', 'MLK/USDT', 'MED/USDT', 'DKA/USDT', 'AHT/USDT', 'BORA/USDT', 'CRO/USDT', 'HUNT/USDT', 'MVL/USDT', 'AQT/USDT', 'META/USDT', 'FCT2/USDT', 'CBK/USDT', 'HPO/USDT', 'STRIKE/USDT', 'CTC/USDT', 'MNT/USDT', 'BEAM/USDT', 'BLAST/USDT', 'TAIKO/USDT', 'ATH/USDT', 'CARV/USDT']
                    )
                ]
                
                # Выполняем задачи параллельно
                upbit_data, binance_data = await asyncio.gather(*tasks)
                
                # Сохраняем данные
                upbit_fetcher.save_to_csv()
                binance_fetcher.save_to_csv()
                
               # print(upbit_fetcher.all_candles_data)
               # print(binance_fetcher.all_pairs_data)

                # Обработка и комбинирование данных
                data_combiner.combine_data('/Users/user/Desktop/upbit vs binance/upbit_data.csv', '/Users/user/Desktop/upbit vs binance/binance_historical_data.csv')
                data_combiner.save_combined_data()
                await data_combiner.send_to_web_service()
                
                print("\n=== Data Collection Cycle Completed ===")
                await asyncio.sleep(260)  # 5 минут пауза
                
            except Exception as e:
                raise e
                await asyncio.sleep(60)
                
    except Exception as e:
        raise e

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user")
    except Exception as e:
        raise e