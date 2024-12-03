import asyncio
from get_data_upbit import UpbitDataFetcher
from get_data_binance import BinanceDataFetcher
from data_combiner import DataCombiner

async def main():
    # Инициализация объектов для сбора данных
    data_combiner = DataCombiner()

    # Комбинирование данных
    print("Combining data...")
    data_combiner.combine_data(
        '/Users/user/Desktop/upbit vs binance/upbit_data.csv', 
        '/Users/user/Desktop/upbit vs binance/binance_historical_data.csv'
    )
    data_combiner.save_combined_data()

    # Отправка данных на веб-сервис
    print("Sending combined data to web service...")
    await data_combiner.send_to_web_service()

if __name__ == "__main__":
    asyncio.run(main())