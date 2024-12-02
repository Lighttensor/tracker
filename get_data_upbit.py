import requests
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
        self.krw_usdt_rate = None  # Курс KRW/USDT

    def fetch_market_pairs(self):
        """Получает список всех пар с Upbit и фильтрует их."""
        url = "https://api.upbit.com/v1/market/all?isDetails=true"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            markets_data = response.json()

            df = pd.DataFrame(markets_data)
            self.filtered_pairs = df[
                df['market'].str.startswith('KRW-') & 
                ~df['market'].isin(self.shit_list)
            ]['market'].tolist()

            print(f"Filtered market pairs: {self.filtered_pairs[:5]} ...")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")

    async def fetch_krw_usdt_rate(self, session):
        """Получает курс KRW/USDT."""
        url = "https://api.upbit.com/v1/ticker"
        params = {"markets": "KRW-USDT"}
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        self.krw_usdt_rate = data[0]["trade_price"]
                        print(f"KRW/USDT rate: {self.krw_usdt_rate}")
        except aiohttp.ClientError as e:
            print(f"Failed to fetch KRW-USDT rate: {e}")

    async def fetch_candle_data(self, pair, session):
        """Асинхронный метод для получения свечей по одной паре."""
        to_date = (datetime.utcnow() - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S')
        params = {'market': pair, 'count': 1, 'to': to_date}
        try:
            async with session.get(self.base_url, params=params, headers=self.headers) as response:
                response.raise_for_status()
                candles = await response.json()

                if candles:
                    # Конвертация цен в доллары
                    for candle in candles:
                        for key in ['opening_price', 'high_price', 'low_price', 'trade_price']:
                            candle[key] /= self.krw_usdt_rate
                        # Преобразуем пару в формат "SYMBOL/USDT" (если это Upbit)
                        if 'KRW-' in pair:
                            candle['market'] = f"{pair.replace('KRW-', '')}/USDT"
                        else:
                            candle['market'] = pair  # Для других бирж пары уже в нужном формате
                        candle['source'] = 'Upbit'
                    self.all_candles_data.extend(candles)
                    print(f"Fetched candles for {pair}")
                else:
                    print(f"No data for {pair}")
        except aiohttp.ClientError as e:
            print(f"Failed to fetch candles for {pair}: {e}")

    async def fetch_candles_batch(self, pairs_batch, session):
        tasks = [self.fetch_candle_data(pair, session) for pair in pairs_batch]
        await asyncio.gather(*tasks)

    async def fetch_all_candles(self):
        async with aiohttp.ClientSession() as session:
            await self.fetch_krw_usdt_rate(session)
            for i in range(0, len(self.filtered_pairs), self.batch_size):
                batch = self.filtered_pairs[i:i + self.batch_size]
                print(f"Fetching batch {i // self.batch_size + 1}")
                await self.fetch_candles_batch(batch, session)
                await asyncio.sleep(self.delay)

    def save_to_csv(self):
        """Сохраняет свечи в CSV с подписью источника."""
        if self.all_candles_data:
            df = pd.DataFrame(self.all_candles_data)
            df.to_csv("upbit_candles_usdt.csv", index=False)
            print("Saved to upbit_candles_usdt.csv")

    def run(self):
        """Запуск сбора данных."""
        self.fetch_market_pairs()
        asyncio.run(self.fetch_all_candles())
        self.save_to_csv()