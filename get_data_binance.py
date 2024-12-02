import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta

class BinanceDataFetcher:
    def __init__(self):
        self.base_url_spot = "https://api.binance.com/api/v3/klines"
        self.base_url_perp = "https://fapi.binance.com/fapi/v1/klines"
        self.all_candles_data = {}

    async def fetch_candle(self, session, base_url, pair, interval="5m"):
        pair_for_binance = pair.replace("/", "").replace("-", "")
        
        params = {
            "symbol": pair_for_binance,
            "interval": interval,
            "limit": 1,
            "endTime": int((datetime.utcnow() - timedelta(minutes=5)).timestamp() * 1000)
        }
        try:
            async with session.get(base_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        return {
                            "market": pair,
                            "source": "Binance",
                            "candle_date_time_utc_x": datetime.utcfromtimestamp(data[0][0] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                            "opening_price": round(float(data[0][1]), 2),
                            "high_price": round(float(data[0][2]), 2),
                            "low_price": round(float(data[0][3]), 2),
                            "trade_price": round(float(data[0][4]), 2),
                            "candle_acc_trade_price": round(float(data[0][7]), 2),
                            "candle_acc_trade_volume": round(float(data[0][5]), 2)
                        }
        except aiohttp.ClientError as e:
            print(f"Error fetching candle for {pair} from {base_url}: {e}")
        return None

    async def fetch_candles_batch(self, pairs_batch, session):
        tasks = []
        for pair in pairs_batch:
            tasks.append(self.fetch_candle(session, self.base_url_spot, pair))
            tasks.append(self.fetch_candle(session, self.base_url_perp, pair))
        results = await asyncio.gather(*tasks)
        return results

    async def fetch_binance_data(self, upbit_pairs):
        async with aiohttp.ClientSession() as session:
            batch_size = 10
            for i in range(0, len(upbit_pairs), batch_size):
                pairs_batch = upbit_pairs[i:i + batch_size]
                print(f"Fetching batch {i // batch_size + 1}")
                candles = await self.fetch_candles_batch(pairs_batch, session)
                for pair in pairs_batch:
                    pair_data = [c for c in candles if c and c['market'] == pair]
                    if pair_data:
                        self.all_candles_data[pair] = pair_data[0]

    def combine_data(self, upbit_df):
        """Объединяет свечи Upbit и Binance в одну таблицу."""
        binance_df = pd.DataFrame(self.all_candles_data.values())  # Конвертируем в DataFrame

        # Переименовываем столбцы для данных Upbit
        upbit_df = upbit_df.rename(columns={
            "opening_price": "opening_price_upbit",
            "high_price": "high_price_upbit",
            "low_price": "low_price_upbit",
            "trade_price": "trade_price_upbit",
            "candle_acc_trade_price": "candle_acc_trade_price_upbit",
            "candle_acc_trade_volume": "candle_acc_trade_volume_upbit",
            "unit": "unit_upbit",
            "source": "source_upbit"
        })

        # Переименовываем столбцы для данных Binance
        binance_df = binance_df.rename(columns={
            "opening_price": "opening_price_binance",
            "high_price": "high_price_binance",
            "low_price": "low_price_binance",
            "trade_price": "trade_price_binance",
            "candle_acc_trade_price": "candle_acc_trade_price_binance",
            "candle_acc_trade_volume": "candle_acc_trade_volume_binance",
            "source": "source_binance"
        })

        # Объединяем данные по паре
        self.combined_data = pd.merge(
            upbit_df, binance_df, on="market", how="left"
        )

    def save_combined_data(self):
        """Сохраняет объединенные данные в CSV с дозаписью."""
        if not self.combined_data.empty:
            try:
                # Пытаемся прочитать существующий файл
                existing_data = pd.read_csv("combined_candles.csv")
                # Объединяем существующие данные с новыми
                combined = pd.concat([existing_data, self.combined_data])
                # Удаляем дубликаты, оставляя последние значения
                combined = combined.drop_duplicates(
                    subset=['market', 'candle_date_time_utc_x'], 
                    keep='last'
                )
                # Сохраняем объединенные данные
                combined.to_csv("combined_candles.csv", index=False)
                print("Сохранены объединенные данные:", combined.head())
            except FileNotFoundError:
                # Если файл не существует, создаем новый
                self.combined_data.to_csv("combined_candles.csv", index=False)
                print("Создан новый файл с данными:", self.combined_data.head())
        else:
            print("Нет данных для сохранения")

    def run(self, upbit_df):
        """Запуск сбора и обработки данных."""
        upbit_pairs = upbit_df["market"].unique().tolist()  # Извлекаем уникальные пары из DataFrame
        asyncio.run(self.fetch_binance_data(upbit_pairs))
        self.combine_data(upbit_df)
        self.save_combined_data()