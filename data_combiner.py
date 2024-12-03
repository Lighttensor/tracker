import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import aiohttp
import logging
from typing import Dict, List, Optional, Union
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_combiner.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataValidationError(Exception):
    """Пользовательское исключение для ошибок валидации данных"""
    pass

class DataCombiner:
    """Класс для объединения и обработки рыночных данных от Upbit и Binance"""
    
    def __init__(self, server_url: str = 'http://localhost:5000'):
        self.server_url = server_url
        self.raw_data = pd.DataFrame()
        self.processed_data = pd.DataFrame()
        
        # Требуемые столбцы для проверки
        self.required_columns = {
            'upbit': {
                'market': str,
                'candle_date_time_utc': str,
                'opening_price': float,
                'high_price': float,
                'low_price': float,
                'trade_price': float,
                'candle_acc_trade_volume': float
            },
            'binance': {
                'market': str,
                'candle_date_time_utc': str,
                'opening_price': float,
                'high_price': float,
                'low_price': float,
                'close_price': float,
                'volume': float
            }
        }

    def _normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализация названий колонок"""
        df.columns = df.columns.str.strip().str.lower()
        return df

    def _check_data_types(self, df: pd.DataFrame, source: str) -> None:
        """Проверка типов данных в колонках"""
        for col, expected_type in self.required_columns[source].items():
            if col in df.columns:
                if expected_type in (float, int):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif expected_type == str:
                    df[col] = df[col].astype(str)
            else:
                raise DataValidationError(f"Missing column {col} in {source} data")

    def validate_input_data(self, df: pd.DataFrame, source: str) -> None:
        """Расширенная валидация входных данных"""
        logger.info(f"Validating {source} data")
        if df.empty:
            raise DataValidationError(f"{source} data is empty")
        self._normalize_column_names(df)
        self._check_data_types(df, source)

    def _process_datetime(self, df: pd.DataFrame, datetime_column: str) -> pd.DataFrame:
        """Преобразование временных меток"""
        if datetime_column in df.columns:
            df[datetime_column] = pd.to_datetime(df[datetime_column], errors='coerce')
            invalid_dates = df[df[datetime_column].isnull()].index
            if len(invalid_dates) > 0:
                logger.warning(f"Found invalid dates in {datetime_column}")
        return df

    def combine_data(self, upbit_file: str, binance_file: str) -> pd.DataFrame:
        """Объединение данных Upbit и Binance"""
        logger.info("Combining data...")
        upbit_df = pd.read_csv(upbit_file)
        binance_df = pd.read_csv(binance_file)

        self.validate_input_data(upbit_df, 'upbit')
        self.validate_input_data(binance_df, 'binance')

        upbit_df = self._process_datetime(upbit_df, 'candle_date_time_utc')
        binance_df = self._process_datetime(binance_df, 'candle_date_time_utc')

        combined_data = []

        upbit_grouped = upbit_df.groupby('market')
        binance_grouped = binance_df.groupby('market')

        for market in upbit_grouped.groups.keys():
            if market in binance_grouped.groups:
                upbit_market_data = upbit_grouped.get_group(market)
                binance_market_data = binance_grouped.get_group(market)

                for _, upbit_row in upbit_market_data.iterrows():
                    upbit_time = upbit_row['candle_date_time_utc']
                    matching_binance = binance_market_data[
                        (binance_market_data['candle_date_time_utc'] >= upbit_time - timedelta(minutes=2)) &
                        (binance_market_data['candle_date_time_utc'] <= upbit_time + timedelta(minutes=2))
                    ]
                    if not matching_binance.empty:
                        matching_binance['time_diff'] = abs(
                            matching_binance['candle_date_time_utc'] - upbit_time
                        )
                        closest_binance = matching_binance.loc[matching_binance['time_diff'].idxmin()]

                        combined_record = {
                            'market': market,
                            'timestamp_upbit': upbit_time,
                            'timestamp_binance': closest_binance['candle_date_time_utc'],
                            'opening_price_upbit': upbit_row['opening_price'],
                            'high_price_upbit': upbit_row['high_price'],
                            'low_price_upbit': upbit_row['low_price'],
                            'trade_price_upbit': upbit_row['trade_price'],
                            'volume_upbit': upbit_row['candle_acc_trade_volume'],
                            'opening_price_binance': closest_binance['opening_price'],
                            'high_price_binance': closest_binance['high_price'],
                            'low_price_binance': closest_binance['low_price'],
                            'close_price_binance': closest_binance['close_price'],
                            'volume_binance': closest_binance['volume'],
                            'time_difference_seconds': closest_binance['time_diff'].total_seconds()
                        }

                        # Расчет дополнительных показателей
                        combined_record['premium_diff'] = (
                            upbit_row['trade_price'] - closest_binance['close_price']
                        )
                        combined_record['premium_percent'] = (
                            combined_record['premium_diff'] / closest_binance['close_price'] * 100
                        )

                        combined_data.append(combined_record)

        self.processed_data = pd.DataFrame(combined_data)
        logger.info(f"Combined {len(self.processed_data)} rows of data")
        return self.processed_data

    def save_combined_data(self, output_file: str = "combined_market_data.csv") -> None:
        """Сохранение объединенных данных"""
        if self.processed_data.empty:
            logger.error("No data to save")
            return
        self.processed_data.to_csv(output_file, index=False)
        logger.info(f"Data saved to {output_file}")

def main():
    combiner = DataCombiner()
    upbit_file = "upbit_data.csv"
    binance_file = "binance_historical_data.csv"

    combined_data = combiner.combine_data(upbit_file, binance_file)
    if not combined_data.empty:
        combiner.save_combined_data()

if __name__ == "__main__":
    main()