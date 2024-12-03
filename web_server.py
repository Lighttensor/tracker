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
        self.features_data = pd.DataFrame()

    def calculate_features(self):
        """Расчет всех дополнительных показателей"""
        if self.processed_data.empty:
            logger.error("No data to calculate features")
            return
        
        try:
            df = self.processed_data.copy()

            # Скользящие средние премий (разница и процент) и объемов
            timeframes = {
                '1H': '1 hour',
                '24H': '24 hours',
                '1M': '1 month'
            }

            for tf, label in timeframes.items():
                df[f'premium_diff_avg_{tf}'] = (
                    df.groupby('market')['premium_diff']
                    .transform(lambda x: x.rolling(label).mean())
                )
                df[f'premium_prct_avg_{tf}'] = (
                    df.groupby('market')['premium_percent']
                    .transform(lambda x: x.rolling(label).mean())
                )
                df[f'volume_upbit_sum_{tf}'] = (
                    df.groupby('market')['volume_upbit']
                    .transform(lambda x: x.rolling(label).sum())
                )
                df[f'volume_binance_sum_{tf}'] = (
                    df.groupby('market')['volume_binance']
                    .transform(lambda x: x.rolling(label).sum())
                )
            
            self.features_data = df
            logger.info("Successfully calculated additional features.")
        
        except Exception as e:
            logger.error(f"Error in calculate_features: {e}", exc_info=True)
            raise

    def combine_data(self, upbit_file: str, binance_file: str) -> pd.DataFrame:
        """Объединение данных из Upbit и Binance с обработкой временных различий"""
        # Загрузка и валидация данных, ранее описанная логика здесь...
        
        # После объединения данных
        self.processed_data = combined_data  # предполагается, что объединение прошло успешно
        
        # Расчет дополнительных показателей
        self.calculate_features()
        return self.features_data

    # Остальные методы, такие как validate_input_data, send_to_web_service, и save_combined_data...

def main():
    try:
        combiner = DataCombiner()
        
        # Указание входных файлов
        upbit_file = "upbit_data.csv"
        binance_file = "binance_historical_data.csv"
        
        # Объединение и расчет дополнительных показателей
        combined_data = combiner.combine_data(upbit_file, binance_file)
        if not combined_data.empty:
            combiner.save_combined_data("combined_market_data_with_features.csv")
            
    except Exception as e:
        logger.error(f"Main execution error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()