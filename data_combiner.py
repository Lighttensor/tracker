# data_combiner.py
import pandas as pd
import numpy as np
import aiohttp
from datetime import datetime, timedelta

class DataCombiner:
    def __init__(self, server_url="http://localhost:5000"):
        self.server_url = server_url
        self.raw_data = pd.DataFrame()
        self.processed_data = pd.DataFrame()
        
    def combine_data(self, upbit_data, binance_data):
        """Combines data from Upbit and Binance exchanges"""
        # Convert to DataFrames
        upbit_df = pd.DataFrame(upbit_data)
        binance_df = pd.DataFrame(binance_data)
        
        # Basic data cleaning and preparation
        for df in [upbit_df, binance_df]:
            df['candle_date_time_utc'] = pd.to_datetime(df['candle_date_time_utc'])
            
        # Merge the dataframes
        self.raw_data = pd.merge(
            upbit_df,
            binance_df,
            on=['market', 'candle_date_time_utc'],
            suffixes=('_upbit', '_binance')
        )
        
        # Process the combined data
        self._process_data()
        
    def _process_data(self):
        """Processes raw data into required format with premium calculations"""
        df = self.raw_data.copy()
        
        # Basic columns
        processed = pd.DataFrame()
        processed['TimeFrame'] = '5min'
        processed['DateTime'] = df['candle_date_time_utc']
        processed['Coin'] = df['market']
        processed['Price @ Upbit'] = df['trade_price_upbit']
        processed['Price @ Binance'] = df['trade_price_binance']
        
        # Calculate Upbit Premium
        processed['Premium diff'] = processed['Price @ Upbit'] - processed['Price @ Binance']
        processed['Premium prct'] = (processed['Premium diff'] / processed['Price @ Binance']) * 100
        
        # Function to calculate rolling premium metrics
        def calculate_premium_metrics(group, window):
            metrics = pd.DataFrame()
            metrics['diff'] = group['Premium diff'].rolling(window=window).mean()
            metrics['prct'] = group['Premium prct'].rolling(window=window).mean()
            metrics['current_diff'] = group['Premium diff'] - metrics['diff']
            metrics['current_prct'] = group['Premium prct'] - metrics['prct']
            return metrics
        
        # Calculate premium metrics for different time periods
        for coin in processed['Coin'].unique():
            mask = processed['Coin'] == coin
            coin_data = processed[mask].sort_values('DateTime')
            
            # 1 Hour Premium (12 5-min candles)
            hour_metrics = calculate_premium_metrics(coin_data, 12)
            processed.loc[mask, 'Avg 1H Premium diff'] = hour_metrics['diff']
            processed.loc[mask, 'Avg 1H Premium prct'] = hour_metrics['prct']
            processed.loc[mask, 'Avg 1H Premium current diff'] = hour_metrics['current_diff']
            processed.loc[mask, 'Avg 1H Premium current prct'] = hour_metrics['current_prct']
            
            # 24 Hour Premium
            day_metrics = calculate_premium_metrics(coin_data, 288)
            processed.loc[mask, 'Avg 24H Premium diff'] = day_metrics['diff']
            processed.loc[mask, 'Avg 24H Premium prct'] = day_metrics['prct']
            processed.loc[mask, 'Avg 24H Premium current diff'] = day_metrics['current_diff']
            processed.loc[mask, 'Avg 24H Premium current prct'] = day_metrics['current_prct']
            
            # 1 Month Premium
            month_metrics = calculate_premium_metrics(coin_data, 8640)
            processed.loc[mask, 'Avg 1M Premium diff'] = month_metrics['diff']
            processed.loc[mask, 'Avg 1M Premium prct'] = month_metrics['prct']
            processed.loc[mask, 'Avg 1M Premium current diff'] = month_metrics['current_diff']
            processed.loc[mask, 'Avg 1M Premium current prct'] = month_metrics['current_prct']
        
        # Add volume data
        processed['Vol @ Upbit'] = df['candle_acc_trade_volume_upbit']
        processed['Vol @ Binance'] = df['candle_acc_trade_volume_binance']
        
        self.processed_data = processed
        
    def save_combined_data(self):
        """Saves combined data to CSV"""
        if not self.processed_data.empty:
            try:
                existing_data = pd.read_csv("combined_candles.csv")
                existing_data['DateTime'] = pd.to_datetime(existing_data['DateTime'])
                
                combined = pd.concat([existing_data, self.processed_data])
                combined = combined.drop_duplicates(
                    subset=['Coin', 'DateTime'],
                    keep='last'
                )
                combined = combined.sort_values(['DateTime', 'Coin'])
                
                combined.to_csv("combined_candles.csv", index=False)
                print(f"Saved combined data: {len(combined)} rows")
            except FileNotFoundError:
                self.processed_data.to_csv("combined_candles.csv", index=False)
                print(f"Created new file with {len(self.processed_data)} rows")
                
    async def send_to_web_service(self):
        """Sends processed data to web service"""
        if not self.processed_data.empty:
            try:
                data_to_send = self.processed_data.copy()
                data_to_send['DateTime'] = data_to_send['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                data_dict = data_to_send.to_dict('records')
                
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.server_url}/update_data",
                        json={"data": data_dict}
                    ) as response:
                        if response.status == 200:
                            print(f"Data successfully sent to web service: {datetime.now()}")
                        else:
                            print(f"Error sending data: {response.status}")
            except Exception as e:
                print(f"Error sending data to web service: {e}")
                
    def get_processed_data(self):
        """Returns the processed DataFrame"""
        return self.processed_data