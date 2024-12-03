from flask import Flask, render_template, request, jsonify
import pandas as pd
import threading
import time
from datetime import datetime

app = Flask(__name__)
latest_data = None

@app.route('/update_data', methods=['POST'])
def update_data_endpoint():
    try:
        data = request.json.get('data')
        if data:
            global latest_data
            latest_data = pd.DataFrame(data)
            print(latest_data)
            latest_data['DateTime'] = pd.to_datetime(latest_data['candle_date_time_utc_x'])
            latest_data = latest_data.sort_values('DateTime', ascending=False)
            return jsonify({"status": "success"})
        return jsonify({"status": "error", "message": "No data received"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/')
def index():
    if latest_data is None:
        return "Loading data... Please refresh in a few moments."
    
    try:
        display_data = latest_data.copy()
        display_data['DateTime'] = display_data['DateTime'].dt.strftime('%Y-%m-%d %H:%M')
        
        # Округляем числовые колонки для лучшего отображения
        numeric_columns = [
            'Price @ Upbit', 'Price @ Binance', 
        ]
        
        for col in numeric_columns:
            if col in display_data.columns:
                display_data[col] = display_data[col].round(7)

        data = display_data.to_dict('records')
        return render_template('data.html', 
                             data=data, 
                             last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    except Exception as e:
        print(f"Error in index route: {e}")
        return f"Error processing data: {str(e)}"

if __name__ == "__main__":
    app.run(host='localhost', port=5000, debug=True)