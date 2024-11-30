# web_server.py
from flask import Flask, render_template, request, jsonify
import pandas as pd
import threading
import time

app = Flask(__name__)
latest_data = None

@app.route('/update_data', methods=['POST'])
def update_data_endpoint():
    try:
        data = request.json.get('data')
        if data:
            global latest_data
            latest_data = pd.DataFrame(data)
            latest_data['candle_date_time_utc_x'] = pd.to_datetime(latest_data['candle_date_time_utc_x'])
            latest_data = latest_data.sort_values('candle_date_time_utc_x', ascending=False)
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
        display_data['candle_date_time_utc_x'] = display_data['candle_date_time_utc_x'].dt.strftime('%Y-%m-%d %H:%M')
        data = display_data.to_dict('records')
        return render_template('data.html', data=data)
    except Exception as e:
        print(f"Error in index route: {e}")
        return "Error processing data. Please try again."

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)