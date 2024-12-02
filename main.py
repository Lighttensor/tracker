from web_server import app
import threading
from web_server import update_data

if __name__ == "__main__":
    # Start the background data update thread
    update_thread = threading.Thread(target=update_data, daemon=True)
    update_thread.start()
    
    # Run the Flask app
    app.run(debug=True, use_reloader=False)