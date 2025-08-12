from flask import Flask, request, jsonify, send_file
from flask_apscheduler import APScheduler
import pytz
import os
import time
import shutil
import logging
from datetime import datetime, timezone
from get_data import process_day

app = Flask(__name__)
scheduler = APScheduler()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('process_service.log')
    ]
)
logger = logging.getLogger('process_service')

def _perform_cleanup(older_than_days=7):
    """
    Core logic to clean up old data files.
    This function is decoupled from the Flask request context.
    """
    deleted_count = 0
    data_dir = "./data"

    if not os.path.exists(data_dir):
        logger.info("No data directory found, nothing to clean up.")
        return 0

    current_time = time.time()

    for dirname in os.listdir(data_dir):
        dir_path = os.path.join(data_dir, dirname)
        if os.path.isdir(dir_path):
            try:
                # Check directory age
                dir_age_seconds = current_time - os.path.getmtime(dir_path)
                dir_age_days = dir_age_seconds / (24 * 3600)

                if dir_age_days > older_than_days:
                    shutil.rmtree(dir_path)
                    deleted_count += 1
                    logger.info(f"Deleted old directory: {dir_path} ({dir_age_days:.1f} days old)")
            except Exception as e:
                logger.error(f"Failed to process or delete {dir_path}: {str(e)}")

    return deleted_count

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/process', methods=['POST'])
def process():
    """Process meteorological data for a specific date and lake"""
    start_time = time.time()

    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON data provided'}), 400

    date_str = data.get('date')
    lake = data.get('lake')

    if not date_str or not lake:
        return jsonify({'error': 'Missing required parameters: date or lake'}), 400

    logger.info(f"Processing request for date={date_str}, lake={lake}")

    try:
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        if date_obj.tzinfo is None:
            date_obj = date_obj.replace(tzinfo=timezone.utc)

        dirname = f"{date_obj.strftime('%Y%m%d_%H')}{lake}"
        file_path = f"./data/{dirname}/{dirname}_in.nc"

        if os.path.exists(file_path):
            logger.info(f"File already exists, skipping processing: {file_path}")
            return jsonify({
                'success': True,
                'dirname': dirname,
                'file_path': file_path,
                'cached': True
            })

        logger.info(f"Starting data processing for {dirname}")
        process_day(date_obj, lake)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Expected output file {file_path} was not created")

        processing_time = time.time() - start_time
        logger.info(f"Processing completed for {dirname} in {processing_time:.2f} seconds")

        return jsonify({
            'success': True,
            'dirname': dirname,
            'file_path': file_path,
            'processing_time_seconds': processing_time
        })

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        return jsonify({'error': str(e), 'details': "Error occurred during data processing"}), 500

@app.route('/download/<path:dirname>')
def download_file(dirname):
    """Download the processed NetCDF file"""
    try:
        dirname = os.path.basename(dirname)
        file_path = f"./data/{dirname}/{dirname}_in.nc"
        logger.info(f"Download request for {file_path}")

        if os.path.exists(file_path):
            return send_file(
                file_path,
                mimetype='application/x-netcdf',
                as_attachment=True,
                download_name=f"{dirname}_in.nc"
            )
        else:
            logger.warning(f"File not found: {file_path}")
            return jsonify({'error': 'File not found'}), 404

    except Exception as e:
        logger.error(f"Error serving file: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/status', methods=['GET'])
def status():
    """Get service status"""
    return jsonify({
        'status': 'running',
        'uptime': os.popen('uptime').read().strip(),
    })

@app.route('/cleanup', methods=['POST'])
def cleanup():
    """Endpoint to manually trigger cleanup of old data files."""
    try:
        data = request.get_json() or {}
        older_than_days = data.get('older_than_days', 7)

        deleted_count = _perform_cleanup(older_than_days)

        return jsonify({
            'success': True,
            'deleted_count': deleted_count,
            'message': f"Deleted {deleted_count} directories older than {older_than_days} days."
        })

    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

def scheduled_cleanup():
    """Function called by the scheduler to clean the data directory daily."""
    logger.info(f"Executing scheduled cleanup at {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    try:
        # Call the core cleanup logic with default 7 days retention
        deleted_count = _perform_cleanup(older_than_days=7)
        logger.info(f"Scheduled cleanup complete: Deleted {deleted_count} directories.")
    except Exception as e:
        logger.error(f"Scheduled cleanup failed: {str(e)}", exc_info=True)
    logger.info("Daily cleanup job finished.")

def init_scheduler():
    """Set up and start the scheduler with jobs."""
    scheduler.init_app(app)
    scheduler.add_job(
        id='scheduled_cleanup',
        func=scheduled_cleanup,
        trigger='cron',
        hour=6,
        minute=0,
        timezone=pytz.UTC
    )
    scheduler.start()
    logger.info("Scheduler started - Daily cleanup scheduled for 06:00 UTC")

# Initialize and start the scheduler when the application module is loaded.
# This ensures it runs in production environments (e.g., Gunicorn).
init_scheduler()

if __name__ == '__main__':
    # This block runs only when the script is executed directly (e.g., `python process_service.py`)
    # It's intended for local development.
    os.makedirs('./data', exist_ok=True)
    os.makedirs('./data/original', exist_ok=True)

    logger.info("Starting process service in development mode on port 5001")
    app.run(host='0.0.0.0', port=5001)
