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

# Track processing history to avoid redundant work
processing_history = {}

def clear_processing_history():
    """Clear the entire processing history"""
    global processing_history
    count = len(processing_history)
    processing_history.clear()
    logger.info(f"Cleared processing history ({count} entries)")
    return count

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/process', methods=['POST'])
def process():
    """Process meteorological data for a specific date and lake"""
    start_time = time.time()

    # Get parameters from request
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON data provided'}), 400

    date_str = data.get('date')
    lake = data.get('lake')

    if not date_str or not lake:
        return jsonify({'error': 'Missing required parameters: date or lake'}), 400

    logger.info(f"Processing request for date={date_str}, lake={lake}")

    try:
        # Parse date from ISO format
        date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        if date_obj.tzinfo is None:
            date_obj = date_obj.replace(tzinfo=timezone.utc)

        # Generate directory name based on date and lake
        dirname = f"{date_obj.strftime('%Y%m%d_%H')}{lake}"

        # Check if we've already processed this exact request
        cache_key = f"{date_str}_{lake}"
        if cache_key in processing_history:
            logger.info(f"Using cached result for {cache_key}")
            return jsonify({
                'success': True,
                'dirname': dirname,
                'file_path': f"./data/{dirname}/{dirname}_in.nc",
                'cached': True
            })

        # Process the data
        logger.info(f"Starting data processing for {dirname}")
        process_day(date_obj, lake)

        # Verify the file exists
        file_path = f"./data/{dirname}/{dirname}_in.nc"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Expected output file {file_path} was not created")

        # Record in processing history
        processing_history[cache_key] = {
            'processed_at': datetime.now().isoformat(),
            'file_path': file_path
        }

        # Remove old history items if there are more than 50
        if len(processing_history) > 50:
            oldest_key = sorted(processing_history.keys(),
                               key=lambda k: processing_history[k]['processed_at'])[0]
            processing_history.pop(oldest_key)

        processing_time = time.time() - start_time
        logger.info(f"Processing completed for {dirname} in {processing_time:.2f} seconds")

        # Return the path of the processed file
        return jsonify({
            'success': True,
            'dirname': dirname,
            'file_path': file_path,
            'processing_time_seconds': processing_time
        })

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'details': f"Error occurred during data processing"
        }), 500

@app.route('/download/<path:dirname>')
def download_file(dirname):
    """Download the processed NetCDF file"""
    try:
        # Sanitize dirname to prevent directory traversal
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
    """Get service status and processing history"""
    return jsonify({
        'status': 'running',
        'uptime': os.popen('uptime').read().strip(),
        'processed_count': len(processing_history),
        'recent_processing': list(processing_history.keys())[-10:] if processing_history else []
    })

@app.route('/cleanup', methods=['POST'])
def cleanup():
    """Clean up old data files to free disk space and sync processing history"""
    try:
        # Get path parameters
        data = request.get_json() or {}
        older_than_days = data.get('older_than_days', 7)

        deleted_count = 0
        data_dir = "./data"

        if not os.path.exists(data_dir):
            return jsonify({'message': 'No data directory found'}), 200

        current_time = time.time()

        for dirname in os.listdir(data_dir):
            dir_path = os.path.join(data_dir, dirname)
            if os.path.isdir(dir_path):
                # Check directory age
                dir_age_days = (current_time - os.path.getmtime(dir_path)) / (24 * 3600)

                if dir_age_days > older_than_days:
                    try:
                        shutil.rmtree(dir_path)
                        deleted_count += 1
                        logger.info(f"Deleted old directory: {dir_path} ({dir_age_days:.1f} days old)")
                    except Exception as e:
                        logger.error(f"Failed to delete {dir_path}: {str(e)}")

        # Clear processing history after cleanup
        removed_entries = clear_processing_history()
        logger.info(f"Cleared processing history cache of {removed_entries} entries")

        return jsonify({
            'success': True,
            'deleted_count': deleted_count,
            'removed_history_entries': removed_entries,
            'message': f"Deleted {deleted_count} directories older than {older_than_days} days and removed {removed_entries} stale history entries"
        })

    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

def scheduled_cleanup():
    """Function called by the scheduler to clean the data directory daily"""
    logger.info(f"Executing scheduled cleanup at {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    try:
        # Call cleanup with default 7 days retention
        response = cleanup()
        logger.info(f"Scheduled cleanup complete: {response.get_json()}")
    except Exception as e:
        logger.error(f"Scheduled cleanup failed: {str(e)}")
    logger.info("Daily cleanup complete - no restart needed")

def init_scheduler():
    """Set up the scheduler with jobs"""
    scheduler.init_app(app)

    # Add job to clean data directory daily at 06:00 UTC
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

if __name__ == '__main__':
    # Create data directory if it doesn't exist
    os.makedirs('./data', exist_ok=True)
    os.makedirs('./data/original', exist_ok=True)

    # Initialize the scheduler
    init_scheduler()

    logger.info("Starting process service on port 5001")
    app.run(host='0.0.0.0', port=5001)
