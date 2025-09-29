import schedule
import time
import logging
from script import run_stock_job
from datetime import datetime

# === Configure logging ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scheduler.log"),
        logging.StreamHandler()  # Also print to console
    ]
)
logger = logging.getLogger(__name__)

def basic_job():
    """Simple heartbeat job to confirm scheduler is alive."""
    logger.info("Heartbeat: Scheduler is running")

def safe_run_stock_job():
    """Wrapper to run stock job with error handling."""
    try:
        logger.info("Starting stock data job...")
        run_stock_job()
        logger.info("Stock data job completed successfully")
    except Exception as e:
        logger.error(f"Stock job failed with error: {e}", exc_info=True)

# === Schedule jobs ===
schedule.every().minute.do(basic_job)
schedule.every().minute.do(safe_run_stock_job)


# === Main loop ===
if __name__ == "__main__":
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        # The while loop that keeps the script running
        # Every second it checks if it is the time to run a scheduled job; if yes -> run it
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.critical(f"Scheduler crashed: {e}", exc_info=True)

# To monitor logs in real-time, run in another terminal:
# tail -f scheduler.log
# Stop monitoring and/or the scheduler with Ctrl+C