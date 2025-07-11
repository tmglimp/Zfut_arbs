import subprocess
import time
import logging
import os

LOG_FILE = "application.log"
SCRIPT_PATH = "man.py"
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/XXX/YYY/ZZZ"

# how often to scan logs
CHECK_INTERVAL = 2
# restart if no MktData Upate/API Depth Pull seen in this many seconds
MAX_IDLE_SECONDS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("watchdog.log"),
        logging.StreamHandler()
    ]
)

def last_log_activity_time(keywords=["Market data updated", "API depth pull", "Fetching market data for futures contracts"], logfile=LOG_FILE):
    try:
        with open(logfile, "r") as f:
            lines = reversed(f.readlines())
            for line in lines:
                if any(kw in line for kw in keywords):
                    timestamp = os.path.getmtime(logfile)
                    return timestamp
    except Exception as e:
        logging.warning(f"Error checking log activity: {e}")
    return 0

def run_app():
    while True:
        logging.info("Launching subprocess...")
        process = subprocess.Popen(["python", SCRIPT_PATH])

        last_activity = time.time()

        try:
            while True:
                time.sleep(CHECK_INTERVAL)

                # Check for inactivity
                last_seen = last_log_activity_time()
                if last_seen > 0:
                    last_activity = last_seen

                if time.time() - last_activity > MAX_IDLE_SECONDS:
                    logging.warning("No Futures contract updates in log. Restarting man.py...")
                    process.kill()
                    break

                # Check if app has exited
                if process.poll() is not None:
                    exit_code = process.returncode
                    logging.info(f"App exited with code {exit_code}")

                    break

        except Exception as e:
            logging.error(f"Exception in watchdog loop: {e}")
            process.kill()

        logging.info("Restarting in 1 second...")
        time.sleep(1)

if __name__ == "__main__":
    run_app()
