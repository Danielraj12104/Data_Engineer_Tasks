import logging
import os
from datetime import datetime

def setup_logging(script_name):
    LOG_DIR = "logs"

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(LOG_DIR, f"{script_name}_{timestamp}.log")

    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return log_file_path
