"""
******************************************************************************************************
**  SCRIPT NAME  : py_incremental_load.py                                                            *
**  PURPOSE      : To automate incremental data loading by processing only new and                   *
**                 modified records while ensuring data consistency using a control table.           *
**  CREATED BY   : Daniel                                                                            *
**  CREATED DATE : 2025-12-29                                                                        *
**  MODIFIED BY  : Daniel                                                                            *
**  MODIFIED DATE: 2025-12-30                                                                        *
******************************************************************************************************
"""

"""
? PSEUDOCODE:
-------------------------------------------------
    STEP 1: TRUNCATING the STAGE table
    STEP 2: READING last processed dates from the CONTROL table
    STEP 3: IDENTIFYING incremental records from the SOURCE table
    STEP 4: LOADING incremental data into the STAGE table
    STEP 5: UPDATING existing records in the TARGET table
    STEP 6: INSERTING new records into the TARGET table
    STEP 7: TARGET LOAD CONFIRMATION
    STEP 8: UPDATING the CONTROL table
    STEP 9: LOGGING execution status
    STEP 10: CLOSING database connection
-------------------------------------------------
"""

import mysql.connector
import logging
from datetime import datetime
import os
from config import DB_CONFIG

# ** -------------------------------------------------
# ** LOG FILE SETUP
# ** -------------------------------------------------
SCRIPT_NAME = "py_incremental_load"
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = f"{LOG_DIR}/{SCRIPT_NAME}_{timestamp}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

conn = None
cursor = None
target_load_success = False

try:
    # ** -------------------------------------------------
    # ** DATABASE CONNECTION
    # ** -------------------------------------------------
    start_time = datetime.now()
    logging.info(f"{SCRIPT_NAME} STARTED AT {start_time}")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    logging.info("Database connection established")

    print("\nIncremental load started\n")

    # ** -------------------------------------------------
    # ** STEP 1: TRUNCATE STAGE
    # ** -------------------------------------------------
    logging.info("STEP 1: Starting stage table truncation")
    cursor.execute("TRUNCATE TABLE stage2")
    conn.commit()
    logging.info("STEP 1: Stage table truncated")
    print("STEP 1: STAGE table truncated")

    # ** -------------------------------------------------
    # ** STEP 2: SOURCE → STAGE (INCREMENTAL)
    # ** -------------------------------------------------
    logging.info("STEP 2: Starting incremental data identification and load")

    cursor.execute("""
        INSERT INTO stage2
        SELECT *
        FROM source2
        WHERE created_date > (
            SELECT max_created_date FROM control WHERE table_id='SOURCE'
        )
        OR modified_date > (
            SELECT max_modified_date FROM control WHERE table_id='SOURCE'
        )
    """)
    rows_stage = cursor.rowcount
    conn.commit()

    logging.info(f"STEP 2: Incremental rows loaded into stage = {rows_stage}")
    print(f"STEP 2: Rows loaded into STAGE = {rows_stage}")

    # ** -------------------------------------------------
    # ** STEP 3: UPDATE TARGET
    # ** -------------------------------------------------
    logging.info("STEP 3: Starting target update operation")

    if rows_stage > 0:
        cursor.execute("""
            UPDATE target t
            JOIN stage2 s ON t.cust_id = s.cust_id
            SET
                t.customer_name  = s.customer_name,
                t.customer_phone = s.customer_phone,
                t.modified_date  = s.modified_date
        """)
        rows_updated = cursor.rowcount
        conn.commit()

        logging.info(f"STEP 3: Target updated ({rows_updated} rows)")
        print(f"STEP 3: Rows UPDATED in TARGET = {rows_updated}")
    else:
        rows_updated = 0
        logging.info("STEP 3 SKIPPED: No incremental data in STAGE")
        print("STEP 3: SKIPPED (No data in STAGE)")

    # ** -------------------------------------------------
    # ** STEP 4: INSERT NEW RECORDS INTO TARGET
    # ** -------------------------------------------------
    logging.info("STEP 4: Starting target insert operation")

    if rows_stage > 0:
        cursor.execute("""
            INSERT INTO target (cust_id, customer_name, customer_phone, created_date, modified_date)
            SELECT
                s.cust_id,
                s.customer_name,
                s.customer_phone,
                s.created_date,
                s.modified_date
            FROM stage2 s
            LEFT JOIN target t ON s.cust_id = t.cust_id
            WHERE t.cust_id IS NULL
        """)
        rows_inserted = cursor.rowcount
        conn.commit()

        logging.info(f"STEP 4: Target insert completed ({rows_inserted} rows)")
        print(f"STEP 4: Rows INSERTED into TARGET = {rows_inserted}")
    else:
        rows_inserted = 0
        logging.info("STEP 4 SKIPPED: No new or changed records")
        print("STEP 4: SKIPPED (No inserts)")

    # ! -------------------------------------------------
    # ! TARGET LOAD CONFIRMATION
    # !-------------------------------------------------
    logging.info("STEP 5: Verifying target load status")

    if rows_updated > 0 or rows_inserted > 0:
        target_load_success = True
        logging.info("STEP 5: TARGET load successful")
        print("TARGET TABLE LOAD STATUS: SUCCESSFUL")
    else:
        logging.info("STEP 5 SKIPPED: TARGET not modified")
        print("TARGET TABLE LOAD STATUS: NO CHANGE")

    # ** -------------------------------------------------
    # ** STEP 5: UPDATE CONTROL TABLE
    # ** -------------------------------------------------
    logging.info("STEP 6: Starting control table update")

    if target_load_success:
        cursor.execute("""
            UPDATE control
            SET
                max_created_date  = (SELECT MAX(created_date) FROM stage2),
                max_modified_date = (SELECT MAX(modified_date) FROM stage2)
            WHERE table_id = 'SOURCE'
        """)
        conn.commit()
        logging.info("STEP 6: Control table updated")
        print("STEP 5: CONTROL table updated")
    else:
        logging.info("STEP 6 SKIPPED: CONTROL table not updated")
        print("STEP 5: CONTROL table NOT updated")

    # *? -------------------------------------------------
    # *? FINAL OUTPUT TO CONSOLE
    # *? -------------------------------------------------
    print("\n========== FINAL SOURCE TABLE ==========")
    cursor.execute("SELECT * FROM source2 ORDER BY cust_id")
    for row in cursor.fetchall():
        print(row)

    print("\n========== FINAL STAGE TABLE ==========")
    cursor.execute("SELECT * FROM stage2 ORDER BY cust_id")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(row)
    else:
        print("STAGE table is empty")

    print("\n========== FINAL TARGET TABLE ==========")
    cursor.execute("SELECT * FROM target ORDER BY cust_id")
    for row in cursor.fetchall():
        print(row)

    print("\n========== FINAL CONTROL TABLE ==========")
    cursor.execute("SELECT * FROM control")
    for row in cursor.fetchall():
        print(row)

    end_time = datetime.now()
    logging.info("STEP 7: Job execution completed successfully")
    logging.info(f"{SCRIPT_NAME} ENDED AT {end_time}")
    logging.info("-------------------------------------------------")

    print("\nIncremental load completed successfully\n")

except Exception as e:
    if conn:
        conn.rollback()
    logging.error(f"Incremental load failed: {e}")
    print(f"\nERROR: {e}\n")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
