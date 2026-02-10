"""
*********************************************************************************************
**  SCRIPT NAME  : Daniel_IncScd2Impln.py                                                   **
**  OBJECTIVE    : To integrate Incremental Load with SCD Type 2 for efficiently processing **
**                 data changes while preserving historical records.                        **
**  CREATED BY   : Daniel                                                                   **
**  CREATED DATE : 2025-12-30                                                               **
**  MODIFIED BY  : Daniel                                                                   **
**  MODIFIED DATE: 2025-12-30                                                               ** 
**********************************************************************************************
"""

"""
# *? PSEUDOCODE:
-------------------------------------------------
STEP 1: TRUNCATE the STAGE table
STEP 2: READ last processed dates from the CONTROL table
STEP 3: IDENTIFY incremental records from the SOURCE table
STEP 4: LOAD incremental records into the STAGE table
STEP 5: CLOSE existing records in the TARGET table (SCD Type 2)
STEP 6: INSERT new records into the TARGET table (SCD Type 2)
STEP 7: TARGET LOAD CONFIRMATION
 ! Once successfully loaded to the target table
STEP 8: UPDATE the CONTROL table with latest dates
STEP 9: LOG execution status
STEP 10: COMMIT transactions~
STEP 11: CLOSE database connection
-------------------------------------------------
"""
import mysql.connector
import logging
from datetime import datetime
from config.db_config import DB_CONFIG
from config.log_config import setup_logging

# ** -------------------------------------------------------
# ** LOG FILE SETUP (NEW FILE PER RUN)
# ** -------------------------------------------------------

SCRIPT_NAME = "Daniel_IncScd2Impln"
log_file_path = setup_logging(SCRIPT_NAME)

conn = None
cursor = None
target_success = False
stage_rows = 0

# ** -------------------------------------------------------
# ** HELPER FUNCTION FOR DATETIME DISPLAY
# ** -------------------------------------------------------
def format_row(row):
    formatted = []
    for col in row:
        if isinstance(col, datetime):
            formatted.append(col.strftime("%Y-%m-%d %H:%M:%S"))
        else:
            formatted.append(col)
    return tuple(formatted)

try:
    # ** -------------------------------------------------
    # ** DATABASE CONNECTION
    # ** -------------------------------------------------
    start_time = datetime.now()
    logging.info(f"{SCRIPT_NAME} STARTED AT {start_time}")
    print(f"\n{SCRIPT_NAME} STARTED\n")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    logging.info("Database connection established")

    # ** -------------------------------------------------
    # ** STEP 1: TRUNCATE STAGE
    # ** -------------------------------------------------
    logging.info("STEP 1: Starting stage table truncation")
    cursor.execute("TRUNCATE TABLE stage_customer")
    conn.commit()
    logging.info("STEP 1: Stage table truncated")

    # ** -------------------------------------------------
    # ** STEP 2: IDENTIFY & LOAD INCREMENTAL DATA
    # ** -------------------------------------------------
    logging.info("STEP 2: Starting incremental data identification and load")
    cursor.execute("""
        INSERT INTO stage_customer
        SELECT *
        FROM source_customer
        WHERE created_date > (
            SELECT max_created_date FROM control_table WHERE table_id='SOURCE'
        )
        OR modified_date > (
            SELECT max_modified_date FROM control_table WHERE table_id='SOURCE'
        )
    """)
    stage_rows = cursor.rowcount
    conn.commit()
    logging.info(
        f"STEP 2: Incremental load completed – {stage_rows} new/updated record(s) identified and loaded into STAGE"
    )

    # ** -------------------------------------------------
    # ** STEP 3: CLOSE OLD SCD2 RECORDS
    # ** -------------------------------------------------
    logging.info("STEP 3: Starting SCD2 close operation")
    if stage_rows > 0:
        cursor.execute("""
            UPDATE target_customer_scd2 t
            JOIN stage_customer s
              ON t.cust_id = s.cust_id
            SET
              t.end_date = s.modified_date,
              t.is_current = 0
            WHERE t.is_current = 1
              AND (
                  t.customer_name <> s.customer_name
               OR t.customer_phone <> s.customer_phone
              )
        """)
        conn.commit()
        logging.info("STEP 3: Old SCD2 records closed")
    else:
        logging.info("STEP 3 SKIPPED: No incremental data in STAGE")

    # ** -------------------------------------------------
    # ** STEP 4: INSERT NEW SCD2 RECORDS  (UPDATED)
    # ** -------------------------------------------------
    logging.info("STEP 4: Starting SCD2 insert operation")
    if stage_rows > 0:
        cursor.execute("""
            INSERT INTO target_customer_scd2 (
                cust_id,
                customer_name,
                customer_phone,
                start_date,
                end_date,
                is_current,
                load_date
            )
            SELECT
                s.cust_id,
                s.customer_name,
                s.customer_phone,
                s.modified_date,
                NULL,
                1,
                NOW()
            FROM stage_customer s
            LEFT JOIN target_customer_scd2 t
              ON s.cust_id = t.cust_id
             AND t.is_current = 1
            WHERE
                t.cust_id IS NULL
                OR (
                    t.customer_name <> s.customer_name
                 OR t.customer_phone <> s.customer_phone
                )
        """)
        conn.commit()
        logging.info("STEP 4: New SCD2 records inserted")
    else:
        logging.info("STEP 4 SKIPPED: No new or changed records")

    # ** -------------------------------------------------
    # ** STEP 5: TARGET LOAD CONFIRMATION
    # ** -------------------------------------------------
    logging.info("STEP 5: Verifying target load status")
    if stage_rows > 0:
        target_success = True
        logging.info("STEP 5: TARGET load successful")
    else:
        logging.info("STEP 5 SKIPPED: TARGET not modified")

    # ** -------------------------------------------------
    # ** STEP 6: UPDATE CONTROL TABLE
    # ** -------------------------------------------------
    logging.info("STEP 6: Starting control table update")
    if target_success and stage_rows > 0:
        cursor.execute("""
            UPDATE control_table
            SET
              max_created_date  = (SELECT MAX(created_date) FROM stage_customer),
              max_modified_date = (SELECT MAX(modified_date) FROM stage_customer)
            WHERE table_id='SOURCE'
        """)
        conn.commit()
        logging.info("STEP 6: CONTROL table updated")
    else:
        logging.info("STEP 6 SKIPPED: CONTROL table not updated")

    # ** -------------------------------------------------
    # ** STEP 7: JOB COMPLETION
    # ** -------------------------------------------------
    logging.info("STEP 7: Job execution completed successfully")

    # ** -------------------------------------------------
    # ** DATA VERIFICATION OUTPUT (CONSOLE)
    # ** -------------------------------------------------
    print("\n========== SOURCE TABLE ==========")
    cursor.execute("SELECT * FROM source_customer ORDER BY cust_id")
    for row in cursor.fetchall():
        print(format_row(row))

    print("\n========== STAGE TABLE ==========")
    cursor.execute("SELECT * FROM stage_customer ORDER BY cust_id")
    for row in cursor.fetchall():
        print(format_row(row))

    print("\n========== TARGET SCD2 TABLE ==========")
    cursor.execute("""
        SELECT cust_key, cust_id, customer_name, customer_phone,
               start_date, end_date, is_current, load_date
        FROM target_customer_scd2
        ORDER BY cust_id, cust_key
    """)
    for row in cursor.fetchall():
        print(format_row(row))

    print("\n========== CONTROL TABLE ==========")
    cursor.execute("SELECT * FROM control_table")
    for row in cursor.fetchall():
        print(format_row(row))

    end_time = datetime.now()
    logging.info(f"{SCRIPT_NAME} ENDED AT {end_time}")
    logging.info("-------------------------------------------------")
    print(f"\n{SCRIPT_NAME} COMPLETED SUCCESSFULLY\n")

except Exception as e:
    if conn:
        conn.rollback()
    logging.error(f"JOB FAILED: {e}")
    print(f"\nERROR: {e}\n")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
