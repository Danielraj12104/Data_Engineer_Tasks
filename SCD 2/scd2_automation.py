import mysql.connector
from mysql.connector import Error
from datetime import datetime

LOG_FILE = "scd2_process.log"

def log(message, level="SUCCESS"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"{timestamp} [{level}] {message}"
    print(log_msg)
    with open(LOG_FILE, "a") as f:
        f.write(log_msg + "\n")

try:
    # 🔹 Connect to MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root123"
    )
    cursor = conn.cursor()
    log("Connected to MySQL")

    # 🔹 Step 1: Create database
    cursor.execute("CREATE DATABASE IF NOT EXISTS scd21;")
    cursor.execute("USE scd21;")
    log("Database created and selected")

    # 🔹 Step 2: Drop tables
    cursor.execute("DROP TABLE IF EXISTS CUST_SRC;")
    cursor.execute("DROP TABLE IF EXISTS SCD2_CUST_DM;")
    log("Old tables dropped")

    # 🔹 Step 3: Create source table
    cursor.execute("""
        CREATE TABLE CUST_SRC (
            cust_id INT PRIMARY KEY,
            phone_no VARCHAR(15),
            created_dt DATE,
            modified_dt DATE
        );
    """)
    log("CUST_SRC table created")

    # 🔹 Step 4: Create SCD2 table
    cursor.execute("""
        CREATE TABLE SCD2_CUST_DM (
            cust_key INT AUTO_INCREMENT PRIMARY KEY,
            cust_id INT,
            phone_no VARCHAR(15),
            start_date DATE,
            end_date DATE,
            created_date DATE,
            modified_date DATE,
            flag INT,
            load_dt DATE
        );
    """)
    log("SCD2_CUST_DM table created")

    # 🔹 Step 5: Load source data
    cursor.execute("""
        INSERT INTO CUST_SRC VALUES
        (101, '9999999999', CURDATE(), CURDATE()),
        (102, '8888888888', CURDATE(), CURDATE()),
        (103, '7777777777', CURDATE(), CURDATE()),
        (104, '6666666666', CURDATE(), CURDATE()),
        (105, '5555555555', CURDATE(), CURDATE()),
        (106, '9876542026', CURDATE(), CURDATE());
    """)
    conn.commit()
    log("Source data inserted")

    # 🔹 Step 6: Initial SCD2 load
    cursor.execute("""
        INSERT INTO SCD2_CUST_DM (
            cust_id, phone_no, start_date, end_date,
            created_date, modified_date, flag, load_dt
        )
        SELECT
            cust_id, phone_no, created_dt, NULL,
            created_dt, modified_dt, 1, CURDATE()
        FROM CUST_SRC;
    """)
    conn.commit()
    log("Initial SCD2 load completed")

    # 🔹 Step 7: Simulate source change
    cursor.execute("""
        UPDATE CUST_SRC
        SET phone_no = '1234562026',
            modified_dt = CURDATE()
        WHERE cust_id = 101;
    """)
    conn.commit()
    log("Source change simulated")

    # 🔹 Step 8: Close old SCD2 record
    cursor.execute("""
        UPDATE SCD2_CUST_DM d
        JOIN CUST_SRC s
          ON d.cust_id = s.cust_id
        SET d.end_date = s.modified_dt,
            d.flag = 0
        WHERE d.flag = 1
          AND d.phone_no <> s.phone_no;
    """)
    conn.commit()
    log("Old SCD2 record closed")

    # 🔹 Step 9: Insert new SCD2 version
    cursor.execute("""
        INSERT INTO SCD2_CUST_DM (
            cust_id, phone_no, start_date, end_date,
            created_date, modified_date, flag, load_dt
        )
        SELECT
            s.cust_id, s.phone_no, s.modified_dt, NULL,
            s.created_dt, s.modified_dt, 1, CURDATE()
        FROM CUST_SRC s
        JOIN SCD2_CUST_DM d
          ON s.cust_id = d.cust_id
        WHERE d.flag = 0
          AND d.end_date = s.modified_dt;
    """)
    conn.commit()
    log("New SCD2 version inserted")

    # 🔹 Step 10: Final result
    cursor.execute("SELECT * FROM SCD2_CUST_DM ORDER BY cust_id, cust_key;")
    rows = cursor.fetchall()
    log("Final SCD2 table fetched")

    for row in rows:
        print(row)

    log("SCD2 automation completed successfully")

except Error as e:
    log(str(e), level="ERROR")

finally:
    if conn.is_connected():
        cursor.close()
        conn.close()
        log("Connection closed")
