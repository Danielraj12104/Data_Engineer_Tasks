-- Create sales TABLE
CREATE TABLE sales (
    prod_id INT PRIMARY KEY,
    prod_desc VARCHAR(50),
    qty INT,
    customer_id VARCHAR(10)
);

-- Create sales_hist TABLE
CREATE TABLE sales_hist (
    prod_id INT,
    prod_desc VARCHAR(50),
    qty INT,
    customer_id VARCHAR(10)
);

-- Create customer TABLE
CREATE TABLE customer (
    customer_id VARCHAR(10),
    customer_address VARCHAR(50),
    date_loc DATE
);

-- Create sales_transaction TABLE
CREATE TABLE sales_transaction (
    sale_id INT PRIMARY KEY,
    source VARCHAR(20),
    `desc` VARCHAR(50),
    amount INT,
    sale_date DATE
);

CREATE TABLE A (
    x INT
);


CREATE TABLE element_sequence (
    element  CHAR(1),
    sequence INT
);


CREATE TABLE customer_product (
    sno INT,
    customer INT,
    product CHAR(1)
);


CREATE TABLE userslist (
    userid INT PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100),
    status VARCHAR(20),
    lastactive DATE
);


CREATE TABLE transactions (
    transactionid INT PRIMARY KEY,
    accountid INT,
    transactiondate DATE,
    amount DECIMAL(10,2)
);


CREATE TABLE employees (
    employeeid INT PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100)
);

CREATE TABLE projects (
    projectid INT PRIMARY KEY,
    employeeid INT,
    projectname VARCHAR(50)
);

