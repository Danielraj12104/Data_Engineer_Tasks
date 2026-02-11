-- Insert into sales
INSERT INTO sales VALUES
(1, 'Vodafone', 10, 'A10256'),
(2, 'Apple', 15, 'A20561'),
(3, 'Samsung', 20, 'B10231'),
(4, 'Redmi', 25, 'C10456');

-- Insert into sales_hist
INSERT INTO sales_hist VALUES
(5, 'one plus', 5, 'A10256'),
(6, 'Vivo', 15, 'A20561'),
(7, 'Samsung', 20, 'B10231'),
(8, 'oppo', 25, 'C10456');

-- Insert into customer
INSERT INTO customer VALUES
('A10256', 'Los Angeles', '2024-01-01'),
('A10256', 'San Diego',  '2020-01-04'),
('A20561', 'San Diego',  '2020-01-04'),
('A20561', 'Coronado',  '2024-01-01'),
('B10231', 'Long Beach','2020-01-04'),
('B10231', 'Palm Desert','2024-01-01'),
('B10231', 'Los Angeles','2023-01-04'),
('C10456', 'Long Beach','2020-01-04'),
('C10456', 'Palm Desert','2024-01-01');

-- Insert into sales_transaction
INSERT INTO sales_transaction VALUES
(1,  'mainframe', 'DS1234',      500, '2025-01-01'),
(2,  'mainframe', 'PS5678',      300, '2025-01-02'),
(3,  'mobile',    'DS9876',      150, '2025-01-03'),
(4,  'web',       'OnlineSale', 1200, '2025-01-04'),
(5,  'mainframe', 'PS4321',      450, '2025-01-05'),
(6,  'mobile',    'MobileSale',  200, '2025-01-06'),
(7,  'web',       'DS8901',      350, '2025-01-07'),
(8,  'desktop',   'DesktopSale',800, '2025-01-08'),
(9,  'mainframe', 'DS1235',      600, '2025-01-09'),
(10, 'mobile',    'PS8765',      400, '2025-01-10'),
(11, 'web',       'WebSale',    1100, '2025-01-11'),
(12, 'web',       'DS6789',      900, '2025-01-12'),
(13, 'desktop',   'PCSale',      700, '2025-01-13'),
(14, 'mobile',    'MobileDiscount',180,'2025-01-14'),
(15, 'mainframe', 'PS5432',      250, '2025-01-15'),
(16, 'mobile',    'DS5432',      650, '2025-01-16'),
(17, 'web',       'DS2345',      500, '2025-01-17'),
(18, 'desktop',   'TabletSale',  300, '2025-01-18'),
(19, 'mobile',    'tab',         220, '2025-01-19'),
(20, 'web',       'DS1111',      150, '2025-01-20'),
(21, 'mainframe', 'DE1235',      600, '2025-01-09'),
(22, 'mainframe', NULL,          100, '2025-01-09'),
(23, 'mainframe', 'DE1235',      200, '2025-01-10');

INSERT INTO A (x) VALUES
(2),
(-2),
(4),
(-4),
(-3),
(0),
(2);

INSERT INTO element_sequence (element, sequence) VALUES
('A', 1),
('A', 2),
('A', 3),
('A', 5),
('A', 6),
('A', 8),
('A', 9),
('B', 11),
('C', 13),
('C', 14),
('C', 15);


INSERT INTO customer_product (sno, customer, product) VALUES
(1,  1, 'a'),
(2,  1, 'b'),
(3,  1, 'd'),
(4,  1, 'e'),
(5,  2, 'a'),
(6,  2, 'b'),
(7,  2, 'c'),
(8,  2, 'd'),
(9,  3, 'a'),
(10, 3, 'c'),
(11, 3, 'e');

INSERT INTO userslist (userid, username, email, status, lastactive) VALUES
(1, 'alice',   'alice@gmail.com',   'active',   '2025-07-01'),
(2, 'bob',     'bob@gmail.com',     'inactive', '2024-10-01'),
(3, 'charlie', 'charlie@gmail.com', 'inactive', '2024-03-15'),
(4, 'david',   'david@gmail.com',   'active',   '2025-08-10'),
(5, 'emma',    'emma@gmail.com',    'inactive', '2024-01-20');


INSERT INTO transactions VALUES
(1, 101, '2025-07-10', 500.00),
(2, 101, '2025-10-05', 1200.00),
(3, 102, '2025-09-15', 750.00),
(4, 103, '2025-06-20', 300.00),
(5, 104, '2025-04-01', 900.00);


INSERT INTO employees VALUES
(1, 'Alice', 'alice@gmail.com'),
(2, 'Bob', 'bob@gmail.com'),
(3, 'Charlie', 'charlie@gmail.com'),
(4, 'David', 'david@gmail.com');


INSERT INTO projects VALUES
(101, 1, 'CRM System'),
(102, 2, 'Data Pipeline');

