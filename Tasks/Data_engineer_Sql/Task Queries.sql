-- Write a query to find the latest customer address.Using the tables Sales and customerk,,
    Select customer_id,customer_address,date_loc
from (
    SELECT s.customer_id,c.customer_address,c.date_loc,
    ROW_NUMBER() Over(partition by s.customer_id 
                      order by c.date_loc DESC) AS rn
    from sales s
    join customer c on s.customer_id = c.customer_id
       )t 
where rn = 2


-- Create  a query to read the data from  sales_transaction with below requirement:
-- 1.To filter out records which has source as desktop
-- 2.to filter out if the records has source as  mainframe and desc start with 'DE'

select * from sales_transaction
where source != 'desktop' and source = 'mainframe' and `desc` LIKE "DE%"

-- Given the following table named A:
--  x
-- ------
-- 2
-- -2
--  4
-- -4
-- -3    
--  0
--  2
-- Write a single query to calculate the sum of all positive values of x and he sum of all negative values of x
SELECT 
    SUM(CASE WHEN x > 0 then x else 0 end) as positive_sum,
    SUM(CASE WHEN x < 0 then x else 0 end) as negative_sum
from A;

-- Output:
-- --------
 --       element | min | max
 --       A | 1 | 3
 --       A | 5 | 6
--        A | 8 | 9
--        B | 11 | 11
--        C | 13 | 15
SELECT  element ,min(sequence) as min , max(sequence)as max from (
    SELECT element , sequence , sequence - ROW_NUMBER() OVER(partition by element order by sequence) as diff from element_sequence
) t
group by element,diff
order by element

-- print customer who consume both product a,b
SELECT a.customer from customer_product a
join customer_product b on a.customer = b.customer
where a.product = 'a' and b.product = 'b'

-- Write a query to delete all users whose status is ‘inactive’ and have not logged in for the past  6months
delete from userslist
where status = 'inactive' and lastactive < DATE_SUB(NOW(),INTERVAL 6 MONTH)


-- You have a table Transactions with columns -TransactionID, AccountID, TransactionDate and Amount. Write a query to find all transactions that occured 3 months before
select * from transactions where transactiondate > Date_Sub(now(), INTERVAL 3 MONTH)

-- Write a query to display the name and email address of employees who have not been assigned to any project. 
select e.name , e.email from employees e
left join projects p on e.employeeid = p.employeeid 
where p.projectid IS NULL

