--1- write a sql query to find top 3 products in each category by highest rolling 3 months total sales for Jan 2020.

WITH cte AS (
SELECT
    category,
    product_id,
    YEAR(order_date) AS YYYY,
    MONTH(order_date) AS MM,
    SUM(sales) AS sales
FROM orders
WHERE YEAR(order_date) IN (2019,2020)
GROUP BY category, product_id, YEAR(order_date), MONTH(order_date)
),
cte2 AS (
SELECT
    *,
    SUM(sales) OVER(PARTITION BY category,product_id ORDER BY YYYY,MM ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_month_sales
FROM cte
ORDER BY category, product_id, YYYY, MM
),
cte3 AS (
SELECT *, RANK() OVER(PARTITION BY category ORDER BY rolling_3_month_sales DESC) AS rn
FROM cte2 WHERE YYYY=2020 AND MM=1
)
SELECT * FROM cte3 WHERE rn<=3;

---------------------------------------------------

--2- write a query to find products for which month over month sales has never declined.

WITH cte AS (
SELECT
    product_id,
    YEAR(order_date) AS YYYY,
    MONTH(order_date) AS MM,
    SUM(sales) AS sales
FROM orders
GROUP BY product_id, YEAR(order_date), MONTH(order_date)
),
cte2 AS (
SELECT
    *,
    LAG(sales,1) OVER(PARTITION BY product_id ORDER BY YYYY,MM) AS prior_month_sales,
    sales - LAG(sales,1) OVER(PARTITION BY product_id ORDER BY YYYY,MM) AS growth
FROM cte
),
cte3 AS (
SELECT
    *,
    (CASE WHEN growth>=0 THEN 1 ELSE 0 END) AS flag
FROM cte2
WHERE growth IS NOT NULL
)
SELECT
    product_id,
    COUNT(*) AS no_of_months,
    SUM(flag) AS non_negative_growth_periods
FROM cte3
GROUP BY product_id
HAVING COUNT(*)=SUM(flag);

------------------------------------------------------

--3- write a query to find month wise sales for each category for months where sales is more than the combined sales of previous 2 months for that category.

WITH cte AS (
SELECT
    category,
    YEAR(order_date) AS YYYY,
    MONTH(order_date) AS MM,
    SUM(sales) AS sales
FROM orders
GROUP BY category, YEAR(order_date), MONTH(order_date)
),
cte2 AS (
SELECT
    *,
    SUM(sales) OVER(PARTITION BY category ORDER BY YYYY,MM ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_2M_sales
FROM cte
)
SELECT * FROM cte2 WHERE sales > prev_2M_sales;

--------------------------------------------------

--4- write a user defined functions  which takes 2 input parameters of DATE data type. 
-- The function should return no of business days between the 2 dates.
-- note -> if any of the 2 input dates are falling on saturday or sunday then function should use immediate Monday 
-- date for calculation

-- example if we pass dates as 2024-11-30 and 2024-12-05..then it should calculate business days 
-- between 2024-12-02 and 2024-12-05



-- Below solution uses Snowflake's syntax:

CREATE OR REPLACE FUNCTION fn_business_days(start_date DATE, end_date DATE)
RETURNS INT
AS
$$
DECLARE
    adj_start DATE;
    adj_end DATE;
    start_dow INT;
    end_dow INT;
BEGIN
    start_dow := DATE_PART(WEEKDAY, start_date);
    end_dow := DATE_PART(WEEKDAY, end_date);

    IF (start_dow = 0) THEN
        adj_start := DATEADD(DAY, 1, start_date);
    ELSEIF (start_dow = 6) THEN
        adj_start := DATEADD(DAY, 2, start_date);
    ELSE
        adj_start := start_date;
    END IF;

    IF (end_dow = 0) THEN
        adj_end := DATEADD(DAY, 1, end_date);
    ELSEIF (end_dow = 6) THEN
        adj_end := DATEADD(DAY, 2, end_date);
    ELSE
        adj_end := end_date;
    END IF;

    RETURN DATEDIFF(DAY, adj_start, adj_end)
           - 2 * DATEDIFF(WEEK, adj_start, adj_end);
END;
$$

-- MSSQL Solution:

CREATE FUNCTION dbo.GetBusinessDays (
    @start_date DATE,
    @end_date DATE
)
RETURNS INT
AS
BEGIN
    -- Adjust the start date to the next Monday if it falls on Saturday or Sunday
    SET @start_date = CASE 
        WHEN DATENAME(WEEKDAY, @start_date) = 'Saturday' THEN DATEADD(DAY, 2, @start_date)
        WHEN DATENAME(WEEKDAY, @start_date) = 'Sunday' THEN DATEADD(DAY, 1, @start_date)
        ELSE @start_date
    END;

    -- Adjust the end date to the next Monday if it falls on Saturday or Sunday
    SET @end_date = CASE 
        WHEN DATENAME(WEEKDAY, @end_date) = 'Saturday' THEN DATEADD(DAY, 2, @end_date)
        WHEN DATENAME(WEEKDAY, @end_date) = 'Sunday' THEN DATEADD(DAY, 1, @end_date)
        ELSE @end_date
    END;

    RETURN 
		(select DATEDIFF(DAY,@start_date,@end_date) - 2 * DATEDIFF(week,@start_date,@end_date))
END;

----------