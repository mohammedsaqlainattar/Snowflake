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