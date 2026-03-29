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