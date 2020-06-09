#Question 1:

SELECT
store.store_label,
order.datetime_shipped AS date_shipped,
rev_items.product_type_label,
rev_items.count_of_items

FROM dbdo.dim_store store 
LEFT JOIN dbdo.fact_order order ON store.store_id = order.order_id
LEFT JOIN 

--> count of successfully bought items per order_id and product_label, excluding upsells
	(
	SELECT 
	items.order_id,
	type.product_type_label,
	COUNT(items.product_id) AS count_of_items

	FROM dbdo.fact_order_line items
	LEFT JOIN dbdo.dim_product_type type ON type.product_type_id = items.product_type_id
	LEFT JOIN dbdo.dim_order_line_statuscode status ON status.dim_order = items.statuscode

	WHERE 1:1
	AND type.product_type_id != 4 --> excludes upsells
	AND status.dim_order = 120 --> only successful purchases

	GROUP BY 1,2
	) rev_items ON rev_items.order_id = order.order_id

WHERE rev_items.count_of_items > 500 --> above 500 items sold
ORDER BY 1 ASC

------------------------------------------------------------------------------------
#Question 2:

--> Simple answer
SELECT 
order_id

FROM temp_order1 one

UNION --> removes duplicates

SELECT
order_id

FROM temp_order2;

--> Another possible answer
SELECT
new_temp.order_id,
COUNT(new_temp.order_id) AS count_id

FROM 	(
		SELECT 
		order_id

		FROM temp_order1 one

		UNION ALL

		SELECT
		order_id

		FROM temp_order2
		) new_temp

GROUP BY 1
HAVING COUNT(new_temp.order_id) > 1 --> identifies and removes duplicates

ORDER BY 1 DESC;

------------------------------------------------------------------------------------
#Question 3:

CREATE TABLE all_test_groups --> inserts results into temporary table

	(
	customer_id INT
	test_group VARCHAR(50)
	)

INSERT INTO all_test_groups

	SELECT
	customer_id,
	CASE
		WHEN random_split <= 50 THEN 'Test Group 1'
		WHEN random_split > 50 AND random_split <= 75 THEN 'Test Group 2'
		WHEN random_split > 75 AND random_split <= 95 THEN 'Test Group 3'
		ELSE 'Test Group 4'
	END AS test_group

	FROM (
			SELECT
			customer_id, NTILE(100) ROW_NUMBER() over (ORDER BY new_cid()) random_split --> random split based on percentiles
			FROM dbdo.fact_order 
		  );

--> Count run from temporary table

SELECT
test_group,
COUNT(DISTINCT customer_id) AS nr_customers

FROM all_test_groups
GROUP BY 1

------------------------------------------------------------------------------------
#Question 4:

SELECT
od.customer_id,
AVG(od.datetime_shipped) AS avg_days_between_orders --> takes the average date difference between 1st and 2nd orders

FROM 
	(
	SELECT 
	orders.customer_id,
	orders.order_id,
	orders.rank,
	DATEDIFF(orders.starting_order, orders.next_order) AS dates_between_orders

	FROM 
		(
		SELECT
		order.customer_id,
		order.order_id,
		RANK() OVER(PARTITION BY order.order_id ORDER BY order.order_id AS) AS rank, --> assigns a 1-to-N rank based on order age (oldest to newest). Alternatively, the LAG() function can be used here.
		order.datetime_shipped AS starting_order,
		ROW_NUMBER() OVER(PARTITION BY order.datetime_shipped ORDER BY order.order_id ASC) AS next_order --> brings next order date to column

		FROM dbdo.fact_order order
		LEFT JOIN dbdo.fact_order_line items ON items.order_id = order.order_id
		LEFT JOIN dbdo.dim_order_line_statuscode status ON status.dim_order = items.statuscode

		WHERE status.dim_order = 120 --> only successful purchases
		ORDER BY 1 ASC
		) orders
			) od

WHERE od.rank = 1 --> selects 1st order to compare it to the next order (2nd)
GROUP BY 1