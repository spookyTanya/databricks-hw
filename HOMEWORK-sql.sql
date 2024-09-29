-- Check if bronze table has data
SELECT * FROM bronze_table;


-- Check if transformations are correct - result must be empty
SELECT * FROM silver_table_test WHERE price <= 0 OR reviews_per_month IS NULL OR latitude IS NULL OR longitude IS NULL;


-- add constraints
ALTER TABLE silver_table_test ALTER COLUMN price SET NOT NULL;
ALTER TABLE silver_table_test ALTER COLUMN minimum_nights SET NOT NULL;
ALTER TABLE silver_table_test ALTER COLUMN availability_365 SET NOT NULL;

ALTER TABLE silver_table_test ADD CONSTRAINT price_check CHECK (price > 0);
ALTER TABLE silver_table_test ADD CONSTRAINT minimum_nights_check CHECK (minimum_nights > 0);
ALTER TABLE silver_table_test ADD CONSTRAINT availability_365_check CHECK (availability_365 >= 0);

-- Test if constraints work by trying to insert a row with price = 0
INSERT INTO silver_table_test (id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, room_type, price, minimum_nights, number_of_reviews, reviews_per_month, calculated_host_listings_count, availability_365, last_review_date) VALUES (10, 'test with 0 price', 82975282, 'Evyiatar', 'Queens', 'Bayswater', 40.60484, 73.76871, 'Private room', 0, 30, 0, 0, 1, 0, date('2019-06-23'));


-- query previous version of data
DESCRIBE HISTORY silver_table_test;

-- Check number of rows after deleting test data
SELECT COUNT(*) FROM silver_table_test@v17;
SELECT COUNT(*) FROM silver_table_test@v18;
