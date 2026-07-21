-- ============================================================
-- Snowflake -> Amazon S3 using Apache Iceberg
-- External Catalog: AWS Glue
-- Student Demo
-- ============================================================

---------------------------------------------------------------
-- 1. Create Warehouse
---------------------------------------------------------------

CREATE OR REPLACE WAREHOUSE project_wh
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE project_wh;

---------------------------------------------------------------
-- 2. Create Database
---------------------------------------------------------------

CREATE OR REPLACE DATABASE iceberg_project;

USE DATABASE iceberg_project;

---------------------------------------------------------------
-- 3. Create Schema
---------------------------------------------------------------

CREATE OR REPLACE SCHEMA proj;

USE SCHEMA proj;

SELECT CURRENT_VERSION();
SELECT CURRENT_REGION();
---------------------------------------------------------------
-- 4. Create External Volume + AWS Role: SnowflakeIceberg
---------------------------------------------------------------
-- Create a new AWS role in IAM called 'SnowflakeIceBerg'
-- under trusted entities use (we will change later, use your own root account ID):
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": {
--         "AWS": "arn:aws:iam::454497087304:root"
--       },
--       "Action": "sts:AssumeRole"
--     }
--   ]
-- }
-- for the permissions, create one inline and point to your own bucket:
-- {
-- 	"Version": "2012-10-17",
-- 	"Statement": [
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"s3:GetBucketLocation",
-- 				"s3:ListBucket"
-- 			],
-- 			"Resource": "arn:aws:s3:::rev-spark-454497087304-us-east-2-an"
-- 		},
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"s3:GetObject",
-- 				"s3:PutObject",
-- 				"s3:DeleteObject"
-- 			],
-- 			"Resource": "arn:aws:s3:::rev-spark-454497087304-us-east-2-an/iceberg/*"
-- 		}
-- 	]
-- }

CREATE OR REPLACE EXTERNAL VOLUME my_external_volume
STORAGE_LOCATIONS =
(
    (
        NAME='s3_location'
        STORAGE_PROVIDER='S3'
        STORAGE_BASE_URL='s3://rev-spark-609375805055-us-east-2-an/iceberg/'
        STORAGE_AWS_ROLE_ARN='arn:aws:iam::609375805055:role/IceBergPolicy'
    )
)
ALLOW_WRITES = TRUE;

-- get the STORAGE_AWS_IAM_USER_ARN & STORAGE_AWS_EXTERNAL_ID
-- we will use these to update our trust policy in the role
-- this would be a best practice in prod
-- we will have to create a role with a generic trust policy first, 
-- then we can update with the information from this describe call 
DESC EXTERNAL VOLUME my_external_volume;

-- trusted entities for the SnowflakeIceberg role is updated with the new information for best security:
-- {
--     "Version": "2012-10-17",
--     "Statement": [
--         {
--             "Effect": "Allow",
--             "Principal": {
--                 "AWS": "arn:aws:iam::389656351827:user/6yqy1000-s"
--             },
--             "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": "YO33549_SFCRole=4_lnEmcJkA5hMbVrgczAt9PMM3HBQ="
--                 }
--             }
--         }
--     ]
-- }


---------------------------------------------------------------
-- 5. Create Glue Catalog Integration + AWS Role: SnowflakeGlueRole
---------------------------------------------------------------

-- Create a new AWS role in IAM called 'SnowflakeGlueRole'
-- under trusted entities use (we will change later, use your own root account ID):
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": {
--         "AWS": "arn:aws:iam::454497087304:root"
--       },
--       "Action": "sts:AssumeRole"
--     }
--   ]
-- }
-- for permissions, create an inline policy:
-- {
-- 	"Version": "2012-10-17",
-- 	"Statement": [
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"glue:GetDatabase",
-- 				"glue:GetDatabases",
-- 				"glue:CreateDatabase",
-- 				"glue:GetTable",
-- 				"glue:GetTables",
-- 				"glue:CreateTable",
-- 				"glue:UpdateTable",
-- 				"glue:DeleteTable"
-- 			],
-- 			"Resource": "*"
-- 		}
-- 	]
-- }

CREATE OR REPLACE CATALOG INTEGRATION my_catalog
CATALOG_SOURCE = GLUE
TABLE_FORMAT = ICEBERG
CATALOG_NAMESPACE = 'iceberg_catalog_db'
GLUE_AWS_ROLE_ARN = 'arn:aws:iam::609375805055:role/SnowflakeGluePolicy'
GLUE_CATALOG_ID = '609375805055'
GLUE_REGION = 'us-east-2'
ENABLED = TRUE;

-- much like with our external volume, we will get values 
-- from this so that we can update our trust policy in our relevant AWS role 
-- output like this will be used in our trust policy:
-- GLUE_AWS_IAM_USER_ARN	String	arn:aws:iam::389656351827:user/6yqy1000-s
-- GLUE_AWS_EXTERNAL_ID	String	YO33549_SFCRole=4_Vdh7RHz9mLExrFBUECCfdyoj1aI=
DESC CATALOG INTEGRATION my_catalog;
-- update the trust policy on the SnowflakeGlueRole with the values from our describe call:
-- {
--     "Version": "2012-10-17",
--     "Statement": [
--         {
--             "Effect": "Allow",
--             "Principal": {
--                 "AWS": "arn:aws:iam::389656351827:user/6yqy1000-s"
--             },
--             "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": "YO33549_SFCRole=4_0ppRjwMKZzCV5EFukMG0BMAUMfw="
--                 }
--             }
--         }
--     ]
-- }

---------------------------------------------------------------
-- 6. Create Iceberg Table - External
---------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE customers
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'customers';

CREATE OR REPLACE ICEBERG TABLE orders
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'orders';

CREATE OR REPLACE ICEBERG TABLE products
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'products';

---------------------------------------------------------------
-- Assignment: Create Internal table (need a schema for this)
---------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE titanic (
    passenger_id INTEGER,
    survived BOOLEAN,
    p_class INTEGER,
    name VARCHAR,
    sex VARCHAR,
    age FLOAT,
    parch INTEGER,
    ticket VARCHAR,
    fare FLOAT,
    cabin VARCHAR,
    embarked VARCHAR
)
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'my_external_volume'

---------------------------------------------------------------
-- 7. Query Data
---------------------------------------------------------------

-- Let's see how many customers have loyalty over 300
SELECT *
FROM customers
WHERE loyalty_points > 300;

-- Let's see how much each customer spent
-- Group by before the join for better performance
-- In hindsight, this is pointless since each customer has only one order
SELECT c.first_name, c.last_name, c.country, o.total
FROM customers c
INNER JOIN (
SELECT customer_id, SUM(total_amount) AS total
FROM orders
GROUP BY customer_id
) o ON c.customer_id = o.customer_id;

-- Let's see how much people spend on average per country
SELECT c.country, AVG(o.total_amount) AS average_spending
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.country;

-- Calculate how much stock we have in total in each of the product categories
SELECT category, SUM(stock_quantity) AS total_stock
FROM products
GROUP BY category;

-- Multi-join: Let's see who bought what
SELECT c.first_name, c.last_name, c.email, p.product_name
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
INNER JOIN products p ON o.product_id = p.product_id;

---------------------------------------------------------------
-- 8. Verify Metadata
---------------------------------------------------------------

SHOW ICEBERG TABLES;

DESCRIBE ICEBERG TABLE titanic;

SELECT COUNT(*)
FROM titanic;

---------------------------------------------------------------
-- 9. Cleanup
---------------------------------------------------------------

DROP TABLE customers;
DROP TABLE orders;
DROP TABLE products;
DROP SCHEMA proj;
DROP DATABASE iceberg_project;
DROP EXTERNAL VOLUME my_external_volume;
DROP CATALOG INTEGRATION my_catalog;