%hive
CREATE TABLE campaign (
    id_line INT,
    id_campaign INT,
    type_campaign STRING,
    days_valid INT,
    data_campaign TIMESTAMP,
    channel STRING,
    return_status STRING,
    return_date TIMESTAMP,
    client_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1")

%hive
LOAD DATA INPATH 's3a://tarefa2/campaigns_2023_hist.csv'
INTO TABLE campaign

%hive
CREATE TABLE purchase (
    purchase_id STRING,
    product_name STRING,
    product_id STRING,
    amount INT,
    price DOUBLE,
    discount_applied DOUBLE,
    payment_method STRING,
    purchase_datetime TIMESTAMP,
    purchase_location STRING,
    client_id STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1")

%hive
LOAD DATA INPATH 's3a://tarefa2/purchases_2023.csv'
INTO TABLE purchase

%hive
SELECT * FROM campaign

%hive
SELECT * FROM purchase

%hive
-- client_id: Identificação do cliente.
SELECT campaign.client_id AS campaign_client_id,
purchase.client_id AS purchase_client_id
FROM campaign INNER JOIN purchase ON campaign.client_id = purchase.client_id

%hive
-- total_price: Total gasto pelo cliente, calculado como (price * amount * discount_applied).
SELECT ROUND(price * amount * discount_applied, 2) AS total_price
FROM campaign  INNER JOIN purchase ON campaign.client_id = purchase.client_id

%hive
-- most_purchase_location: Local mais utilizado pelo cliente para realizar compras (website, app, store).
SELECT 
  purchase_location  AS most_purchase_location,
  COUNT(*) AS purchase_count
FROM purchase
GROUP BY purchase.purchase_location
ORDER BY purchase_count DESC
LIMIT 1

%hive
-- first_purchase: Data da primeira compra realizada pelo cliente.
SELECT purchase_datetime AS first_purchase
FROM purchase
GROUP BY purchase_datetime
ORDER BY purchase_datetime ASC
LIMIT 1

%hive
-- last_purchase: Data da última compra realizada pelo cliente.
SELECT purchase_datetime AS last_purchase
FROM purchase
GROUP BY purchase_datetime
ORDER BY purchase_datetime DESC
LIMIT 1

%hive
-- most_campaign: Campanha mais recebida pelo cliente.
SELECT 
  type_campaign AS most_campaign,
  COUNT(*) AS received_campaign
FROM campaign
WHERE return_status = "received"
GROUP BY type_campaign
ORDER BY received_campaign DESC
LIMIT 1

%hive
-- quantity_error: Quantidade de campanhas que retornaram o status "error" para o cliente.
SELECT 
  type_campaign,
  COUNT(type_campaign) AS quantity_error
FROM campaign
WHERE return_status = "error"
GROUP BY type_campaign
ORDER BY error_campaign DESC

%hive
-- date_today: Data atual formatada como YYYY-MM-DD.
SELECT CURRENT_DATE AS date_today

%hive
-- anomes_today: Data atual formatada como MMYYYY (tipo int).
SELECT CAST(date_format(current_date, 'MMyyyy') AS INT) AS anomes_today


%hive
WITH 
client_ids AS (
    SELECT campaign.client_id AS campaign_client_id,
           purchase.client_id AS purchase_client_id
    FROM campaign 
    INNER JOIN purchase ON campaign.client_id = purchase.client_id
),

total_spent AS (
    SELECT ROUND(price * amount * discount_applied, 2) AS total_price
    FROM campaign 
    INNER JOIN purchase ON campaign.client_id = purchase.client_id
),

most_location AS (
    SELECT 
        purchase_location AS most_purchase_location,
        COUNT(*) AS purchase_count
    FROM purchase
    GROUP BY purchase.purchase_location
    ORDER BY purchase_count DESC
    LIMIT 1
),

first_purchase_date AS (
    SELECT purchase_datetime AS first_purchase
    FROM purchase
    GROUP BY purchase_datetime
    ORDER BY purchase_datetime ASC
    LIMIT 1
),

last_purchase_date AS (
    SELECT purchase_datetime AS last_purchase
    FROM purchase
    GROUP BY purchase_datetime
    ORDER BY purchase_datetime DESC
    LIMIT 1
),

most_received_campaign AS (
    SELECT 
        type_campaign AS most_campaign,
        COUNT(*) AS received_campaign
    FROM campaign
    WHERE return_status = 'received'
    GROUP BY type_campaign
    ORDER BY received_campaign DESC
    LIMIT 1
),

error_campaign_count AS (
    SELECT 
        type_campaign AS quantity_error,
        COUNT(type_campaign) AS error_campaign
    FROM campaign
    WHERE return_status = 'error'
    GROUP BY type_campaign
    ORDER BY error_campaign DESC
)

SELECT
    COUNT(DISTINCT campaign_client_id) AS num_clients,
    SUM(total_price) AS total_spent,
    most_purchase_location,
    first_purchase AS first_purchase_date,
    last_purchase AS last_purchase_date,
    most_campaign AS most_received_campaign,
    SUM(error_campaign) AS total_errors,
    CURRENT_DATE AS date_today,
    CAST(DATE_FORMAT(CURRENT_DATE, 'MMyyyy') AS INT) AS anomes_today
FROM client_ids, total_spent, most_location, first_purchase_date, last_purchase_date, most_received_campaign, error_campaign_count
GROUP BY most_purchase_location, first_purchase, last_purchase, most_campaign