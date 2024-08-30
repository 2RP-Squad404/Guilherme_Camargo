# Tarefa: Criação de Script SQL para Análise de Campanhas e Compras

[Script SQL](tarefa2.sql)

Nessa tarefa era necessário utilizar dois datasets CSV para obtenção das informações pedidas por meio de script SQL. Os datasets eram Campaign Dataset e Purchase Dataset.

As informações pedidas eram:
1. client_id: Identificação do cliente.
2. total_price: Total gasto pelo cliente, calculado como (price * amount * discount_applied).
3. most_purchase_location: Local mais utilizado pelo cliente para realizar compras (website, app, store).
4. first_purchase: Data da primeira compra realizada pelo cliente.
5. last_purchase: Data da última compra realizada pelo cliente.
6. most_campaign: Campanha mais recebida pelo cliente.
7. quantity_error: Quantidade de campanhas que retornaram o status "error" para o cliente.
8. date_today: Data atual formatada como YYYY-MM-DD.
9. anomes_today: Data atual formatada como MMYYYY (tipo int).

Primeiramente iniciei com a criação das tabelas para a inserção das informações dos datasets:
```sql
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
```

```sql
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
```
Após a criação das tabelas realizei a inserção dos dados nas respectivas tabelas:

```sql
%hive
LOAD DATA INPATH 's3a://tarefa2/campaigns_2023_hist.csv'
INTO TABLE campaign
```

```sql
%hive
LOAD DATA INPATH 's3a://tarefa2/purchases_2023.csv'
INTO TABLE purchase
```

Para confirmar o processo realizei uma query em ambas as tabelas:

```sql
%hive
SELECT * FROM campaign
```

```sql
%hive
SELECT * FROM purchase
```

Antes de realizar o retorno de uma tabela completa comecei realizando consultas individuais para cada informação.

## client_id

Como em ambos os datasets haviam haviam client_id resolvi retornar ambos os valores somente para indicar a relação entre os datasets.

```sql
%hive
-- client_id: Identificação do cliente.
SELECT campaign.client_id AS campaign_client_id,
purchase.client_id AS purchase_client_id
FROM campaign INNER JOIN purchase ON campaign.client_id = purchase.client_id
```

## total_price

Para retornar o preço total realizei o cálculo preço * quantidade * desconto com os valores presentes na tabela Purchase.

```sql
%hive
-- total_price: Total gasto pelo cliente, calculado como (price * amount * discount_applied).
SELECT ROUND(price * amount * discount_applied, 2) AS total_price
FROM campaign  INNER JOIN purchase ON campaign.client_id = purchase.client_id
```

## most_purchase_location

Para encontrar o local mais utilizado para a realização de compras pelo cliente organizei por contagem e limitei somente ao primeiro valor da coluna criada.

```sql
%hive
-- most_purchase_location: Local mais utilizado pelo cliente para realizar compras (website, app, store).
SELECT 
  purchase_location  AS most_purchase_location,
  COUNT(*) AS purchase_count
FROM purchase
GROUP BY purchase.purchase_location
ORDER BY purchase_count DESC
LIMIT 1
```

## first_purchase

Para encontrar a data da primeira compra realizada eu ordenei todas as datas de forma ascendente e limitei somente ao primeiro resultado.

```sql
%hive
-- first_purchase: Data da primeira compra realizada pelo cliente.
SELECT purchase_datetime AS first_purchase
FROM purchase
GROUP BY purchase_datetime
ORDER BY purchase_datetime ASC
LIMIT 1
```

## last_purchase

Para encontrar a data da última compra organizei as datas de compras de forma descendente e limitei somente ao primeiro valor.

```sql
%hive
-- last_purchase: Data da última compra realizada pelo cliente.
SELECT purchase_datetime AS last_purchase
FROM purchase
GROUP BY purchase_datetime
ORDER BY purchase_datetime DESC
LIMIT 1
```

## most_campaign

Para encontrar qual a campanha que foi mais recebida pelos clientes eu realizei a contabilização das campanhas que possuiam o status de received e organizei de forma descendente limitando somente a um resultado.

```sql
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
```

## quantity_error

Para encontrar a quantidade de campanhas que retornaram o status "error" separei cada campanha por tipo e depois realizei a contagem de status que apresentavam a mensagem "error", organizando e ordenando de forma descendente.

```sql
%hive
-- quantity_error: Quantidade de campanhas que retornaram o status "error" para o cliente.
SELECT 
  type_campaign,
  COUNT(type_campaign) AS quantity_error
FROM campaign
WHERE return_status = "error"
GROUP BY type_campaign
ORDER BY quantity_error DESC
```

## date_today

Para receber a data atual utilizei a função CURRENT_DATE que retorna a data atual no formato YYYY-MM-DD.

```sql
%hive
-- date_today: Data atual formatada como YYYY-MM-DD.
SELECT CURRENT_DATE AS date_today
```

## anomes_today

Para retornar atual no formato MMYYYY (int) utilizei a função CAST() para realizar a conversão de CURRENT_DATE para int e mdoficar o formato.

```sql
%hive
-- anomes_today: Data atual formatada como MMYYYY (tipo int).
SELECT CAST(date_format(current_date, 'MMyyyy') AS INT) AS anomes_today
```

## Tabela

Após finalizar as consultas individuais tentei utilizar CTEs (Common Table Expression) para melhorar o processo de geração da tabela. Porém no momento de execução foi encontrado diferentes problemas como o tempo de execução e falhas inesperadas devido a formulação da query.

Aqui esta a última query que foi produzida e que não foi totalmente realizada, sendo impedida durante a realização.

```sql
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
```
