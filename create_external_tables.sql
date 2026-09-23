-- Create External Data Source

CREATE EXTERNAL DATA SOURCE blob_retail_datasource
WITH (
    LOCATION = 'https://synapsesgacc.blob.core.windows.net/synasstorage'
);


-- Create External File Format

CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET
);


-- Create External Table

CREATE EXTERNAL TABLE daily_revenue (
    event_date DATE,
    daily_revenue FLOAT,
    total_purchases BIGINT
)
WITH (
    LOCATION = '/gold/',
    DATA_SOURCE = blob_retail_datasource,
    FILE_FORMAT = ParquetFormat
);


-- Read the data

SELECT *
FROM daily_revenue;
