-- Create analytics database for the football warehouse (besides Airflow metadata)
CREATE DATABASE "ETL_FOOTBALL";
GRANT ALL PRIVILEGES ON DATABASE "ETL_FOOTBALL" TO airflow;


