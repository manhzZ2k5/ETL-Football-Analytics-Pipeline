# ETL Football Project

ETL pipeline for football data extraction, transformation, and loading into PostgreSQL database.

pip install -r requirements.txt

Setup Database
Tạo database PostgreSQL:
sql:
CREATE DATABASE ETL_FOOTBALL;
CREATE USER football_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE ETL_FOOTBALL TO football_user;

file :"scr/database.ini":
[postgresql]
host=localhost
database=ETL_FOOTBALL
user=football_user
password=your_password
port=5432

Project Structure

ETL_Football/
├── scr/                    
│   ├── Extract.py         
│   ├── transform.py       
│   ├── load.py            
│   └── database.ini       
├── data/                  
├── data_raw/              
├── data_processed/        
├── requirements.txt       
└── README.md             

