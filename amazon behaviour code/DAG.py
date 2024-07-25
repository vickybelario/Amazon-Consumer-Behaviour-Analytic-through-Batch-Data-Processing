from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd
from dateutil import parser
from elasticsearch import Elasticsearch

# from elasticsearch.helpers import bulk
#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.utils.task_group import TaskGroup

database = "airflow"
username = "airflow"
password = "airflow"
host = "postgres"

# Membuat URL koneksi PostgreSQL
postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"



def load_csv_to_postgres():
    '''
    Fungsi untuk memuat data dari file CSV ke dalam tabel PostgreSQL.
    Membaca file CSV yang berada di '/opt/airflow/dags/amazon_data_raw.csv'
    dan menyimpannya ke tabel 'table' di database PostgreSQL.
    '''
    # menggunakan URL saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/amazon_data_raw.csv')
    #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
    df.to_sql('table', conn, 
              index=False, 
              if_exists='replace')  # M
    


def ambil_data():
    ''' 
    Fungsi untuk mengambil data dari tabel PostgreSQL dan menyimpannya ke file CSV baru.
    Membaca data dari tabel 'table' di PostgreSQL dan menyimpannya ke 
    '/opt/airflow/dags/amazon_data_new.csv'.
    '''
    # membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query('select * from table', conn) #nama table sesuai dengan nama table di postgres
    df.to_csv('/opt/airflow/dags/amazon_data_new.csv', sep=',', index=False)
    


def preprocessing(): 
    ''' 
    Fungsi untuk membersihkan data.
    Menghapus duplikat, memformat kolom 'Timestamp', dan mengubah nama kolom menjadi huruf kecil.
    Menyimpan data yang sudah dibersihkan ke file CSV '/opt/airflow/dags/amazon_data_clean.csv'.
    '''
    # pembersihan data
    df = pd.read_csv("/opt/airflow/dags/amazon_data_new.csv")
    
    #df['Timestamp'] = df['Timestamp'].apply(parser.parse)
    # Convert timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    #Add ID column starting from 1
    df['ID'] = df.index + 1

    df.columns = df.columns.str.lower()

    # Replace '.' and 'others' values with NaN
    df['purchase_categories'] = df['purchase_categories'].replace(['.', 'others'], pd.NA)
    df['service_appreciation'] = df['service_appreciation'].replace(['.'], pd.NA)
    df['improvement_areas'] = df['improvement_areas'].replace(['.'], pd.NA)
    df['cart_abandonment_factors'] = df['cart_abandonment_factors'].replace(['others'], pd.NA)  
    df['product_search_method'] = df['product_search_method'].replace(['others'], pd.NA)
    
    # Menghapus baris yang mengandung nilai NaN dari DataFrame
    df.dropna(inplace=True)     
    df.drop_duplicates(inplace=True)

    df.to_csv('/opt/airflow/dags/amazon_data_clean.csv', index=False)

def upload_to_elasticsearch():
    '''
    Fungsi untuk mengunggah data yang telah dibersihkan ke Elasticsearch.
    Membaca data dari file CSV '/opt/airflow/dags/amazon_data_clean.csv' dan mengunggahnya ke index 'table_test' di Elasticsearch.
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/amazon_data_clean.csv')     
    
    for i, r in df.iterrows():
        doc = r.to_dict() #Konversi baris menjadi dictionary
        res = es.index(index="table_test", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        


# Konfigurasi default_args untuk DAG
default_args = {
    'owner': 'vickybelario', 
    'start_date': datetime(2024, 6, 18, 13, 00)
}



with DAG(
    "DAG", #atur nama project 
    description='amazon customer behaviour',
    schedule_interval='30 6 * * *', #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    


    # Task : 1 - Memuat data CSV ke PostgreSQL
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuai dengan nama fungsi yang dibuat
    
    # Task: 2 - Mengambil data dari PostgreSQL dan menyimpannya ke file CSV baru
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data) #
    
    # Task: 3 - Menjalankan pembersihan data
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # Task: 4 - Mengunggah data yang sudah dibersihkan ke Elasticsearch
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    # Proses untuk menjalankan task di Airflow
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data



