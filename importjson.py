import json
from datetime import datetime
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests


def get_book_report_today():
    url ='https://www.naiin.com/category?category_1_code=28&product_type_id=1'
    r = requests.get(url)
    soup = BeautifulSoup(r.content,'html.parser')
    script = soup.find_all('script')[26].text.strip()[26:-193]
    dataset = json.loads(script)
    #with open('data2.json', 'w') as f:
        #json.dump(data2, f)
    return dataset


def save_data_into_db():
    #mysql_hook = MySqlHook(mysql_conn_id='app_db')
    dataset = get_book_report_today()
    for data in dataset:
        import mysql.connector
        db = mysql.connector.connect(host='35.168.74.59',user='root',passwd='C]h;c9jot',db='myPitikorn')
        cursor = db.cursor()
        id = data['id']
        name = data['name']
        price = data['price']
        category = data['category']
        discount = data['discount']
        

        cursor.execute('INSERT INTO Book (id,name,price,category,discount)'
                  'VALUES("%s", "%s", "%s","%s", "%s")',
                   (id, name, price,category,discount))
                          
        db.commit()
        print("Record inserted successfully into book table")
        cursor.close()
                                       
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    
}
with DAG('book_data_pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for travel report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_book_report_today',
        python_callable= get_book_report_today
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t1 >> t2 
