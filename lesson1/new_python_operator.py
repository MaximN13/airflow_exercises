
import airflow
import requests
import datetime as dt
from datetime import timedelta, date
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


dag = DAG(
    dag_id="invoke_create_common_file",
    start_date=airflow.utils.dates.days_ago(2),
    schedule_interval=None
)

def create_common_file():
    flag_first_write = True #TODO 
    content = list()
    str_list = list()
    content_filter = list()

    now = dt.datetime.now().strftime("%m-%d-%Y")
    timestamp_requested = dt.datetime.now().strftime("%m-%d-%Y %H.%M.%S")

    store_dir = os.getcwd()
    print(store_dir)
    input_name = 'common_file.csv'
    file_path = os.path.join(store_dir, input_name)

    header = 'date, region, infected, recovered, timestamp_requested'

    for x in range(1, 8):
        minus_one_day = dt.datetime.strptime(str(now), "%m-%d-%Y") - dt.timedelta(days=x)
        url = 'https://raw.githubusercontent.com/CSSEGISandData/Covid-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{}.csv'.format(minus_one_day.strftime("%m-%d-%Y"))

        if(flag_first_write):
            f = open(file_path, "w")
            f.write(header)
            f.write('\n')
            flag_first_write = False
        else:
            f = open(file_path, "a")

        response = requests.get(url)
        content = response.text.split('\n')

        str_list = [i.split(',') for i in content if len(i) >= 3]
        content_filter = [i for i in str_list if i[3] == 'Russia']

        for i in content_filter:
            app_str = i[4] + ',' + i[2] + ',' + i[7] + ',' + i[9] + ',' + timestamp_requested
            f.write(app_str)
            f.write('\n')

    f.close() 


invoke_create_common_file = PythonOperator(
        task_id='invoke_create_common_file',
        python_callable=create_common_file,
        dag=dag
    )

























