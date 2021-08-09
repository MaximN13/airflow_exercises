from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.utils import trigger_rule
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

import io
import pandas as pd
import requests
import os
import datetime as dt

my_args= {
    'owner':'me',    
}

def calc_age(birthdate):
    born_date =  dt.datetime.strptime(birthdate, '%Y-%m-%d').date()
    today = dt.date.today()
    return (today.year - born_date.year)

def calc_status(status_dict: dict) -> str:     
    if(status_dict['0']['success']):
        return('success: True')
    else:
        return('error: ' + ' '.join(status_dict['0']['errors']))    

def exec_sql(sql):   
    hook = PostgresHook(postgres_conn_id='my_postgres_db2')
    hook.run(sql=sql)

def download_orders_file(url, path):
    url_orders = url
    response_orders = requests.get(url_orders)
    df_write = pd.read_json(response_orders.text, lines=True)
    
    with open(path, 'w') as f:
        for row in df_write.iterrows():
            row[1].to_json(f, force_ascii=False)
            f.write('\n')   

def download_transactions_file(url, path):
    url_transactions = url
    response_transactions = requests.get(url_transactions)
    df_write = pd.read_json(response_transactions.text, lines=True)
    
    with open(path, 'w') as f:
        for row in df_write.iterrows():
            row[1].to_json(f)
            f.write('\n')        
    
def download_customers_file(path):
    url_customers = BaseHook.get_connection('customers_file').host
    
    login = BaseHook.get_connection('customers_file').login
    token = BaseHook.get_connection('customers_file').password 

    req_customers = requests.get(url_customers, auth=(login, token)).content

    df_customers = pd.read_csv(io.StringIO(req_customers.decode('utf-8')))

    df_customers.to_csv(path, encoding='utf-8')
      

def merge_orders_with_transactions(input_path, filename_orders, filename_transactions, output_path):
    file_orders_path = os.path.join(input_path, filename_orders)
    file_transactions_path = os.path.join(input_path, filename_transactions)

    df_orders = pd.read_json(file_orders_path, lines=True)
    df_transactions = pd.read_json(file_transactions_path, lines=True)

    df_col_transactions = pd.DataFrame(list(df_transactions.items()), columns=['order_uuid', 'status'])
    
    if(pd.api.types.is_string_dtype(df_orders['order_uuid']) and pd.api.types.is_string_dtype(df_col_transactions['order_uuid'])):
        df_orders_status = df_orders.merge(df_col_transactions, on='order_uuid'.strip())
        with open(output_path, 'w') as f:
            for row in df_orders_status.iterrows():
                row[1].to_json(f, force_ascii=False)
                f.write('\n')                
    else:
        print('column order_uuid in orders or transactions files is not string type')    

def merge_orders_with_goods(input_path, filename_orders, filename_goods, output_path):
    file_orders_path = os.path.join(input_path, filename_orders)
    file_goods_path = os.path.join(input_path, filename_goods)
        
    df_orders = pd.read_json(file_orders_path, lines=True)
    df_goods = pd.read_csv(file_goods_path, names=['id', 'sku_name', 'sku_price'])

    if(pd.api.types.is_string_dtype(df_orders['sku_name']) and pd.api.types.is_string_dtype(df_goods['sku_name'])):
        df_orders_sku = df_orders.merge(df_goods, on='sku_name'.strip())
        
        with open(output_path, 'w') as f:
            for row in df_orders_sku.iterrows():
                row[1].to_json(f, force_ascii=False)
                f.write('\n')
    else:
        print('column sku_name in orders or goods files is not string type')

def merge_orders_with_cust(input_path, filename_orders, filename_cust):
    file_orders_path = os.path.join(input_path, filename_orders)
    file_cust_path = os.path.join(input_path, filename_cust)
    
    df_orders = pd.read_json(file_orders_path, lines=True)
    df_cust = pd.read_csv(file_cust_path, names=['id', 'name', 'birthdate', 'gender', 'email'])

    if(pd.api.types.is_string_dtype(df_orders['email']) and pd.api.types.is_string_dtype(df_cust['email'])):
        df_final = df_orders.merge(df_cust, on='email'.strip())
    else:
        print('column email in orders or customers files is not string type')

    last_modified_dt = dt.datetime.now().strftime("%Y-%b-%d %H:%M")

    exec_sql('truncate table table_info;')    
    for index, row in df_final.iterrows():
        invoke_insert((row['full_name']).strip(), calc_age((row['birthdate']).strip()), (row['sku_name']).strip(), row['order_date'], \
                        calc_status(row['status']), row['qty'] * row['sku_price'], last_modified_dt)    
    exec_sql('commit;')

def invoke_insert(name, age, sku_name, date, payment_status, total_price, last_modified_dt):
    sql = f"""insert into table_info(
                    name ,
                    age ,
                    sku_name ,
                    date ,
                    payment_status ,
                    total_price ,
                    last_modified_dt 
                    )
            values (
                '{name}',
                {age},
                '{sku_name}',
                '{date}',
                '{payment_status}',
                {total_price},
                '{last_modified_dt}'
                );                       
                """
    print(sql)
    exec_sql(sql)    

with DAG(params={"postgres_conn_id": "my_postgres_db2"}, 
    dag_id='lesson2_1',   
    default_args=my_args, 
    schedule_interval=None,
    start_date=days_ago(2),
    template_searchpath = '/tmp/lessons_2_files'
) as dag: 

     t1 = PythonOperator(
        task_id='download_orders_file',
        python_callable=download_orders_file,
        op_kwargs={'url': f'https://tmp.mosyag.in/rb-airflow-hw2/orders.json',
                    'path': '/tmp/lessons_2_files/orders_temp.json'}                              
    ) 

     t2 = PythonOperator(
        task_id='download_transactions_file',
        python_callable=download_transactions_file,
        op_kwargs={'url': f'https://api.jsonbin.io/b/5ed7391379382f568bd22822',
                    'path': '/tmp/lessons_2_files/transactions_temp.json'}                     
    ) 

     t3 = PythonOperator(
        task_id='download_customers_file',
        python_callable=download_customers_file,
        op_kwargs={'path': '/tmp/lessons_2_files/customers_temp.csv'} 
    ) 
    
     t4 = PythonOperator(
        task_id='merge_orders_with_transactions',
        python_callable=merge_orders_with_transactions,
        op_kwargs={"input_path": "/tmp/lessons_2_files", "filename_orders": "orders_temp.json", "filename_transactions": "transactions_temp.json",
                            'output_path': '/tmp/lessons_2_files/merge_orders_with_transactions.json'}                            
    ) 

     t5 = PythonOperator(
        task_id='merge_orders_with_goods',
        python_callable=merge_orders_with_goods,
        op_kwargs={"input_path": "/tmp/lessons_2_files", "filename_orders": "merge_orders_with_transactions.json", "filename_goods": "goods.csv",
                            'output_path': '/tmp/lessons_2_files/merge_orders_with_goods.json'}      
    )  

     t6 = PythonOperator(
        task_id='merge_orders_with_cust',
        python_callable=merge_orders_with_cust,
        op_kwargs={"input_path": "/tmp/lessons_2_files", "filename_orders": "merge_orders_with_goods.json", "filename_cust": "customers_temp.csv"}                                
    )         

     run_this_first = DummyOperator(
        task_id = 'run_this_first'
    )

     run_this_last = DummyOperator(
        task_id = 'run_this_last',
        trigger_rule = TriggerRule.ALL_DONE
    )
     
     #Pipeline
     run_this_first >> [t1, t2, t3] >> t4 >> t5 >> t6 >> run_this_last         