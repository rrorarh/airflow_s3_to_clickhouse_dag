from airflow import DAG
from airflow.models import Variable
from datetime import tzinfo, datetime, timedelta, timezone
import logging
import zipfile
from io import BytesIO
import re 
import pandas as pd
import numpy as np

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook

S3_CONN_ID = 'a_my_s3_conn'
CLICKHOUSE_CONN_ID='a_my_clickhouse_conn'

S3_SOURCE_BUCKET = Variable.get("s3_bucket_raw_data")
S3_TARGET_BUCKET = Variable.get("s3_bucket_reports")
EMAILS_TO_ALERT = Variable.get("email_list")

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 25),
    'email': EMAILS_TO_ALERT,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60)
}

### вспомогательные функции ###
#сохраняем s3 object в BytesIO buffer object
def save_s3obj_to_bitesio(object_key,bucket_):
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    s3_obj= s3.get_key(object_key,bucket_)
    logging.info(f"объект из бакета: {str(s3_obj)}")
    logging.info(type(s3_obj))
    buffer = BytesIO(s3_obj.get()["Body"].read())
    return buffer
#открываем BytesIO buffer object с помощью zipfile, получаем список файлов из зип-архива
def get_list_from_bitesio_obj(bitesio_obj):
    zip_file_ = zipfile.ZipFile(bitesio_obj)
    list_of_files_from_zip_archive = zip_file_.namelist()
    logging.info(f"список объектов из зип-архива: {str(list_of_files_from_zip_archive)}")
    logging.info(list_of_files_from_zip_archive)
    return list_of_files_from_zip_archive, zip_file_
#обработка колонок с числовыми значениями перед заливкой в кликхаус
def convert_d_val_funct(val):
    if val == np.nan or val in ('','NULL'):
        return 0 # or whatever else you want to represent your NaN with
    return int(val)
#делаем из датафрейма объект bytesio и закидываем файл в s3 бакет с отчетами 
def transform_df_to_bytesio(df_,filename_starts_with):
    csv_buffer = BytesIO()
    df_.to_csv(csv_buffer, encoding="utf-8", index=False, header = True)
    file_name = f"{filename_starts_with}{datetime.today().strftime('%Y-%m-%d')}.csv"
    return csv_buffer, file_name
#загрузка файла из зип-архива в clcikhouse (по частям)
def load_csv_to_clickhouse(zip_file_to_process):
    ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
    with zip_arch as myzip:
        with myzip.open(el) as myfile:
            #делим файл на части и инсертим поочередно в таблицу кликхаус
            chunksize_to_process = 20000
            for chunk in pd.read_csv(myfile, encoding = "UTF-8", 
                                            chunksize = chunksize_to_process, 
                                            parse_dates = [1,2],
                                            na_values= False,
                                            converters = { "tripduration": lambda x: convert_d_val_funct(x),
                                                            "start station id": lambda x: convert_d_val_funct(x),#np.int64,
                                                            "start station name": lambda x: str(x),
                                                            "start station latitude": lambda x: str(x),
                                                            "start station longitude": lambda x: str(x),
                                                            "end station id": lambda x: convert_d_val_funct(x),
                                                            "end station name": lambda x: str(x),
                                                            "end station latitude": lambda x: str(x),
                                                            "end station longitude": lambda x: str(x),
                                                            "bikeid": lambda x: convert_d_val_funct(x),
                                                            "usertype": lambda x: str(x),
                                                            "birth year": lambda x: str(x),
                                                            "gender": lambda x: convert_d_val_funct(x),
                                                        }  
                                    ): 
                ch_hook.run("INSERT INTO default.raw_data_city_bike_trips values", chunk.to_dict('records'),types_check=True)
                logging.info("CHUNK IS INSERTED")

##### основные функции ######

#получаем список объектов в s3 bucket
def get_list_of_s3_object_keys(s3_bucket, **kwargs):
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    key_list = s3.list_keys(s3_bucket)
    logging.info(key_list)
    return key_list
#если список объектов для обработки пустой, завершаем выполнение дага, 
#если не пустой- ветка с тасками по обработке данных
def choose_branch(**kwargs):
    ti = kwargs['ti']
    key_list = ti.xcom_pull(task_ids='get_list_of_s3_objects')
    logging.info(key_list)
    if key_list == []:
        return 'end_dag'
    else:
        return 'process_files'

#обработка файлов s3 и загрузка данных в кликхаус
def process_s3_files(**kwargs):
    ti = kwargs['ti']
    s3_objects_keys_list = ti.xcom_pull(task_ids='get_list_of_s3_objects')
    logging.info(f"список ключей файлов в бакете:  {str(s3_objects_keys_list)}")

    for s3_object_key in s3_objects_keys_list:
        #сохраняем s3 object в BytesIO buffer object
        buffer_obj = save_s3obj_to_bitesio(s3_object_key,S3_SOURCE_BUCKET)
        #открываем его с помощью zipfile, получаем список файлов из зип-архива
        list_of_files, zip_arch = get_list_from_bitesio_obj(buffer_obj)

        #проверяем название каждого файла из списка на сооттветствие шаблону
        #т.к. в некоторых архивах есть не только csv файлы
        for el in list_of_files:
            if re.fullmatch('20[\w|\s|\S]*\.csv', el) is not None:
                logging.info(f"файл '{el}' подходит")
                #загрузка файла из зип-архива в clcikhouse
                load_zip_to_clickhouse(zip_arch)
        logging.info("обработка списка файлов из этого зип-арива завершена")

#удаляем обработанные объекты из s3 bucket по списку
def delete_list_of_s3_object_keys(s3_bucket, xcom_task_ids_, **kwargs):
    ti = kwargs['ti']
    key_list_to_del = ti.xcom_pull(task_ids=xcom_task_ids_)
    logging.info(key_list_to_del)

    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    s3.delete_objects(bucket = s3_bucket,keys = key_list_to_del)
    logging.info('objects deleted')

#селекты к таблице кликхаус и сохранение отчетов в бакет
def make_reports_and_put_into_s3_bucket(**kwargs):  
    ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    #Отчет 1: количество поездок по дням
    #формируем из результата sql-запроса датафрейм
    records_list_1 = ch_hook.get_records(sql="""select toDate(starttime) as report_dt,count(*) as qty_of_trips \
                 from default.raw_data_city_bike_trips\
                 group by toDate(starttime)\
                 order by 1 \
                 """
                 )
    report_df_1 = pd.DataFrame.from_records(records_list_1, columns=['report_date', 'qty_of_trips'])
    #делаем из датафрейма объект bytesio и закидываем файл в s3 бакет с отчетами   
    csv_buffer_1,file_name_1 = transform_df_to_bytesio(report_df_1,"trips_by_day_")
    s3.load_bytes(bucket_name=S3_TARGET_BUCKET, bytes_data=csv_buffer_1.getvalue(),replace=True,key=file_name_1)
    logging.info("отчет по кол-ву поездок по дням загружен")
    #Отчет 2: средняя продолжительность поездок по дням
    records_list_2 = ch_hook.get_records(sql="""select toDate(starttime),avg(tripduration)/(60*60) as avg_trip_dur_in_hours \
                 from default.raw_data_city_bike_trips\
                 group by toDate(starttime)\
                 order by 1 \
                 """
                 )
    report_df_2 = pd.DataFrame.from_records(records_list_2, columns=['report_date', 'avg_trip_dur_in_hours']) 
    csv_buffer_2,file_name_2 = transform_df_to_bytesio(report_df_2,"avg_trip_duration_by_day_")
    s3.load_bytes(bucket_name=S3_TARGET_BUCKET, bytes_data=csv_buffer_2.getvalue(),replace=True,key=file_name_2)
    #Отчет 3: распределение поездок пользователей, разбитых по категории «gender»
    records_list_3 = ch_hook.get_records(sql="""select gender,count(*)/ (select count(*) from default.raw_data_city_bike_trips)  as qty_trips_percent \
                 from default.raw_data_city_bike_trips\
                 group by gender\
                 order by 1 \
                 """
                 )
    report_df_3 = pd.DataFrame.from_records(records_list_3, columns=['gender', 'qty_trips_percent']) 
    csv_buffer_3,file_name_3 = transform_df_to_bytesio(report_df_3,"total_trips_by_gender_")
    s3.load_bytes(bucket_name=S3_TARGET_BUCKET, bytes_data=csv_buffer_3.getvalue(),replace=True,key=file_name_3)

########## DAG ##########
dag = DAG('a_dag_s3_to_clickhouse', default_args=DEFAULT_DAG_ARGS, schedule_interval= '@hourly')

keylist = PythonOperator(
    task_id='get_list_of_s3_objects',
    python_callable= get_list_of_s3_object_keys,
    op_kwargs={'s3_bucket': S3_SOURCE_BUCKET},
    provide_context=True,
    dag=dag
)

choose_branch_ = BranchPythonOperator(
    task_id='choose_branch',
    python_callable=choose_branch,
    provide_context=True,
    dag=dag
)

end_dag_ = DummyOperator(
    task_id="end_dag",
    dag=dag
)

process_task = PythonOperator(
    task_id='process_files',
    python_callable= process_s3_files,
    provide_context=True,
    dag=dag
)

del_processed_files = PythonOperator(
    task_id='delete_processed_s3_files',
    python_callable= delete_list_of_s3_object_keys,
    op_kwargs={'s3_bucket': S3_SOURCE_BUCKET, 
                'xcom_task_ids_': 'get_list_of_s3_objects'
                },
    provide_context=True,
    dag=dag
)

make_reports = PythonOperator(
    task_id='reports',
    python_callable=make_reports_and_put_into_s3_bucket,
    provide_context=True,
    dag=dag
)


keylist >> choose_branch_ >> [process_task, end_dag_]
process_task >> del_processed_files >> make_reports