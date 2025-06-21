from datetime import datetime, timedelta
import json
import logging
from pydoc import text
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.mysql_hook import MySqlHook

from extractors.batdongsan_extractor import BatDongSanExtractor
from extractors.batdongsanonline_extractor import BatDongSanOnlineExtractor
from extractors.dothi_extractor import DothiExtractor
from extractors.homedy_extractor import HomedyExtractor
# from dags.transformers.batdongsan_transformer import DataTransformer
from transformers.batdongsan_transformer import BatDongSanTransformer
from transformers.batdongsanonline_transformer import BatDongSanOnlineTransformer
from transformers.dothi_transformer import DothiTransformer
from transformers.homedy_transformer import HomedyTransformer
from transformers.dataunify import DataProcessor
from transformers.dataunify import UnifiedDataProcessor

from loaders.warehouse_loader import WarehouseLoader
from loaders.json_loader import JsonLoader
import os
from sqlalchemy import text

# Định nghĩa các tham số mặc định
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Danh sách các trang web để crawl dữ liệu
WEBSITES = [
    {
        'name': 'batdongsan',
        'extractor': BatDongSanExtractor,
        'transformer': BatDongSanTransformer,
        'url': 'https://batdongsan.com.vn',
        'start_path': '/nha-dat-ban',
        'max_pages': 2,
        'driver_port': 9516
    },
    {
        'name': 'batdongsanonline',
        'extractor': BatDongSanOnlineExtractor,
        'transformer': BatDongSanOnlineTransformer,
        'url': 'https://batdongsanonline.vn',
        'start_path': '/mua-ban-nha',
        'max_pages': 2,
        'driver_port': 9515
    },
    {
        'name': 'dothi',
        'extractor': DothiExtractor,
        'transformer': DothiTransformer,
        'url': 'https://dothi.net',
        'start_path': '/nha-dat-ban.htm',
        'max_pages': 2,
        'driver_port': 9517
    },
    {
        'name': 'homedy',
        'extractor': HomedyExtractor,
        'transformer': HomedyTransformer,
        'url': 'https://homedy.com',
        'start_path': '/ban-nha-dat',
        'max_pages': 2,
        'driver_port': 9518
    }
]

# Khởi tạo DAG
with DAG(
    'real_estate_etl_pipeline',
    default_args=default_args,
    description='Luồng ETL dữ liệu bất động sản từ nhiều nguồn',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['real_estate', 'etl'],
) as dag:

    # Task bắt đầu
    start = DummyOperator(task_id='start')
    
    # Task kết thúc
    end = DummyOperator(task_id='end')

    # Tạo các task extract tuần tự
    extract_tasks = []
    transform_tasks = []
    
    for website in WEBSITES:
        # Task extract
        extract_task = PythonOperator(
            task_id=f'extract_{website["name"]}',
            python_callable=lambda website_info=website, **kwargs: extract_data(
                website_info, 
                kwargs['ds_nodash']
            ),
            dag=dag,
        )
        extract_tasks.append(extract_task)
        
        # Task transform cho từng website - chạy song song
        transform_task = PythonOperator(
            task_id=f'transform_{website["name"]}',
            python_callable=lambda website_info=website, **kwargs: transform_website_data(
                website_info,
                kwargs['ds_nodash']
            ),
            dag=dag,
        )
        transform_tasks.append(transform_task)

    # Task gộp dữ liệu sau khi biến đổi
    unify_task = PythonOperator(
        task_id='unify_data',
        python_callable=lambda **kwargs: unify_data(
            WEBSITES,
            kwargs['ds_nodash']
        ),
        dag=dag,
    )

    # Task tải dữ liệu vào các kho
    with TaskGroup(group_id='load_tasks') as load_group:
        # Task tải dữ liệu vào OLAP
        load_olap_task = PythonOperator(
            task_id='load_data_to_olap',
            python_callable=lambda **kwargs: load_data_to_olap(
                f"data/staging/unified_data_{kwargs['ds_nodash']}.csv"
            ),
            dag=dag,
        )
        
        # Task tải dữ liệu vào JSON
        load_json_task = PythonOperator(
            task_id='load_data_to_json',
            python_callable=lambda **kwargs: load_data_to_json(
                f"data/staging/unified_data_{kwargs['ds_nodash']}.csv"
            ),
            dag=dag,
        )

    # Xác định thứ tự thực hiện các task
    # Start chạy các task extract tuần tự
    start >> extract_tasks[0]
    for i in range(len(extract_tasks) - 1):
        extract_tasks[i] >> extract_tasks[i + 1]
    
    # Sau khi extract xong, bắt đầu transform song song
    for i, extract_task in enumerate(extract_tasks):
        if i == len(extract_tasks) - 1:  # Chỉ connect từ task extract cuối cùng
            extract_task >> transform_tasks

    transform_tasks >> unify_task >> load_group >> end
    # Sau khi tất cả transform xong, chạy unify
    # transform_tasks >> unify_task >> load_group >> end


def extract_data(website_info, ds_nodash):
    """
    Hàm trích xuất dữ liệu từ một trang web cụ thể
    """
    extractor = website_info['extractor'](
        base_url=website_info['url'],
        start_path=website_info['start_path'],
        max_pages=website_info['max_pages'],
        data_path="data/raw"
    )
    data = extractor.extract()
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs(f"data/raw/{website_info['name']}", exist_ok=True)
    
    # Lưu dữ liệu thô vào raw data
    raw_data_path = f"data/raw/{website_info['name']}/{website_info['name']}_{ds_nodash}.json"
    extractor.save_data(data, raw_data_path)
    
    return raw_data_path

def transform_website_data(website_info, ds_nodash):
    """
    Hàm biến đổi dữ liệu cho một trang web cụ thể
    """
    # Khởi tạo transformer cho trang web cụ thể
    transformer = website_info['transformer']()
    
    # Đường dẫn đến dữ liệu thô
    raw_data_path = f"data/raw/{website_info['name']}/{website_info['name']}_{ds_nodash}.json"
    # raw_data_path = f"data/raw/{website_info['name']}/{website_info['name']}.json"
    
    # Biến đổi dữ liệu
    transformed_data = transformer.transform(raw_data_path)
    
    # Tạo thư mục staging nếu chưa tồn tại
    os.makedirs(f"data/staging/{website_info['name']}", exist_ok=True)
    
    # Lưu dữ liệu đã biến đổi vào staging
    staging_data_path = f"data/staging/{website_info['name']}/{website_info['name']}_{ds_nodash}.json"
    transformer.save_data(transformed_data, staging_data_path)
    
    return staging_data_path


def load_data_to_olap(staging_data_path):
    """
    Hàm tải dữ liệu vào OLAP warehouse sử dụng WarehouseLoader
    """
    try:
        # Khởi tạo WarehouseLoader
        warehouse_loader = WarehouseLoader()
        
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs("data/warehouse/olap", exist_ok=True)
        
        # Đọc dữ liệu OLTP từ các file CSV đã được tạo bởi unify_data
        oltp_data = warehouse_loader.load_oltp_data_from_staging(staging_data_path)
        
        # Transform dữ liệu OLTP thành OLAP star schema
        olap_data = warehouse_loader.transform_to_olap(oltp_data)
        
        # Load dữ liệu OLAP vào MySQL warehouse
        warehouse_loader.load_data(olap_data, batch_size=1000)
        
        # Tùy chọn: Lưu OLAP data thành file để backup/debug
        warehouse_loader.save_olap_data_to_files(olap_data, "data/warehouse/olap")
        
        return "data/warehouse/olap"
        
    except Exception as e:
        print(f"Lỗi khi load dữ liệu vào OLAP: {str(e)}")
        raise



def load_data_to_json(staging_data_path):
    """
    Hàm tải dữ liệu vào kho JSON cho mô hình dự đoán
    """
    json_loader = JsonLoader()
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs("data/warehouse/json", exist_ok=True)
    
    # json_loader.load(staging_data_path, "data/warehouse/json")
    return "data/warehouse/json"

def unify_data(websites, ds_nodash):
    """
    Hàm gộp dữ liệu từ nhiều nguồn đã được biến đổi và tổ chức thành các bảng riêng biệt
    """
    processor = UnifiedDataProcessor()
    
    # Gộp dữ liệu từ các nguồn
    unified_data_path = processor.unify_data(websites, ds_nodash)
    
    return unified_data_path
