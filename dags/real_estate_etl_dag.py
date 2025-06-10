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
    # raw_data_path = f"data/raw/{website_info['name']}/{website_info['name']}_{ds_nodash}.json"
    raw_data_path = f"data/raw/{website_info['name']}/{website_info['name']}.json"
    
    # Biến đổi dữ liệu
    transformed_data = transformer.transform(raw_data_path)
    
    # Tạo thư mục staging nếu chưa tồn tại
    os.makedirs(f"data/staging/{website_info['name']}", exist_ok=True)
    
    # Lưu dữ liệu đã biến đổi vào staging
    staging_data_path = f"data/staging/{website_info['name']}/{website_info['name']}_{ds_nodash}.json"
    transformer.save_data(transformed_data, staging_data_path)
    
    return staging_data_path

# def unify_data(websites, ds_nodash):
#     """
#     Hàm gộp dữ liệu từ nhiều nguồn đã được biến đổi
#     """
#     # Khởi tạo transformer chung
#     transformer = DataTransformer()
    
#     # Thu thập dữ liệu đã biến đổi từ các nguồn
#     transformed_data = {}
#     for website in websites:
#         staging_data_path = f"data/staging/{website['name']}/{website['name']}_{ds_nodash}.json"
#         transformed_data[website['name']] = transformer.load_data(staging_data_path)
    
#     # Gộp dữ liệu từ các nguồn
#     unified_data = transformer.unify_data(transformed_data)
    
#     # Tạo thư mục staging nếu chưa tồn tại
#     os.makedirs("data/staging", exist_ok=True)
    
#     # Lưu dữ liệu thống nhất vào staging
#     unified_data_path = f"data/staging/unified_data_{ds_nodash}.csv"
#     transformer.save_data(unified_data, unified_data_path)
    
#     return unified_data_path

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
        oltp_data = load_oltp_data_from_staging(staging_data_path)
        
        # Transform dữ liệu OLTP thành OLAP star schema
        olap_data = warehouse_loader.transform_to_olap(oltp_data)
        
        # Load dữ liệu OLAP vào MySQL warehouse
        warehouse_loader.load_data(olap_data, batch_size=1000)
        
        # Tùy chọn: Lưu OLAP data thành file để backup/debug
        save_olap_data_to_files(olap_data, "data/warehouse/olap")
        
        return "data/warehouse/olap"
        
    except Exception as e:
        print(f"Lỗi khi load dữ liệu vào OLAP: {str(e)}")
        raise

def load_oltp_data_from_staging(staging_data_path):
    """
    Đọc dữ liệu OLTP từ thư mục staging
    """
    # Lấy thư mục chứa các file CSV từ unify_data
    # staging_data_path có dạng: "data/staging/unified_data_20250529.csv"
    # Cần lấy thư mục: "data/staging/proceed/20250529/"
    
    date_part = staging_data_path.split('_')[-1].replace('.csv', '')
    proceed_dir = f"data/staging/proceed/{date_part}"
    
    oltp_data = {}
    
    try:
        # Danh sách các file CSV cần đọc
        csv_files = [
            'raw_property_listings.csv',
            'raw_property_details.csv', 
            'raw_contact_info.csv',
            'property_address.csv',
            'extracted_features.csv',
            'property_media.csv',
            'raw_property_amenities.csv',
            'invalid_records.csv',
            'duplicate_records.csv'
        ]
        
        for csv_file in csv_files:
            file_path = os.path.join(proceed_dir, csv_file)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, encoding='utf-8')
                table_name = csv_file.replace('.csv', '')
                oltp_data[table_name] = df
                print(f"Đã đọc {len(df)} records từ {csv_file}")
            else:
                print(f"Không tìm thấy file: {file_path}")
                
        # Đọc thêm unified data nếu cần
        unified_path = os.path.join(proceed_dir, "unified_data.csv")
        if os.path.exists(unified_path):
            oltp_data['unified_data'] = pd.read_csv(unified_path, encoding='utf-8')
            
    except Exception as e:
        print(f"Lỗi khi đọc dữ liệu OLTP: {str(e)}")
        raise
        
    return oltp_data

def save_olap_data_to_files(olap_data, output_dir):
    """
    Lưu OLAP data thành các file CSV để backup/debug
    """
    try:
        olap_backup_dir = os.path.join(output_dir, "backup")
        os.makedirs(olap_backup_dir, exist_ok=True)
        
        for table_name, df in olap_data.items():
            if not df.empty:
                file_path = os.path.join(olap_backup_dir, f"{table_name}.csv")
                df.to_csv(file_path, index=False, encoding='utf-8')
                print(f"Đã lưu backup {table_name}: {len(df)} records")
                
    except Exception as e:
        print(f"Lỗi khi lưu backup OLAP data: {str(e)}")


def load_data_to_json(staging_data_path):
    """
    Hàm tải dữ liệu vào kho JSON cho mô hình dự đoán
    """
    json_loader = JsonLoader()
    
    # Tạo thư mục nếu chưa tồn tại
    os.makedirs("data/warehouse/json", exist_ok=True)
    
    json_loader.load(staging_data_path, "data/warehouse/json")
    return "data/warehouse/json"

def unify_data(websites, ds_nodash):
    """
    Hàm gộp dữ liệu từ nhiều nguồn đã được biến đổi và tổ chức thành các bảng riêng biệt
    Xử lý các trường bị trùng nhau giữa các bảng
    """
    # Thu thập dữ liệu đã biến đổi từ các nguồn
    raw_property_listings = []
    raw_property_details = []
    raw_contact_info = []
    property_address = []
    extracted_features = []
    property_media = []
    raw_property_amenities = []
    invalid_records = []
    duplicate_records = []
    
    for website in websites:
        staging_data_path = f"data/staging/{website['name']}/{website['name']}_{ds_nodash}.json"
        
        try:
            # Đọc dữ liệu đã biến đổi
            with open(staging_data_path, 'r', encoding='utf-8') as f:
                website_data = json.load(f)
                
            # Thu thập dữ liệu từ các nguồn khác nhau
            if 'raw_property_listings' in website_data:
                raw_property_listings.extend(website_data['raw_property_listings'])
            if 'raw_property_details' in website_data:
                raw_property_details.extend(website_data['raw_property_details'])
            if 'raw_contact_info' in website_data:
                raw_contact_info.extend(website_data['raw_contact_info'])
            if 'property_address' in website_data:
                property_address.extend(website_data['property_address'])
            if 'extracted_features' in website_data:
                extracted_features.extend(website_data['extracted_features'])
            if 'property_media' in website_data:
                property_media.extend(website_data['property_media'])
            if 'raw_property_amenities' in website_data:
                raw_property_amenities.extend(website_data['raw_property_amenities'])
            if 'invalid_records' in website_data:
                invalid_records.extend(website_data['invalid_records'])
            if 'duplicate_records' in website_data:
                duplicate_records.extend(website_data['duplicate_records'])
                
        except FileNotFoundError:
            print(f"Không tìm thấy file dữ liệu: {staging_data_path}")
        except json.JSONDecodeError:
            print(f"Lỗi định dạng JSON trong file: {staging_data_path}")
            
    # Tạo thư mục đầu ra cho dữ liệu thống nhất
    output_dir = f"data/staging/proceed/{ds_nodash}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Khởi tạo DataProcessor để xử lý dữ liệu
    processor = DataProcessor(output_dir=output_dir)
    
    # Chuyển đổi các danh sách thành DataFrame
    df_listings = pd.DataFrame(raw_property_listings) if raw_property_listings else pd.DataFrame()
    df_details = pd.DataFrame(raw_property_details) if raw_property_details else pd.DataFrame()
    df_contact = pd.DataFrame(raw_contact_info) if raw_contact_info else pd.DataFrame()
    df_address = pd.DataFrame(property_address) if property_address else pd.DataFrame()
    df_features = pd.DataFrame(extracted_features) if extracted_features else pd.DataFrame()
    df_media = pd.DataFrame(property_media) if property_media else pd.DataFrame()
    df_amenities = pd.DataFrame(raw_property_amenities) if raw_property_amenities else pd.DataFrame()
    df_invalid = pd.DataFrame(invalid_records) if invalid_records else pd.DataFrame()
    df_duplicate = pd.DataFrame(duplicate_records) if duplicate_records else pd.DataFrame()
    
    # Thêm các DataFrame đã xử lý vào processor
    processor.add_dataframe('raw_property_listings', df_listings)
    processor.add_dataframe('raw_property_details', df_details)
    processor.add_dataframe('raw_contact_info', df_contact)
    processor.add_dataframe('property_address', df_address)
    processor.add_dataframe('extracted_features', df_features)
    processor.add_dataframe('property_media', df_media)
    processor.add_dataframe('raw_property_amenities', df_amenities)
    processor.add_dataframe('invalid_records', df_invalid)
    processor.add_dataframe('duplicate_records', df_duplicate)
    
    # Lưu các DataFrame thành file CSV riêng biệt
    processor.save_to_csv()
    
    # Tạo unified DataFrame bằng cách kết hợp các bảng, xử lý các trường trùng lặp
    unified_df = create_unified_dataframe(df_listings, df_details, df_contact, df_address, df_features)
    
    # Lưu unified DataFrame vào thư mục đầu ra
    unified_path = os.path.join(output_dir, "unified_data.csv")
    if not unified_df.empty:
        unified_df.to_csv(unified_path, index=False, encoding='utf-8')
    
    # Lưu thêm một bản vào thư mục staging theo quy ước của DAG
    staging_path = f"data/staging/unified_data_{ds_nodash}.csv"
    if not unified_df.empty:
        unified_df.to_csv(staging_path, index=False, encoding='utf-8')

    load_data_to_mysql(ds_nodash)
    
    return processor.output_dir


def create_unified_dataframe(df_listings, df_details, df_contact, df_address, df_features):
    """
    Tạo DataFrame thống nhất từ các bảng với xử lý trùng lặp
    """
    if df_listings.empty:
        return pd.DataFrame()  # Trả về DataFrame rỗng nếu không có dữ liệu listings
    
    # Bắt đầu với bảng listings làm cơ sở
    unified_df = df_listings.copy()
    
    # Danh sách các trường có thể bị trùng lặp
    duplicate_fields = {
        'property_type': ['raw_property_listings', 'raw_property_details'],
        'description': ['raw_property_listings', 'raw_property_details'],
        'bedrooms': ['raw_property_listings', 'raw_property_details'],
        'bathrooms': ['raw_property_listings', 'raw_property_details'],
        'project': ['raw_property_listings', 'raw_property_details', 'property_address'],
        'city': ['raw_property_listings', 'property_address'],
        'district': ['raw_property_listings', 'property_address'],
        'ward': ['raw_property_listings', 'property_address'],
        'street': ['raw_property_listings', 'property_address'],
        'source_id': ['raw_property_listings', 'raw_property_details', 'property_address', 'extracted_features']
    }
    
    # Kết hợp với df_details, ưu tiên giá trị từ df_details cho các trường trùng lặp
    if not df_details.empty:
        # Đổi tên các cột trùng để tránh xung đột khi merge
        details_renamed = df_details.copy()
        
        # Xác định các cột trùng lặp giữa listings và details
        common_columns = set(unified_df.columns).intersection(set(details_renamed.columns)) - {'listing_id'}
        
        # Đổi tên các cột trùng
        rename_dict = {col: f"{col}_details" for col in common_columns}
        details_renamed.rename(columns=rename_dict, inplace=True)
        
        # Merge bảng
        unified_df = pd.merge(unified_df, details_renamed, on='listing_id', how='left')
        
        # Ưu tiên các giá trị từ bảng details cho các trường trùng
        for col in common_columns:
            col_details = f"{col}_details"
            # Chỉ cập nhật khi giá trị details không phải NaN và khác rỗng
            mask = (~unified_df[col_details].isna()) & (unified_df[col_details] != '')
            unified_df.loc[mask, col] = unified_df.loc[mask, col_details]
            # Xóa cột trùng lặp sau khi đã xử lý
            unified_df.drop(columns=[col_details], inplace=True)
    
    # Kết hợp với df_address
    if not df_address.empty:
        address_renamed = df_address.copy()
        common_columns = set(unified_df.columns).intersection(set(address_renamed.columns)) - {'listing_id'}
        rename_dict = {col: f"{col}_address" for col in common_columns}
        address_renamed.rename(columns=rename_dict, inplace=True)
        
        unified_df = pd.merge(unified_df, address_renamed, on='listing_id', how='left')
        
        # Ưu tiên giá trị từ bảng address cho các trường địa chỉ
        for col in common_columns:
            col_address = f"{col}_address"
            mask = (~unified_df[col_address].isna()) & (unified_df[col_address] != '')
            unified_df.loc[mask, col] = unified_df.loc[mask, col_address]
            unified_df.drop(columns=[col_address], inplace=True)
    
    # Kết hợp với df_features
    if not df_features.empty:
        features_renamed = df_features.copy()
        common_columns = set(unified_df.columns).intersection(set(features_renamed.columns)) - {'listing_id'}
        rename_dict = {col: f"{col}_features" for col in common_columns}
        features_renamed.rename(columns=rename_dict, inplace=True)
        
        unified_df = pd.merge(unified_df, features_renamed, on='listing_id', how='left')
        
        # Ưu tiên giá trị từ bảng features
        for col in common_columns:
            col_features = f"{col}_features"
            mask = (~unified_df[col_features].isna()) & (unified_df[col_features] != '')
            unified_df.loc[mask, col] = unified_df.loc[mask, col_features]
            unified_df.drop(columns=[col_features], inplace=True)
    
    # Kết hợp với df_contact
    if not df_contact.empty:
        # Để đơn giản, giữ nguyên cột contact_id vì nó không trùng với các bảng khác
        unified_df = pd.merge(unified_df, df_contact, on='listing_id', how='left')
    
    # Loại bỏ các cột trùng lặp không mong muốn
    unified_df = unified_df.loc[:, ~unified_df.columns.duplicated()]
    
    return unified_df


def load_data_to_postgres(ds_nodash):
    """
    Hàm tải dữ liệu vào PostgreSQL sử dụng Airflow Connection
    """
    try:
        # Sử dụng PostgresHook để kết nối với database
        postgres_hook = postgres_hook(postgres_conn_id='postgres_real_estate')
        
        # Lấy SQLAlchemy engine từ hook
        engine = postgres_hook.get_sqlalchemy_engine()
        
        # Đường dẫn đến thư mục dữ liệu đã xử lý
        processed_dir = f"data/staging/proceed/{ds_nodash}"
        
        # Tìm thư mục có timestamp mới nhất
        timestamped_dirs = [d for d in os.listdir(processed_dir) if os.path.isdir(os.path.join(processed_dir, d))]
        if not timestamped_dirs:
            raise ValueError(f"Không tìm thấy thư mục dữ liệu trong {processed_dir}")
        
        # Lấy thư mục mới nhất (sắp xếp theo tên - timestamp)
        latest_dir = sorted(timestamped_dirs)[-1]
        data_dir = os.path.join(processed_dir, latest_dir)
        
        logging.info(f"Đang tải dữ liệu từ: {data_dir}")
        
        # Danh sách các bảng cần tạo với cấu trúc
        table_schemas = {
            'raw_property_listings': create_listings_table_sql(),
            'raw_property_details': create_details_table_sql(),
            'raw_contact_info': create_contact_table_sql(),
            'property_address': create_address_table_sql(),
            'extracted_features': create_features_table_sql(),
            # 'property_media': create_media_table_sql(),
            'raw_property_amenities': create_amenities_table_sql(),
            'invalid_records': create_invalid_records_table_sql(),
            'duplicate_records': create_duplicate_records_table_sql(),
            'unified_data': create_unified_table_sql()
        }
        
        # Tạo các bảng nếu chưa tồn tại
        with engine.connect() as conn:
            for table_name, create_sql in table_schemas.items():
                try:
                    conn.execute(text(create_sql))
                    conn.commit()
                    logging.info(f"Đã tạo/kiểm tra bảng: {table_name}")
                except Exception as e:
                    logging.warning(f"Lỗi khi tạo bảng {table_name}: {str(e)}")
        
        # Load dữ liệu từ các file CSV vào PostgreSQL
        loaded_tables = []
        for csv_file in os.listdir(data_dir):
            if csv_file.endswith('.csv'):
                table_name = os.path.splitext(csv_file)[0]
                file_path = os.path.join(data_dir, csv_file)
                
                try:
                    # Đọc CSV với xử lý lỗi encoding
                    df = pd.read_csv(file_path, encoding='utf-8')
                    
                    if df.empty:
                        logging.warning(f"File {csv_file} trống, bỏ qua")
                        continue
                    
                    # Làm sạch tên cột (loại bỏ ký tự đặc biệt, khoảng trắng)
                    df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() 
                                for col in df.columns]
                    
                    # Thêm cột batch_date để tracking
                    df['batch_date'] = ds_nodash
                    df['created_at'] = datetime.now()
                    
                    # Load dữ liệu vào PostgreSQL
                    # Sử dụng 'replace' để thay thế dữ liệu cũ, hoặc 'append' để thêm vào
                    df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='append',  # Có thể thay đổi thành 'replace' nếu muốn thay thế
                        index=False,
                        method='multi',  # Tăng hiệu suất insert
                        chunksize=1000   # Insert theo batch để tránh memory issue
                    )
                    
                    loaded_tables.append(table_name)
                    logging.info(f"Đã load {len(df)} bản ghi vào bảng {table_name}")
                    
                except Exception as e:
                    logging.error(f"Lỗi khi load file {csv_file}: {str(e)}")
                    continue
        
        # Tạo indexes để tăng hiệu suất query
        create_indexes(engine)
        
        # Log thống kê
        with engine.connect() as conn:
            for table_name in loaded_tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.fetchone()[0]
                    logging.info(f"Bảng {table_name}: {count} bản ghi")
                except Exception as e:
                    logging.warning(f"Không thể đếm bản ghi trong bảng {table_name}: {str(e)}")
        
        return f"Đã tải thành công {len(loaded_tables)} bảng vào PostgreSQL: {', '.join(loaded_tables)}"
        
    except Exception as e:
        logging.error(f"Lỗi khi tải dữ liệu vào PostgreSQL: {str(e)}")
        raise


def load_data_to_mysql(ds_nodash):
    """
    Hàm tải dữ liệu vào MySQL sử dụng Airflow Connection
    """
    try:
        # Kết nối với MySQL qua Airflow Connection
        mysql_hook = MySqlHook(mysql_conn_id='mysql_real_estate')

        # Lấy SQLAlchemy engine
        engine = mysql_hook.get_sqlalchemy_engine()

        # Đường dẫn thư mục dữ liệu
        processed_dir = f"data/staging/proceed/{ds_nodash}"
        # timestamped_dirs = [d for d in os.listdir(processed_dir) if os.path.isdir(os.path.join(processed_dir, d))]
        # if not timestamped_dirs:
        #     raise ValueError(f"Không tìm thấy thư mục dữ liệu trong {processed_dir}")
        
        # latest_dir = sorted(timestamped_dirs)[-1]
        # data_dir = os.path.join(processed_dir, latest_dir)
        data_dir = processed_dir

        logging.info(f"Đang tải dữ liệu từ: {data_dir}")

        # Danh sách bảng cần tạo
        table_schemas = {
            'raw_property_listings': create_listings_table_sql(),
            'raw_property_details': create_details_table_sql(),
            'raw_contact_info': create_contact_table_sql(),
            'property_address': create_address_table_sql(),
            'extracted_features': create_features_table_sql(),
            'raw_property_amenities': create_amenities_table_sql(),
            'invalid_records': create_invalid_records_table_sql(),
            'duplicate_records': create_duplicate_records_table_sql(),
            'unified_data': create_unified_table_sql()
        }

        # Tạo bảng
        with engine.connect() as conn:
            for table_name, create_sql in table_schemas.items():
                try:
                    conn.execute(text(create_sql))
                    # conn.commit()
                    logging.info(f"Đã tạo/kiểm tra bảng: {table_name}")
                except Exception as e:
                    logging.warning(f"Lỗi khi tạo bảng {table_name}: {str(e)}")

        # Load CSV vào MySQL
        loaded_tables = []
        for csv_file in os.listdir(data_dir):
            if csv_file.endswith('.csv'):
                table_name = os.path.splitext(csv_file)[0]
                file_path = os.path.join(data_dir, csv_file)

                try:
                    df = pd.read_csv(file_path, encoding='utf-8')

                    if df.empty:
                        logging.warning(f"File {csv_file} trống, bỏ qua")
                        continue

                    df.columns = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in df.columns]
                    df['batch_date'] = ds_nodash
                    df['created_at'] = datetime.now()

                    df.to_sql(
                        name=table_name,
                        con=engine,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000
                    )

                    loaded_tables.append(table_name)
                    logging.info(f"Đã load {len(df)} bản ghi vào bảng {table_name}")

                except Exception as e:
                    logging.error(f"Lỗi khi load file {csv_file}: {str(e)}")
                    continue

        # Tạo index nếu cần (tuỳ bạn định nghĩa create_indexes)
        # create_indexes(engine)

        with engine.connect() as conn:
            for table_name in loaded_tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.fetchone()[0]
                    logging.info(f"Bảng {table_name}: {count} bản ghi")
                except Exception as e:
                    logging.warning(f"Không thể đếm bản ghi trong bảng {table_name}: {str(e)}")

        return f"Đã tải thành công {len(loaded_tables)} bảng vào MySQL: {', '.join(loaded_tables)}"

    except Exception as e:
        logging.error(f"Lỗi khi tải dữ liệu vào MySQL: {str(e)}")
        raise
def create_listings_table_sql():
    """Tạo SQL cho bảng raw_property_listings (phiên bản MySQL)"""
    return """
    CREATE TABLE IF NOT EXISTS raw_property_listings (
        id VARCHAR(200) AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(255) NOT NULL UNIQUE,
        bds_id VARCHAR(255),
        source_id VARCHAR(50),
        url TEXT,
        title TEXT,
        scraped_at DATETIME,
        price_text VARCHAR(255),
        price_value DECIMAL(15,2),
        price_unit VARCHAR(50),
        price_per_m2 DECIMAL(15,2),
        area_text VARCHAR(255),
        area_value DECIMAL(10,2),
        address TEXT,
        post_date DATETIME,
        expiration_date DATETIME,
        post_type VARCHAR(100),
        property_type VARCHAR(100),
        is_available BOOLEAN,
        raw_data JSON,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """

def create_details_table_sql():
    """Tạo SQL cho bảng raw_property_details (phiên bản MySQL)"""
    return """
    CREATE TABLE IF NOT EXISTS raw_property_details (
        id VARCHAR(200) AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(255),
        property_details VARCHAR(100),
        description TEXT,
        description_length INT,
        description_word_count INT,
        bedrooms INT,
        bedrooms_value DECIMAL(5,2),
        bathrooms INT,
        bathrooms_value DECIMAL(5,2),
        legal_status VARCHAR(100),
        floors INT,
        floors_value DECIMAL(5,2),
        direction VARCHAR(100),
        balcony_direction VARCHAR(100),
        road_width VARCHAR(100),
        road_width_value DECIMAL(5,2),
        house_front VARCHAR(100),
        house_front_value DECIMAL(5,2),
        width DECIMAL(5,2),
        width_value DECIMAL(5,2),
        length DECIMAL(5,2),
        length_value DECIMAL(5,2),
        project VARCHAR(255),
        furniture VARCHAR(100),
        raw_details JSON,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (listing_id) REFERENCES raw_property_listings(listing_id)
    );
    """

def create_contact_table_sql():
    """Tạo SQL cho bảng raw_contact_info (phiên bản MySQL)"""
    return """
    CREATE TABLE IF NOT EXISTS raw_contact_info (
        id VARCHAR(36) PRIMARY KEY,  -- UUID
        listing_id VARCHAR(255),
        contact_name VARCHAR(255),
        contact_phone VARCHAR(50),
        contact_phone_extracted VARCHAR(50),
        is_phone_masked BOOLEAN,
        contact_email VARCHAR(255),
        contact_address TEXT,
        contact_company VARCHAR(255),
        is_agent BOOLEAN,
        raw_contact JSON,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (listing_id) REFERENCES raw_property_listings(listing_id)
    );
    """
def create_address_table_sql():
    """Tạo SQL cho bảng property_address (MySQL style)"""
    return """
    CREATE TABLE IF NOT EXISTS property_address (
        id VARCHAR(36) PRIMARY KEY,  -- UUID nếu cần
        listing_id VARCHAR(255),
        source_id VARCHAR(50),
        full_address TEXT,
        city VARCHAR(100),
        district VARCHAR(100),
        ward VARCHAR(100),
        street VARCHAR(255),
        project_name VARCHAR(255),
        address_normalized BOOLEAN,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (listing_id) REFERENCES raw_property_listings(listing_id)
    );
    """


def create_features_table_sql():
    """Tạo SQL cho bảng extracted_features (MySQL style)"""
    return """
    CREATE TABLE IF NOT EXISTS extracted_features (
        id VARCHAR(36) PRIMARY KEY,  -- UUID nếu dùng
        listing_id VARCHAR(255),
        source_id VARCHAR(50),
        amenities_list JSON,
        amenities_count INT,
        view_types JSON,
        discount_percentage DECIMAL(5,2),
        loan_support_percentage DECIMAL(5,2),
        promotions JSON,
        has_promotions BOOLEAN,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (listing_id) REFERENCES raw_property_listings(listing_id)
    );
    """

# def create_media_table_sql():
#     """Tạo SQL cho bảng property_media"""
#     return """
#     CREATE TABLE IF NOT EXISTS property_media (
#         id SERIAL PRIMARY KEY,
#         listing_id VARCHAR(255),
#         media_type VARCHAR(50),
#         media_url TEXT,
#         media_title VARCHAR(255),
#         media_description TEXT,
#         display_order INTEGER,
#         batch_date VARCHAR(20),
#         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#         FOREIGN KEY (listing_id) REFERENCES raw_property_listings(listing_id)
#     );
#     """

def create_amenities_table_sql():
    """Tạo SQL cho bảng raw_property_amenities (MySQL style)"""
    return """
    CREATE TABLE IF NOT EXISTS raw_property_amenities (
        id VARCHAR(200) AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(255),
        source_id VARCHAR(50),
        amenity_name VARCHAR(255),
        amenity_category VARCHAR(100),
        amenity_description TEXT,
        amenity_value VARCHAR(255),
        raw_amenity JSON,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (listing_id) REFERENCES raw_property_listings(listing_id)
    );
    """

def create_invalid_records_table_sql():
    """Tạo SQL cho bảng invalid_records (MySQL style)"""
    return """
    CREATE TABLE IF NOT EXISTS invalid_records (
        id VARCHAR(200) AUTO_INCREMENT PRIMARY KEY,
        scraping_id VARCHAR(50),
        listing_id VARCHAR(255),
        source_url TEXT,
        status VARCHAR(20),
        reason TEXT,
        processing_time VARCHAR(50),
        error_detail TEXT,
        timestamp DATETIME,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """

def create_duplicate_records_table_sql():
    """Tạo SQL cho bảng duplicate_records (MySQL style)"""
    return """
    CREATE TABLE IF NOT EXISTS duplicate_records (
        id VARCHAR(200) AUTO_INCREMENT PRIMARY KEY,
        original_listing_id VARCHAR(255),
        duplicate_listing_id VARCHAR(255),
        similarity_score DECIMAL(5,4),
        duplicate_reason TEXT,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """

def create_unified_table_sql():
    """Tạo SQL cho bảng unified_data (MySQL style)"""
    return """
    CREATE TABLE IF NOT EXISTS unified_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        listing_id VARCHAR(255) UNIQUE,
        title TEXT,
        price DECIMAL(15,2),
        area DECIMAL(10,2),
        property_type VARCHAR(100),
        bedrooms INT,
        bathrooms INT,
        street VARCHAR(255),
        ward VARCHAR(255),
        district VARCHAR(255),
        city VARCHAR(255),
        description TEXT,
        contact_name VARCHAR(255),
        phone_number VARCHAR(50),
        source VARCHAR(50),
        url TEXT,
        batch_date VARCHAR(20),
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """

def create_indexes(engine):
    """Tạo các indexes để tăng hiệu suất"""
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_listings_listing_id ON raw_property_listings(listing_id);",
        "CREATE INDEX IF NOT EXISTS idx_listings_source ON raw_property_listings(source);",
        "CREATE INDEX IF NOT EXISTS idx_listings_property_type ON raw_property_listings(property_type);",
        "CREATE INDEX IF NOT EXISTS idx_listings_location ON raw_property_listings(location);",
        "CREATE INDEX IF NOT EXISTS idx_listings_price ON raw_property_listings(price);",
        "CREATE INDEX IF NOT EXISTS idx_listings_batch_date ON raw_property_listings(batch_date);",
        "CREATE INDEX IF NOT EXISTS idx_address_city ON property_address(city);",
        "CREATE INDEX IF NOT EXISTS idx_address_district ON property_address(district);",
        "CREATE INDEX IF NOT EXISTS idx_contact_listing_id ON raw_contact_info(listing_id);",
        "CREATE INDEX IF NOT EXISTS idx_details_listing_id ON raw_property_details(listing_id);",
        "CREATE INDEX IF NOT EXISTS idx_unified_property_type ON unified_data(property_type);",
        "CREATE INDEX IF NOT EXISTS idx_unified_city ON unified_data(city);",
        "CREATE INDEX IF NOT EXISTS idx_unified_price ON unified_data(price);",
    ]
    
    with engine.connect() as conn:
        for index_sql in indexes:
            try:
                conn.execute(text(index_sql))
                conn.commit()
                logging.info(f"Đã tạo index: {index_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                logging.warning(f"Lỗi khi tạo index: {str(e)}")
