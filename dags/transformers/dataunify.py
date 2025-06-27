from datetime import datetime
import logging
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import json
from sqlalchemy import text
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
class DataProcessor:
    def __init__(self, output_dir="processed_data"):
        """
        Khởi tạo class DataProcessor
        
        Args:
            output_dir (str): Thư mục lưu trữ dữ liệu đã xử lý
        """
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # # Tạo thư mục với timestamp để tránh ghi đè
        # self.timestamped_dir = os.path.join(output_dir)
        # os.makedirs(self.timestamped_dir)
        
        # Dict lưu trữ các DataFrame
        self.dataframes = {}
    
    def add_dataframe(self, name, df):
        """
        Thêm DataFrame vào bộ xử lý
        
        Args:
            name (str): Tên của DataFrame
            df (pd.DataFrame): DataFrame cần xử lý
        """
        if df is not None and not df.empty:
            self.dataframes[name] = df
    
    def save_to_csv(self):
        """
        Lưu các DataFrame thành file CSV riêng biệt
        """
        for name, df in self.dataframes.items():
            if not df.empty:
                file_path = os.path.join(self.output_dir, f"{name}.csv")
                df.to_csv(file_path, index=False, encoding='utf-8')
                print(f"Đã lưu {name}.csv với {len(df)} dòng")
    
    def create_unified_csv(self):
        """
        Tạo file CSV tổng hợp từ tất cả các DataFrame
        """
        all_dataframes = []
        
        # Thêm cột nguồn vào mỗi DataFrame
        for name, df in self.dataframes.items():
            if not df.empty:
                df_copy = df.copy()
                df_copy['source_table'] = name
                all_dataframes.append(df_copy)
        
        if all_dataframes:
            # Nối tất cả DataFrame lại với nhau
            unified_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Lưu DataFrame thống nhất
            unified_file_path = os.path.join(self.timestamped_dir, "unified_data.csv")
            unified_df.to_csv(unified_file_path, index=False, encoding='utf-8')
            print(f"Đã tạo unified_data.csv với {len(unified_df)} dòng")
            
            return unified_df
        else:
            print("Không có dữ liệu để tạo file thống nhất")
            return pd.DataFrame()
    
    def save_to_postgres(self, connection_string):
        """
        Lưu các DataFrame vào cơ sở dữ liệu PostgreSQL
        
        Args:
            connection_string (str): Chuỗi kết nối đến PostgreSQL
        """
        try:
            # Tạo kết nối đến PostgreSQL
            engine = create_engine(connection_string)
            
            # Lưu từng DataFrame vào PostgreSQL
            for name, df in self.dataframes.items():
                if not df.empty:
                    # Xử lý các loại dữ liệu đặc biệt nếu cần
                    df_clean = df.copy()
                    
                    # Xử lý các cột dạng list/dict thành chuỗi JSON
                    for col in df_clean.columns:
                        if df_clean[col].apply(lambda x: isinstance(x, (list, dict))).any():
                            df_clean[col] = df_clean[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
                    
                    # Lưu vào PostgreSQL
                    df_clean.to_sql(
                        name, 
                        engine, 
                        if_exists='replace',
                        index=False,
                        schema='public'
                    )
                    print(f"Đã lưu bảng {name} vào PostgreSQL với {len(df)} dòng")
                    
            print("Đã hoàn tất việc lưu dữ liệu vào PostgreSQL")
            
        except Exception as e:
            print(f"Lỗi khi lưu vào PostgreSQL: {str(e)}")


class UnifiedDataProcessor:
    def __init__(self, output_dir="unified_data"):
        """
        Khởi tạo class UnifiedDataProcessor
        
        Args:
            output_dir (str): Thư mục lưu trữ dữ liệu đã xử lý
        """
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Tạo thư mục đầu ra nếu chưa tồn tại
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        self.data_processor = DataProcessor(output_dir=output_dir)
    
    def add_dataframe(self, name, df):
        """
        Thêm DataFrame vào bộ xử lý
        
        Args:
            name (str): Tên của DataFrame
            df (pd.DataFrame): DataFrame cần xử lý
        """
        self.data_processor.add_dataframe(name, df)
    
    def unify_data(self, websites, ds_nodash):
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
        unified_df = self.create_unified_dataframe(df_listings, df_details, df_contact, df_address, df_features)
        
        # Lưu unified DataFrame vào thư mục đầu ra
        unified_path = os.path.join(output_dir, "unified_data.csv")
        if not unified_df.empty:
            unified_df.to_csv(unified_path, index=False, encoding='utf-8')
        
        # Lưu thêm một bản vào thư mục staging theo quy ước của DAG
        staging_path = f"data/staging/unified_data_{ds_nodash}.csv"
        if not unified_df.empty:
            unified_df.to_csv(staging_path, index=False, encoding='utf-8')

        self.load_data_to_mysql(ds_nodash)
        
        return processor.output_dir


    def create_unified_dataframe(self, df_listings, df_details, df_contact, df_address, df_features):
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


    def load_data_to_mysql(self, ds_nodash):
        """
        Hàm tải dữ liệu vào MySQL sử dụng Airflow Connection
        """
        try:
            # Kết nối với MySQL qua Airflow Connection
            conn = BaseHook.get_connection("mysql_real_estate")

            # Tạo connection string thủ công dùng pymysql
            connection_string = f"mysql+pymysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            engine = create_engine(connection_string)

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
                'raw_property_listings': self.create_listings_table_sql(),
                'raw_property_details': self.create_details_table_sql(),
                'raw_contact_info': self.create_contact_table_sql(),
                'property_address': self.create_address_table_sql(),
                'extracted_features':self.create_features_table_sql(),
                'raw_property_amenities': self.create_amenities_table_sql(),
                'invalid_records': self.create_invalid_records_table_sql(),
                'duplicate_records': self.create_duplicate_records_table_sql(),
                'unified_data': self.create_unified_table_sql()
            }

            # Tạo bảng
            with engine.connect() as conn:
                from sqlalchemy import inspect
                inspector = inspect(engine)
                for table_name, create_sql in table_schemas.items():
                    try:
                        if table_name in inspector.get_table_names():
                            logging.info(f"Bảng {table_name} đã tồn tại, bỏ qua.")
                            continue
                        conn.execute(text(create_sql))
                        # conn.commit()
                        logging.info(f"Đã tạo/kiểm tra bảng: {table_name}")
                    except Exception as e:
                        logging.info(f"Lỗi khi tạo bảng {table_name}: {str(e)}")

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
    def create_listings_table_sql(self):
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

    def create_details_table_sql(self):
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

    def create_contact_table_sql(self):
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
    def create_address_table_sql(self):
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


    def create_features_table_sql(self):
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

    def create_amenities_table_sql(self):
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

    def create_invalid_records_table_sql(self):
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

    def create_duplicate_records_table_sql(self):
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

    def create_unified_table_sql(self):
        """Tạo SQL cho bảng unified_data với đầy đủ schema (MySQL style)"""
        return """
        CREATE TABLE IF NOT EXISTS unified_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            listing_id VARCHAR(255) UNIQUE,
            bds_id VARCHAR(255),
            source_id VARCHAR(50),
            url TEXT,
            title TEXT,
            scraped_at DATETIME,
            price_text VARCHAR(100),
            price_value DECIMAL(15, 2),
            price_unit VARCHAR(50),
            price_per_m2 DECIMAL(15, 2),
            area_text VARCHAR(100),
            area_value DECIMAL(10, 2),
            address TEXT,
            post_date DATETIME,
            expiration_date DATETIME,
            post_type VARCHAR(100),
            property_type VARCHAR(100),
            is_available BOOLEAN,
            raw_data VARCHAR(500),
            property_details VARCHAR(500),
            description TEXT,
            description_length INT,
            description_word_count INT,
            bedrooms VARCHAR(50),
            bedrooms_value INT,
            bathrooms VARCHAR(50),
            bathrooms_value INT,
            legal_status VARCHAR(100),
            floors VARCHAR(50),
            floors_value INT,
            direction VARCHAR(100),
            balcony_direction VARCHAR(100),
            road_width VARCHAR(50),
            road_width_value DECIMAL(10,2),
            house_front VARCHAR(50),
            house_front_value DECIMAL(10,2),
            width VARCHAR(50),
            width_value DECIMAL(10,2),
            length VARCHAR(50),
            length_value DECIMAL(10,2),
            project VARCHAR(255),
            furniture VARCHAR(255),
            raw_details VARCHAR(255),
            full_address TEXT,
            city VARCHAR(100),
            district VARCHAR(100),
            ward VARCHAR(100),
            street VARCHAR(255),
            project_name VARCHAR(255),
            address_normalized TEXT,
            amenities_list VARCHAR(255),
            amenities_count INT,
            view_types VARCHAR(255),
            discount_percentage DECIMAL(5,2),
            loan_support_percentage DECIMAL(5,2),
            promotions VARCHAR(255),
            has_promotions BOOLEAN,
            contact_id VARCHAR(255),
            contact_name VARCHAR(255),
            contact_phone VARCHAR(50),
            contact_phone_extracted VARCHAR(50),
            is_phone_masked BOOLEAN,
            contact_email VARCHAR(255),
            contact_address VARCHAR(255),
            contact_company VARCHAR(255),
            is_agent BOOLEAN,
            raw_contact VARCHAR(255),
            batch_date VARCHAR(20),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """

    def create_indexes(self, engine):
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

    