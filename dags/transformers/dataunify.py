from datetime import datetime
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import json

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
