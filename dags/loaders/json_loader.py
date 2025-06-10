import os
import json
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class JsonLoader:
    """
    Lớp tải dữ liệu vào kho dữ liệu JSON cho mô hình dự đoán
    """
    
    def __init__(self):
        pass
    
    def load(self, staging_data_path, warehouse_json_dir):
        """
        Tải dữ liệu từ vùng staging vào kho dữ liệu JSON
        
        Args:
            staging_data_path: Đường dẫn đến file CSV chứa dữ liệu đã biến đổi
            warehouse_json_dir: Thư mục chứa kho dữ liệu JSON
            
        Returns:
            bool: True nếu tải thành công, False nếu có lỗi
        """
        try:
            # Đảm bảo thư mục tồn tại
            os.makedirs(warehouse_json_dir, exist_ok=True)
            
            # Đọc dữ liệu từ file CSV
            df = pd.read_csv(staging_data_path)
            
            # Tạo cấu trúc dữ liệu cho JSON
            json_data = self._prepare_json_data(df)
            
            # Lưu vào file JSON
            date_str = datetime.now().strftime('%Y%m%d')
            json_file_path = os.path.join(warehouse_json_dir, f"real_estate_data_{date_str}.json")
            
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            # Tạo file metadata cho ML
            self._create_metadata_file(df, warehouse_json_dir)
            
            logger.info(f"Đã tải dữ liệu vào kho JSON: {json_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu vào kho JSON: {e}")
            return False
    
    def _prepare_json_data(self, df):
        """
        Chuẩn bị dữ liệu cho định dạng JSON
        
        Args:
            df: DataFrame chứa dữ liệu đã biến đổi
            
        Returns:
            dict: Dữ liệu được cấu trúc cho JSON
        """
        # Lọc dữ liệu cho mô hình dự đoán (chỉ giữ các dòng có đầy đủ thông tin)
        prediction_df = df.dropna(subset=['price_normalized', 'area_normalized', 'district', 'city'])
        
        # Chuyển đổi df thành danh sách các dictionary
        properties = []
        
        for _, row in prediction_df.iterrows():
            property_dict = {
                'title': row.get('title'),
                'price': float(row.get('price_normalized')) if pd.notna(row.get('price_normalized')) else None,
                'area': float(row.get('area_normalized')) if pd.notna(row.get('area_normalized')) else None,
                'price_per_sqm': float(row.get('price_per_sqm')) if pd.notna(row.get('price_per_sqm')) else None,
                'address': row.get('address'),
                'district': row.get('district'),
                'city': row.get('city'),
                'source': row.get('source'),
                'url': row.get('url'),
                'date': row.get('date'),
                'features': {
                    # Thêm các tính năng khác nếu có
                    'bedrooms': int(row.get('bedrooms')) if pd.notna(row.get('bedrooms')) else None,
                    'bathrooms': int(row.get('bathrooms')) if pd.notna(row.get('bathrooms')) else None,
                }
            }
            
            properties.append(property_dict)
        
        # Cấu trúc dữ liệu cho file JSON
        json_data = {
            'metadata': {
                'total_properties': len(properties),
                'collection_date': datetime.now().isoformat(),
                'sources': prediction_df['source'].unique().tolist()
            },
            'properties': properties
        }
        
        return json_data
    
    def _create_metadata_file(self, df, warehouse_json_dir):
        """
        Tạo file metadata cho mô hình học máy
        
        Args:
            df: DataFrame chứa dữ liệu
            warehouse_json_dir: Thư mục chỉ định
        """
        # Tính toán các thống kê cơ bản
        stats = {
            'price': {
                'mean': df['price_normalized'].mean(),
                'median': df['price_normalized'].median(),
                'min': df['price_normalized'].min(),
                'max': df['price_normalized'].max(),
                'std': df['price_normalized'].std()
            },
            'area': {
                'mean': df['area_normalized'].mean(),
                'median': df['area_normalized'].median(),
                'min': df['area_normalized'].min(),
                'max': df['area_normalized'].max(),
                'std': df['area_normalized'].std()
            },
            'price_per_sqm': {
                'mean': df['price_per_sqm'].mean(),
                'median': df['price_per_sqm'].median(),
                'min': df['price_per_sqm'].min(),
                'max': df['price_per_sqm'].max(),
                'std': df['price_per_sqm'].std()
            }
        }
        
        # Thống kê phân bố theo khu vực
        district_counts = df['district'].value_counts().to_dict()
        city_counts = df['city'].value_counts().to_dict()
        
        metadata = {
            'stats': stats,
            'distributions': {
                'district': district_counts,
                'city': city_counts
            },
            'feature_columns': [
                'price_normalized', 'area_normalized', 'district', 'city',
                'bedrooms', 'bathrooms'
            ],
            'target_column': 'price_normalized',
            'updated_at': datetime.now().isoformat()
        }
        
        # Lưu file metadata
        metadata_path = os.path.join(warehouse_json_dir, 'metadata.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Đã tạo file metadata tại: {metadata_path}")