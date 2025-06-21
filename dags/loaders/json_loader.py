import os
import json
import pandas as pd
import logging
from datetime import datetime
import pandas as pd
import pymongo
from pymongo import MongoClient
from datetime import datetime
import logging
import os
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class JsonLoader:
    """
    Lớp tải dữ liệu vào kho dữ liệu JSON cho mô hình dự đoán
    """
    
    def __init__(self):
        pass
    
    def load(self, staging_data_path: str, mongodb_connection_string: str, 
            database_name: str = "real_estate_warehouse", 
            collection_name: str = "properties") -> bool:
        """
        Tải dữ liệu từ vùng staging vào MongoDB Atlas
        
        Args:
            staging_data_path: Đường dẫn đến file CSV chứa dữ liệu đã biến đổi
            mongodb_connection_string: Connection string của MongoDB Atlas
            database_name: Tên database (mặc định: real_estate_warehouse)
            collection_name: Tên collection (mặc định: properties)
            
        Returns:
            bool: True nếu tải thành công, False nếu có lỗi
        """
        client = None
        try:
            # Kết nối đến MongoDB Atlas
            client = MongoClient(mongodb_connection_string)
            
            # Kiểm tra kết nối
            client.admin.command('ping')
            logger.info("Kết nối MongoDB Atlas thành công")
            
            # Chọn database và collection
            db = client[database_name]
            collection = db[collection_name]
            
            # Đọc dữ liệu từ file CSV
            df = pd.read_csv(staging_data_path)
            logger.info(f"Đã đọc {len(df)} bản ghi từ file CSV")
            
            # Chuẩn bị dữ liệu cho MongoDB
            documents = self._prepare_mongodb_documents(df)
            
            # Thêm metadata cho mỗi document
            current_time = datetime.now()
            for doc in documents:
                doc['_metadata'] = {
                    'loaded_at': current_time,
                    'source_file': os.path.basename(staging_data_path),
                    'batch_id': f"batch_{current_time.strftime('%Y%m%d_%H%M%S')}"
                }
            
            # Insert dữ liệu vào MongoDB
            if documents:
                # Sử dụng insert_many cho hiệu suất tốt hơn
                result = collection.insert_many(documents, ordered=False)
                logger.info(f"Đã chèn {len(result.inserted_ids)} documents vào MongoDB")
                
                # Tạo index để tối ưu truy vấn
                self._create_indexes(collection)
                
                # Lưu thống kê vào collection metadata
                self._save_load_statistics(db, len(documents), current_time)
                
                logger.info("Tải dữ liệu vào MongoDB Atlas thành công")
                return True
            else:
                logger.warning("Không có dữ liệu để tải")
                return False
                
        except pymongo.errors.ConnectionFailure as e:
            logger.error(f"Lỗi kết nối MongoDB: {e}")
            return False
        except pymongo.errors.BulkWriteError as e:
            logger.error(f"Lỗi bulk write: {e}")
            # Có thể một số documents đã được chèn thành công
            return False
        except Exception as e:
            logger.error(f"Lỗi khi tải dữ liệu vào MongoDB: {e}")
            return False
        finally:
            # Đóng kết nối
            if client:
                client.close()

    def _prepare_mongodb_documents(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Chuẩn bị dữ liệu DataFrame thành list documents cho MongoDB
        
        Args:
            df: DataFrame chứa dữ liệu
            
        Returns:
            List[Dict]: Danh sách documents
        """
        documents = []
        
        for _, row in df.iterrows():
            # Chuyển đổi Series thành dict và xử lý NaN values
            doc = row.to_dict()
            
            # Xử lý NaN values (MongoDB không hỗ trợ NaN)
            for key, value in doc.items():
                if pd.isna(value):
                    doc[key] = None
                elif isinstance(value, (int, float)) and pd.isna(value):
                    doc[key] = None
            
            # Thêm các trường bổ sung nếu cần
            doc = self._enrich_document(doc)
            
            documents.append(doc)
        
        return documents

    def _enrich_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Làm giàu document với các trường bổ sung
        
        Args:
            doc: Document gốc
            
        Returns:
            Dict: Document đã được làm giàu
        """
        # Thêm các trường tính toán hoặc phân loại
        if 'price' in doc and doc['price']:
            doc['price_category'] = self._categorize_price(doc['price'])
        
        if 'area' in doc and doc['area']:
            doc['area_category'] = self._categorize_area(doc['area'])
        
        # Thêm geolocation nếu có thông tin địa chỉ
        if 'address' in doc:
            doc['location'] = {
                'address': doc['address'],
                'coordinates': None  # Có thể thêm geocoding sau
            }
        
        return doc

    def _categorize_price(self, price: float) -> str:
        """Phân loại giá theo khoảng"""
        if price < 1000000000:  # < 1 tỷ
            return "budget"
        elif price < 3000000000:  # < 3 tỷ
            return "mid_range"
        elif price < 10000000000:  # < 10 tỷ
            return "premium"
        else:
            return "luxury"

    def _categorize_area(self, area: float) -> str:
        """Phân loại diện tích"""
        if area < 50:
            return "small"
        elif area < 100:
            return "medium"
        elif area < 200:
            return "large"
        else:
            return "extra_large"

    def _create_indexes(self, collection) -> None:
        """
        Tạo các index để tối ưu truy vấn
        
        Args:
            collection: MongoDB collection
        """
        try:
            # Index cho các trường thường được query
            indexes = [
                [("price", 1)],
                [("area", 1)],
                [("location.address", "text")],
                [("_metadata.loaded_at", -1)],
                [("_metadata.batch_id", 1)],
                [("price_category", 1), ("area_category", 1)]  # Compound index
            ]
            
            for index_keys in indexes:
                try:
                    collection.create_index(index_keys)
                    logger.info(f"Đã tạo index: {index_keys}")
                except pymongo.errors.OperationFailure as e:
                    logger.warning(f"Không thể tạo index {index_keys}: {e}")
                    
        except Exception as e:
            logger.error(f"Lỗi khi tạo indexes: {e}")

    def _save_load_statistics(self, db, record_count: int, load_time: datetime) -> None:
        """
        Lưu thông tin thống kê về quá trình load
        
        Args:
            db: MongoDB database
            record_count: Số lượng bản ghi đã load
            load_time: Thời gian load
        """
        try:
            stats_collection = db['load_statistics']
            stats_doc = {
                'load_time': load_time,
                'record_count': record_count,
                'status': 'success',
                'collection': 'properties'
            }
            stats_collection.insert_one(stats_doc)
            logger.info("Đã lưu thống kê load")
        except Exception as e:
            logger.warning(f"Không thể lưu thống kê: {e}")

    # Hàm tiện ích để lấy connection string từ environment variables
    def get_mongodb_connection_string() -> str:
        """
        Lấy MongoDB connection string từ environment variables
        
        Returns:
            str: Connection string
        """
        username = os.getenv('MONGODB_USERNAME')
        password = os.getenv('MONGODB_PASSWORD')
        cluster = os.getenv('MONGODB_CLUSTER')
        
        if not all([username, password, cluster]):
            raise ValueError("Thiếu thông tin MongoDB connection. Vui lòng set environment variables: MONGODB_USERNAME, MONGODB_PASSWORD, MONGODB_CLUSTER")
        
        return f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority"

    # Ví dụ sử dụng:
    """
    # Set environment variables trước khi chạy:
    # export MONGODB_USERNAME="your_username"
    # export MONGODB_PASSWORD="your_password"  
    # export MONGODB_CLUSTER="your_cluster.mongodb.net"

    # Sử dụng hàm:
    connection_string = get_mongodb_connection_string()
    success = load(self, "path/to/staging_data.csv", connection_string)
    """