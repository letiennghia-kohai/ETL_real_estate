# import os
# import json
# import pandas as pd
# import logging
# from datetime import datetime
# import pandas as pd
# import pymongo
# from pymongo import MongoClient
# from datetime import datetime
# import logging
# import os
# from typing import Dict, Any, List

# logger = logging.getLogger(__name__)

# class JsonLoader:
#     """
#     Lớp tải dữ liệu vào kho dữ liệu JSON cho mô hình dự đoán
#     """
    
#     def __init__(self):
#         pass
    
#     def load(self, staging_data_path: str, mongodb_connection_string: str, 
#             database_name: str = "real_estate_warehouse", 
#             collection_name: str = "properties") -> bool:
#         """
#         Tải dữ liệu từ vùng staging vào MongoDB Atlas
        
#         Args:
#             staging_data_path: Đường dẫn đến file CSV chứa dữ liệu đã biến đổi
#             mongodb_connection_string: Connection string của MongoDB Atlas
#             database_name: Tên database (mặc định: real_estate_warehouse)
#             collection_name: Tên collection (mặc định: properties)
            
#         Returns:
#             bool: True nếu tải thành công, False nếu có lỗi
#         """
#         client = None
#         try:
#             # Kết nối đến MongoDB Atlas
#             client = MongoClient(mongodb_connection_string)
            
#             # Kiểm tra kết nối
#             client.admin.command('ping')
#             logger.info("Kết nối MongoDB Atlas thành công")
            
#             # Chọn database và collection
#             db = client[database_name]
#             collection = db[collection_name]
            
#             # Đọc dữ liệu từ file CSV
#             df = pd.read_csv(staging_data_path)
#             logger.info(f"Đã đọc {len(df)} bản ghi từ file CSV")
            
#             # Chuẩn bị dữ liệu cho MongoDB
#             documents = self._prepare_mongodb_documents(df)
            
#             # Thêm metadata cho mỗi document
#             current_time = datetime.now()
#             for doc in documents:
#                 doc['_metadata'] = {
#                     'loaded_at': current_time,
#                     'source_file': os.path.basename(staging_data_path),
#                     'batch_id': f"batch_{current_time.strftime('%Y%m%d_%H%M%S')}"
#                 }
            
#             # Insert dữ liệu vào MongoDB
#             if documents:
#                 # Sử dụng insert_many cho hiệu suất tốt hơn
#                 result = collection.insert_many(documents, ordered=False)
#                 logger.info(f"Đã chèn {len(result.inserted_ids)} documents vào MongoDB")
                
#                 # Tạo index để tối ưu truy vấn
#                 self._create_indexes(collection)
                
#                 # Lưu thống kê vào collection metadata
#                 self._save_load_statistics(db, len(documents), current_time)
                
#                 logger.info("Tải dữ liệu vào MongoDB Atlas thành công")
#                 return True
#             else:
#                 logger.warning("Không có dữ liệu để tải")
#                 return False
                
#         except pymongo.errors.ConnectionFailure as e:
#             logger.error(f"Lỗi kết nối MongoDB: {e}")
#             return False
#         except pymongo.errors.BulkWriteError as e:
#             logger.error(f"Lỗi bulk write: {e}")
#             # Có thể một số documents đã được chèn thành công
#             return False
#         except Exception as e:
#             logger.error(f"Lỗi khi tải dữ liệu vào MongoDB: {e}")
#             return False
#         finally:
#             # Đóng kết nối
#             if client:
#                 client.close()

#     def _prepare_mongodb_documents(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
#         """
#         Chuẩn bị dữ liệu DataFrame thành list documents cho MongoDB
        
#         Args:
#             df: DataFrame chứa dữ liệu
            
#         Returns:
#             List[Dict]: Danh sách documents
#         """
#         documents = []
        
#         for _, row in df.iterrows():
#             # Chuyển đổi Series thành dict và xử lý NaN values
#             doc = row.to_dict()
            
#             # Xử lý NaN values (MongoDB không hỗ trợ NaN)
#             for key, value in doc.items():
#                 if pd.isna(value):
#                     doc[key] = None
#                 elif isinstance(value, (int, float)) and pd.isna(value):
#                     doc[key] = None
            
#             # Thêm các trường bổ sung nếu cần
#             doc = self._enrich_document(doc)
            
#             documents.append(doc)
        
#         return documents

#     def _enrich_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
#         """
#         Làm giàu document với các trường bổ sung
        
#         Args:
#             doc: Document gốc
            
#         Returns:
#             Dict: Document đã được làm giàu
#         """
#         # Thêm các trường tính toán hoặc phân loại
#         if 'price' in doc and doc['price']:
#             doc['price_category'] = self._categorize_price(doc['price'])
        
#         if 'area' in doc and doc['area']:
#             doc['area_category'] = self._categorize_area(doc['area'])
        
#         # Thêm geolocation nếu có thông tin địa chỉ
#         if 'address' in doc:
#             doc['location'] = {
#                 'address': doc['address'],
#                 'coordinates': None  # Có thể thêm geocoding sau
#             }
        
#         return doc

#     def _categorize_price(self, price: float) -> str:
#         """Phân loại giá theo khoảng"""
#         if price < 1000000000:  # < 1 tỷ
#             return "budget"
#         elif price < 3000000000:  # < 3 tỷ
#             return "mid_range"
#         elif price < 10000000000:  # < 10 tỷ
#             return "premium"
#         else:
#             return "luxury"

#     def _categorize_area(self, area: float) -> str:
#         """Phân loại diện tích"""
#         if area < 50:
#             return "small"
#         elif area < 100:
#             return "medium"
#         elif area < 200:
#             return "large"
#         else:
#             return "extra_large"

#     def _create_indexes(self, collection) -> None:
#         """
#         Tạo các index để tối ưu truy vấn
        
#         Args:
#             collection: MongoDB collection
#         """
#         try:
#             # Index cho các trường thường được query
#             indexes = [
#                 [("price", 1)],
#                 [("area", 1)],
#                 [("location.address", "text")],
#                 [("_metadata.loaded_at", -1)],
#                 [("_metadata.batch_id", 1)],
#                 [("price_category", 1), ("area_category", 1)]  # Compound index
#             ]
            
#             for index_keys in indexes:
#                 try:
#                     collection.create_index(index_keys)
#                     logger.info(f"Đã tạo index: {index_keys}")
#                 except pymongo.errors.OperationFailure as e:
#                     logger.warning(f"Không thể tạo index {index_keys}: {e}")
                    
#         except Exception as e:
#             logger.error(f"Lỗi khi tạo indexes: {e}")

#     def _save_load_statistics(self, db, record_count: int, load_time: datetime) -> None:
#         """
#         Lưu thông tin thống kê về quá trình load
        
#         Args:
#             db: MongoDB database
#             record_count: Số lượng bản ghi đã load
#             load_time: Thời gian load
#         """
#         try:
#             stats_collection = db['load_statistics']
#             stats_doc = {
#                 'load_time': load_time,
#                 'record_count': record_count,
#                 'status': 'success',
#                 'collection': 'properties'
#             }
#             stats_collection.insert_one(stats_doc)
#             logger.info("Đã lưu thống kê load")
#         except Exception as e:
#             logger.warning(f"Không thể lưu thống kê: {e}")

#     # Hàm tiện ích để lấy connection string từ environment variables
#     def get_mongodb_connection_string() -> str:
#         """
#         Lấy MongoDB connection string từ environment variables
        
#         Returns:
#             str: Connection string
#         """
#         username = os.getenv('MONGODB_USERNAME')
#         password = os.getenv('MONGODB_PASSWORD')
#         cluster = os.getenv('MONGODB_CLUSTER')
        
#         if not all([username, password, cluster]):
#             raise ValueError("Thiếu thông tin MongoDB connection. Vui lòng set environment variables: MONGODB_USERNAME, MONGODB_PASSWORD, MONGODB_CLUSTER")
        
#         return f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority"

#     # Ví dụ sử dụng:
#     """
#     # Set environment variables trước khi chạy:
#     # export MONGODB_USERNAME="your_username"
#     # export MONGODB_PASSWORD="your_password"  
#     # export MONGODB_CLUSTER="your_cluster.mongodb.net"

#     # Sử dụng hàm:
#     connection_string = get_mongodb_connection_string()
#     success = load(self, "path/to/staging_data.csv", connection_string)
#     """



import pandas as pd
import pymongo
from pymongo import MongoClient
import json
import logging
from datetime import datetime
import os
from typing import Dict, List, Any, Optional

class JsonLoader:
    """
    JsonLoader để tải dữ liệu vào MongoDB Atlas
    """
    
    def __init__(self, 
                 connection_string: Optional[str] = None,
                 database_name: str = "real_estate_db",
                 collection_name: str = "properties"):
        """
        Khởi tạo JsonLoader
        
        Args:
            connection_string: MongoDB Atlas connection string
            database_name: Tên database
            collection_name: Tên collection
        """
        self.connection_string = connection_string or os.getenv('MONGODB_CONNECTION_STRING')
        self.database_name = database_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        if not self.connection_string:
            raise ValueError("MongoDB connection string is required")
    
    def connect(self):
        """Kết nối đến MongoDB Atlas"""
        try:
            self.client = MongoClient(self.connection_string)
            # Test connection
            self.client.admin.command('ping')
            
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            self.logger.info(f"Connected to MongoDB Atlas - Database: {self.database_name}, Collection: {self.collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to MongoDB Atlas: {str(e)}")
            raise
    
    def disconnect(self):
        """Đóng kết nối MongoDB"""
        if self.client:
            self.client.close()
            self.logger.info("Disconnected from MongoDB Atlas")
    
    def load(self, staging_data_path: str, batch_size: int = 1000) -> str:
        """
        Tải dữ liệu từ CSV vào MongoDB Atlas
        
        Args:
            staging_data_path: Đường dẫn file CSV
            batch_size: Số record insert mỗi batch
            
        Returns:
            str: Thông báo kết quả
        """
        try:
            # Kết nối database
            self.connect()
            
            # Đọc dữ liệu từ CSV
            df = pd.read_csv(staging_data_path)
            self.logger.info(f"Loaded {len(df)} records from {staging_data_path}")
            
            # Chuyển đổi dữ liệu
            documents = self._prepare_documents(df)
            
            # Insert dữ liệu theo batch
            inserted_count = self._insert_batch(documents, batch_size)
            
            result_message = f"Successfully loaded {inserted_count} documents to MongoDB Atlas"
            self.logger.info(result_message)
            
            return result_message
            
        except Exception as e:
            error_message = f"Error loading data to MongoDB: {str(e)}"
            self.logger.error(error_message)
            raise
            
        finally:
            self.disconnect()
    
    def _prepare_documents(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Chuẩn bị documents để insert vào MongoDB
        
        Args:
            df: DataFrame chứa dữ liệu
            
        Returns:
            List[Dict]: Danh sách documents
        """
        documents = []
        
        for _, row in df.iterrows():
            # Chuyển đổi row thành dictionary
            doc = row.to_dict()
            
            # Xử lý các giá trị NaN
            doc = self._handle_nan_values(doc)
            
            # Thêm metadata
            doc['_loaded_at'] = datetime.utcnow()
            doc['_source'] = 'real_estate_etl_pipeline'
            
            # Xử lý các trường đặc biệt
            doc = self._process_special_fields(doc)
            
            documents.append(doc)
        
        return documents
    
    def _handle_nan_values(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Xử lý các giá trị NaN trong document"""
        cleaned_doc = {}
        
        for key, value in doc.items():
            if pd.isna(value):
                cleaned_doc[key] = None
            elif isinstance(value, (int, float)) and pd.isna(value):
                cleaned_doc[key] = None
            else:
                cleaned_doc[key] = value
        
        return cleaned_doc
    
    def _process_special_fields(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Xử lý các trường đặc biệt như coordinates, price, etc.
        """
        # Xử lý tọa độ
        if 'latitude' in doc and 'longitude' in doc:
            if doc['latitude'] is not None and doc['longitude'] is not None:
                doc['location'] = {
                    'type': 'Point',
                    'coordinates': [float(doc['longitude']), float(doc['latitude'])]
                }
        
        # Xử lý giá
        price_fields = ['price', 'price_per_sqm', 'rent_price']
        for field in price_fields:
            if field in doc and doc[field] is not None:
                try:
                    doc[field] = float(doc[field])
                except (ValueError, TypeError):
                    doc[field] = None
        
        # Xử lý diện tích
        area_fields = ['area', 'land_area', 'floor_area']
        for field in area_fields:
            if field in doc and doc[field] is not None:
                try:
                    doc[field] = float(doc[field])
                except (ValueError, TypeError):
                    doc[field] = None
        
        # Xử lý ngày tháng
        date_fields = ['posted_date', 'updated_date']
        for field in date_fields:
            if field in doc and doc[field] is not None:
                try:
                    if isinstance(doc[field], str):
                        doc[field] = pd.to_datetime(doc[field]).to_pydatetime()
                except:
                    doc[field] = None
        
        return doc
    
    def _insert_batch(self, documents: List[Dict[str, Any]], batch_size: int) -> int:
        """
        Insert documents theo batch
        
        Args:
            documents: Danh sách documents
            batch_size: Kích thước mỗi batch
            
        Returns:
            int: Số document đã insert
        """
        total_inserted = 0
        
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            
            try:
                # Insert batch với unordered để continue khi có lỗi
                result = self.collection.insert_many(batch, ordered=False)
                inserted_count = len(result.inserted_ids)
                total_inserted += inserted_count
                
                self.logger.info(f"Inserted batch {i//batch_size + 1}: {inserted_count} documents")
                
            except pymongo.errors.BulkWriteError as e:
                # Xử lý lỗi duplicate key hoặc validation errors
                inserted_count = e.details.get('nInserted', 0)
                total_inserted += inserted_count
                
                self.logger.warning(f"Batch {i//batch_size + 1} had {len(e.details.get('writeErrors', []))} errors, inserted {inserted_count} documents")
        
        return total_inserted
    
    def create_indexes(self):
        """Tạo indexes cho collection"""
        try:
            self.connect()
            
            # Index cho location (geospatial)
            self.collection.create_index([("location", "2dsphere")])
            
            # Index cho các trường thường query
            self.collection.create_index("website_source")
            self.collection.create_index("property_type")
            self.collection.create_index("district")
            self.collection.create_index("city")
            self.collection.create_index("price")
            self.collection.create_index("_loaded_at")
            
            # Compound index
            self.collection.create_index([
                ("city", 1),
                ("district", 1),
                ("property_type", 1)
            ])
            
            self.logger.info("Created indexes successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating indexes: {str(e)}")
            raise
        finally:
            self.disconnect()
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Lấy thống kê collection"""
        try:
            self.connect()
            
            stats = self.db.command("collStats", self.collection_name)
            count = self.collection.count_documents({})
            
            return {
                'document_count': count,
                'storage_size': stats.get('storageSize', 0),
                'index_count': stats.get('nindexes', 0),
                'avg_document_size': stats.get('avgObjSize', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting collection stats: {str(e)}")
            return {}
        finally:
            self.disconnect()


    def load_data_to_json(self, staging_data_path):
        """
        Hàm tải dữ liệu vào MongoDB Atlas cho mô hình dự đoán
        """
        try:
            # Khởi tạo JsonLoader
            json_loader = JsonLoader(
                database_name="real_estate_db",
                collection_name="properties"
            )
            
            # Tạo indexes nếu chưa có
            json_loader.create_indexes()
            
            # Load dữ liệu
            result = json_loader.load(staging_data_path, batch_size=500)
            
            # Lấy thống kê
            stats = json_loader.get_collection_stats()
            
            logging.info(f"Load completed: {result}")
            logging.info(f"Collection stats: {stats}")
            
            return {
                'status': 'success',
                'message': result,
                'stats': stats
            }
            
        except Exception as e:
            error_message = f"Failed to load data to MongoDB: {str(e)}"
            logging.error(error_message)
            return {
                'status': 'error',
                'message': error_message
            }


# Example usage và configuration
if __name__ == "__main__":
    # Test the JsonLoader
    import os
    
    # Set MongoDB connection string
    os.environ['MONGODB_CONNECTION_STRING'] = 'mongodb+srv://mongodb:mongodb@cluster0.eiuib.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'
    
    # Test load
    result = load_data_to_json("data/staging/unified_data_20250621.csv")
    print(result)