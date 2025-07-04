import os
from random import random
import pandas as pd
import json
import re
from datetime import datetime
from typing import Dict, List, Any
import hashlib
import logging
import sqlalchemy as sa
from sqlalchemy import text, MetaData, Table, Column, Integer, String, Float, DateTime, Boolean, Date, Text
from sqlalchemy.dialects.mysql import BIGINT, DECIMAL, LONGTEXT
from sqlalchemy.engine import Engine
from typing import Dict, List, Optional
from datetime import datetime
import pymysql

logger = logging.getLogger(__name__)

class WarehouseLoader:
    """
    Transform OLTP data into OLAP star schema structure
    """
    
    def __init__(self):
        self.location_cache = {}
        self.property_cache = {}
        self.legal_cache = {}
        self.structure_cache = {}
        self.amenity_cache = {}
        self.contact_cache = {}
        self.source_cache = {}
        self.legal_key_cache = {}
        self.structure_key_cache = {}
        self.amenity_key_cache = {}
        self.contact_key_cache = {}
        
        # Auto-increment keys
        self.next_location_key = 1
        self.next_property_key = 1
        self.next_legal_key = 1
        self.next_structure_key = 1
        self.next_amenity_key = 1
        self.next_contact_key = 1
        self.next_source_key = 1
        self.connection_string = "mysql+pymysql://airflow_user:airflow_password@mysql_container:3306/airflow_db"
        self.schema_name = 'airflow_db'
        self.engine = None
        self.metadata = MetaData()
        self.amenity_keywords = {
            'parking': ['bãi đỗ xe', 'chỗ đỗ xe', 'garage', 'parking', 'để xe'],
            'air_conditioning': ['điều hòa', 'máy lạnh', 'air conditioning', 'ac', 'klimat'],
            'balcony': ['ban công', 'balcony', 'loggia', 'sân thượng nhỏ'],
            'elevator': ['thang máy', 'elevator', 'lift', 'thang bộ tự động'],
            'security': ['bảo vệ', 'an ninh', 'security', 'camera', 'thẻ từ', 'cổng tự động'],
            'gym': ['phòng gym', 'gym', 'fitness', 'thể dục', 'tập thể hình'],
            'pool': ['hồ bơi', 'swimming pool', 'pool', 'bể bơi'],
            'garden': ['sân vườn', 'garden', 'công viên', 'cây xanh', 'không gian xanh']
        }
        
        # Phân loại tiện ích luxury vs basic
        self.luxury_amenities = [
            'hồ bơi', 'gym', 'spa', 'sauna', 'sân tennis', 'golf', 'cinema', 
            'rooftop', 'sky bar', 'concierge', 'valet parking', 'wine cellar',
            'private elevator', 'maid service', 'housekeeping'
        ]
        
        self.basic_amenities = [
            'thang máy', 'bảo vệ', 'điều hòa', 'ban công', 'bãi đỗ xe',
            'internet', 'cable tv', 'water heater', 'kitchen', 'wardrobe'
        ]
        
    def load_oltp_data_from_staging(self, staging_data_path):
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

    def save_olap_data_to_files(self, olap_data, output_dir):
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

    def transform_to_olap(self, oltp_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transform OLTP data to OLAP star schema
        
        Args:
            oltp_data: Dictionary of OLTP DataFrames from original transformer
            
        Returns:
            Dictionary of OLAP DataFrames (dimensions + facts)
        """
        # Initialize dimension tables
        dim_time = self._build_dim_time(oltp_data)
        dim_location = self._build_dim_location(oltp_data)
        dim_property = self._build_dim_property(oltp_data)
        dim_legal = self._build_dim_legal(oltp_data)
        dim_structure = self._build_dim_structure(oltp_data)
        dim_amenity = self._build_dim_amenity(oltp_data)
        dim_contact = self._build_dim_contact(oltp_data)
        dim_source = self._build_dim_source(oltp_data)
        
        # Build fact tables
        fact_price_analysis = self._build_fact_price_analysis(oltp_data)
        fact_area_analysis = self._build_fact_area_analysis(oltp_data)
        fact_amenities_analysis = self._build_fact_amenities_analysis(oltp_data)
        fact_listing_analysis = self._build_fact_listing_analysis(oltp_data)
        
        return {
            # Dimension tables
            'dim_time': dim_time,
            'dim_location': dim_location,
            'dim_property': dim_property,
            'dim_legal': dim_legal,
            'dim_structure': dim_structure,
            'dim_amenity': dim_amenity,
            'dim_contact': dim_contact,
            'dim_source': dim_source,
            
            # Fact tables
            'fact_price_analysis': fact_price_analysis,
            'fact_area_analysis': fact_area_analysis,
            'fact_amenities_analysis': fact_amenities_analysis,
            'fact_listing_analysis': fact_listing_analysis
        }
    
    def _build_dim_time(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build time dimension from listing dates"""
        if 'raw_property_listings' not in oltp_data or oltp_data['raw_property_listings'].empty:
            return pd.DataFrame()
        
        # Extract unique dates from listings
        listings_df = oltp_data['raw_property_listings']
        dates = []
        
        for col in ['created_at', 'updated_at', 'scraped_at']:
            if col in listings_df.columns:
                dates.extend(pd.to_datetime(listings_df[col], errors='coerce').dropna().dt.date.unique())
        
        if not dates:
            # Create at least current date
            dates = [datetime.now().date()]
        
        dates = list(set(dates))  # Remove duplicates
        
        time_data = []
        for i, date in enumerate(sorted(dates), 1):
            dt = pd.to_datetime(date)
            time_data.append({
                'time_key': i,
                'full_date': date,
                'year': dt.year,
                'quarter': dt.quarter,
                'month': dt.month,
                'month_name': dt.strftime('%B'),
                'week': dt.isocalendar()[1],
                'day_of_month': dt.day,
                'day_of_week': dt.dayofweek + 1,
                'day_name': dt.strftime('%A'),
                'is_weekend': dt.dayofweek >= 5,
                'is_holiday': False,  # Would need holiday calendar
                'season': self._get_season(dt.month),
                'fiscal_year': dt.year if dt.month >= 4 else dt.year - 1,
                'fiscal_quarter': ((dt.month - 4) // 3 + 1) if dt.month >= 4 else ((dt.month + 8) // 3 + 1)
            })
        
        return pd.DataFrame(time_data)
    
    def _build_dim_location(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build location dimension"""
        if 'property_address' not in oltp_data or oltp_data['property_address'].empty:
            return pd.DataFrame()
        
        address_df = oltp_data['property_address']
        location_data = []
        
        for _, row in address_df.iterrows():
            location_key = self._get_or_create_location_key(row)
            
            location_data.append({
                'location_key': location_key,
                'city': row.get('city', ''),
                'district': row.get('district', ''),
                'ward': row.get('ward', ''),
                'street': row.get('street', ''),
                'project_name': row.get('project_name', ''),
                'region': self._get_region(row.get('city', '')),
                'city_tier': self._get_city_tier(row.get('city', '')),
                'location_score': self._calculate_location_score(row),
                'infrastructure_score': self._calculate_infrastructure_score(row),
                'full_address': row.get('full_address', ''),
                'latitude': row.get('latitude', None),
                'longitude': row.get('longitude', None)
            })
        
        return pd.DataFrame(location_data).drop_duplicates(subset=['location_key'])
    
    def _build_dim_property(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build property dimension"""
        if 'raw_property_listings' not in oltp_data or oltp_data['raw_property_listings'].empty:
            return pd.DataFrame()
        
        listings_df = oltp_data['raw_property_listings']
        property_data = []
        
        for _, row in listings_df.iterrows():
            property_key = self._get_or_create_property_key(row)
            
            property_data.append({
                'property_key': property_key,
                'property_type': row.get('property_type', ''),
                'property_subtype': row.get('property_subtype', ''),
                'property_category': self._get_property_category(row.get('property_type', '')),
                'property_segment': self._get_property_segment(row),
                'age_group': self._get_age_group(row.get('year_built', None)),
                'building_type': self._get_building_type(row),
                'ownership_type': row.get('ownership_type', '')
            })
        
        return pd.DataFrame(property_data).drop_duplicates(subset=['property_key'])
    
    def _build_dim_legal(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build legal dimension"""
        if 'raw_property_details' not in oltp_data or oltp_data['raw_property_details'].empty:
            return pd.DataFrame()
        
        details_df = oltp_data['raw_property_details']
        legal_data = []
        
        for _, row in details_df.iterrows():
            legal_key = self._get_or_create_legal_key(row)
            
            legal_data.append({
                'legal_key': legal_key,
                'legal_status': row.get('legal_documents', ''),
                'legal_category': self._categorize_legal_status(row.get('legal_documents', '')),
                'can_get_loan': self._can_get_bank_loan(row.get('legal_documents', '')),
                'transferable': self._is_transferable(row.get('legal_documents', '')),
                'legal_risk_level': self._assess_legal_risk(row.get('legal_documents', '')),
                'legal_score': self._calculate_legal_score(row.get('legal_documents', ''))
            })
        
        return pd.DataFrame(legal_data).drop_duplicates(subset=['legal_key'])
    
    def _build_dim_structure(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build structure dimension"""
        if 'raw_property_details' not in oltp_data or oltp_data['raw_property_details'].empty:
            return pd.DataFrame()
        
        details_df = oltp_data['raw_property_details']
        structure_data = []
        
        for _, row in details_df.iterrows():
            structure_key = self._get_or_create_structure_key(row)
            
            structure_data.append({
                'structure_key': structure_key,
                'direction': row.get('direction', ''),
                'balcony_direction': row.get('balcony_direction', ''),
                'structure_type': self._get_structure_type(row),
                'layout_type': self._get_layout_type(row),
                'feng_shui_score': self._calculate_feng_shui_score(row.get('direction', '')),
                'ventilation_score': self._calculate_ventilation_score(row),
                'lighting_score': self._calculate_lighting_score(row)
            })
        
        return pd.DataFrame(structure_data).drop_duplicates(subset=['structure_key'])
    
    def _build_dim_amenity(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build amenity dimension"""
        if 'raw_property_amenities' not in oltp_data or oltp_data['raw_property_amenities'].empty:
            return pd.DataFrame()
        
        amenities_df = oltp_data['raw_property_amenities']
        amenity_data = []
        
        # Get unique amenities
        unique_amenities = amenities_df['amenity_name'].unique() if 'amenity_name' in amenities_df.columns else []
        
        for amenity_name in unique_amenities:
            amenity_key = self._get_or_create_amenity_key(amenity_name)
            
            amenity_data.append({
                'amenity_key': amenity_key,
                'amenity_name': amenity_name,
                'amenity_category': self._categorize_amenity(amenity_name),
                'amenity_type': self._get_amenity_type(amenity_name),
                'value_impact': self._get_amenity_value_impact(amenity_name),
                'popularity_score': self._calculate_amenity_popularity(amenity_name, amenities_df),
                'maintenance_cost': self._estimate_maintenance_cost(amenity_name)
            })
        
        return pd.DataFrame(amenity_data).drop_duplicates(subset=['amenity_key'])
    
    def _build_dim_contact(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build contact dimension"""
        if 'raw_contact_info' not in oltp_data or oltp_data['raw_contact_info'].empty:
            return pd.DataFrame()
        
        contact_df = oltp_data['raw_contact_info']
        contact_data = []
        
        for _, row in contact_df.iterrows():
            contact_key = self._get_or_create_contact_key(row)
            
            contact_data.append({
                'contact_key': contact_key,
                'contact_type': row.get('contact_type', ''),
                'is_agent': self._is_agent(row),
                'is_company': self._is_company(row),
                'agent_level': self._get_agent_level(row),
                'company_size': self._get_company_size(row),
                'experience_years': self._estimate_experience_years(row),
                'listing_count': self._estimate_listing_count(row),
                'success_rate': self._estimate_success_rate(row),
                'rating': self._get_rating(row)
            })
        
        return pd.DataFrame(contact_data).drop_duplicates(subset=['contact_key'])
    
    def _build_dim_source(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build source dimension"""
        source_data = [{
            'source_key': 1,
            'source_name': 'batdongsan.com.vn',
            'source_type': 'Website',
            'reliability_score': 85.0,
            'data_quality_score': 80.0,
            'update_frequency': 'Realtime',
            'premium_source': True
        }]
        
        return pd.DataFrame(source_data)
    
    def _build_fact_price_analysis(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build price analysis fact table"""
        if 'raw_property_listings' not in oltp_data or oltp_data['raw_property_listings'].empty:
            return pd.DataFrame()
        
        listings_df = oltp_data['raw_property_listings']
        fact_data = []
        
        for _, row in listings_df.iterrows():
            # Get dimension keys
            time_key = self._get_time_key(row.get('created_at'))
            location_key = self._get_location_key_for_listing(row, oltp_data)
            property_key = self._get_property_key_for_listing(row)
            legal_key = self._get_legal_key_for_listing(row, oltp_data)
            
            price_value = self._convert_to_numeric(row.get('price_value', 0))
            area_value = self._convert_to_numeric(row.get('area_value', 0))
            
            fact_data.append({
                'listing_id': row.get('listing_id', ''),
                'time_key': time_key,
                'location_key': location_key,
                'property_key': property_key,
                'legal_key': legal_key,
                'price_value': price_value,
                'price_per_m2': price_value / area_value if area_value > 0 else None,
                'price_usd': price_value / 24000 if price_value > 0 else None,  # Rough conversion
                'price_category': 'HIGH',
                'price_change_pct': None,  # Would need historical data
                'market_price_ratio': None,  # Would need market averages
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
        
        return pd.DataFrame(fact_data)
    
    def _build_fact_area_analysis(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build area analysis fact table"""
        if 'raw_property_listings' not in oltp_data or oltp_data['raw_property_listings'].empty:
            return pd.DataFrame()
        
        listings_df = oltp_data['raw_property_listings']
        details_df = oltp_data.get('raw_property_details', pd.DataFrame())
        fact_data = []
        
        for _, row in listings_df.iterrows():
            listing_id = row.get('listing_id', '')
            
            # Get matching details
            matched_rows = details_df[details_df['listing_id'] == listing_id]
            if not matched_rows.empty:
                detail_row = matched_rows.iloc[0]
            else:
                detail_row = {}  # fallback to empty dict
            area_value = self._convert_to_numeric(row.get('area_value', 0))
            bedrooms = self._convert_to_numeric(detail_row.get('bedrooms', 0) if isinstance(detail_row, (dict, pd.Series)) else 0)
            bathrooms = self._convert_to_numeric(detail_row.get('bathrooms', 0) if isinstance(detail_row, (dict, pd.Series)) else 0)
            
            fact_data.append({
                'listing_id': listing_id,
                'time_key': self._get_time_key(row.get('created_at')),
                'location_key': self._get_location_key_for_listing(row, oltp_data),
                'property_key': self._get_property_key_for_listing(row),
                'structure_key': self._get_structure_key_for_listing(detail_row),
                'area_value': area_value,
                'bedrooms_count': bedrooms,
                'bathrooms_count': bathrooms,
                'floors_count': self._convert_to_numeric(detail_row.get('floors', 0) if isinstance(detail_row, (dict, pd.Series)) else 0),
                'width_value': self._convert_to_numeric(detail_row.get('width', 0) if isinstance(detail_row, (dict, pd.Series)) else 0),
                'length_value': self._convert_to_numeric(detail_row.get('length', 0) if isinstance(detail_row, (dict, pd.Series)) else 0),
                'road_width_value': self._convert_to_numeric(detail_row.get('road_width', 0) if isinstance(detail_row, (dict, pd.Series)) else 0),
                'house_front_value': self._convert_to_numeric(detail_row.get('front_width', 0) if isinstance(detail_row, (dict, pd.Series)) else 0),
                'area_efficiency_ratio': None,  # Would need calculation
                'area_category': 'HIGH',
                'room_density': (bedrooms + bathrooms) / area_value if area_value > 0 else None,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
        
        return pd.DataFrame(fact_data)
    
    def _build_fact_amenities_analysis(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build amenities analysis fact table"""
        if 'raw_property_listings' not in oltp_data or oltp_data['raw_property_listings'].empty:
            return pd.DataFrame()
        
        listings_df = oltp_data['raw_property_listings']
        amenities_df = oltp_data.get('raw_property_amenities', pd.DataFrame())
        fact_data = []
        
        for _, row in listings_df.iterrows():
            listing_id = row.get('listing_id', '')
            
            # Get amenities for this listing
            listing_amenities = amenities_df[amenities_df['listing_id'] == listing_id] if not amenities_df.empty else pd.DataFrame()
            
            amenities_count = len(listing_amenities)
            
            fact_data.append({
                'amenity_fact_id': f"{listing_id}_amenities",
                'listing_id': listing_id,
                'time_key': self._get_time_key(row.get('created_at')),
                'location_key': self._get_location_key_for_listing(row, oltp_data),
                'property_key': self._get_property_key_for_listing(row),
                'amenity_key': None,  # Would be populated for each amenity separately
                'amenities_count': amenities_count,
                'amenity_score': self._calculate_amenity_score(listing_amenities),
                'has_parking': self._has_amenity(listing_amenities, 'parking'),
                'has_air_conditioning': self._has_amenity(listing_amenities, 'air_conditioning'),
                'has_balcony': self._has_amenity(listing_amenities, 'balcony'),
                'has_elevator': self._has_amenity(listing_amenities, 'elevator'),
                'has_security': self._has_amenity(listing_amenities, 'security'),
                'has_gym': self._has_amenity(listing_amenities, 'gym'),
                'has_pool': self._has_amenity(listing_amenities, 'pool'),
                'has_garden': self._has_amenity(listing_amenities, 'garden'),
                'luxury_amenities_count': self._count_luxury_amenities(listing_amenities),
                'basic_amenities_count': self._count_basic_amenities(listing_amenities),
                'amenity_price_premium': self._calculate_amenity_premium(listing_amenities),
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
        
        return pd.DataFrame(fact_data)
    
    def _build_fact_listing_analysis(self, oltp_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Build listing analysis fact table"""
        if 'raw_property_listings' not in oltp_data or oltp_data['raw_property_listings'].empty:
            return pd.DataFrame()
        
        listings_df = oltp_data['raw_property_listings']
        # media_df = oltp_data.get('property_media', pd.DataFrame())
        fact_data = []
        
        for _, row in listings_df.iterrows():
            listing_id = row.get('listing_id', '')
            
            # Get media for this listing
            # listing_media = media_df[media_df['listing_id'] == listing_id] if not media_df.empty else pd.DataFrame()
            
            description = str(row.get('description', ''))
            title = str(row.get('title', ''))
            
            fact_data.append({
                'listing_id': listing_id,
                'time_key': self._get_time_key(row.get('created_at')),
                'location_key': self._get_location_key_for_listing(row, oltp_data),
                'property_key': self._get_property_key_for_listing(row),
                'contact_key': self._get_contact_key_for_listing(row, oltp_data),
                'source_key': 1,  # batdongsan.com.vn
                'description_length': len(description),
                'description_word_count': len(description.split()) if description else 0,
                'title_length': len(title),
                'days_on_market': None,
                'view_count': None,  # Not available in current data
                'contact_count': None,  # Not available in current data
                # 'listing_quality_score': self._calculate_listing_quality_score(row, listing_media),
                # 'has_photos': len(listing_media) > 0,
                # 'photos_count': len(listing_media),
                # 'has_video': self._has_video(listing_media),
                # 'has_360_view': self._has_360_view(listing_media),
                'phone_reveal_count': None,  # Not available
                'save_count': None,  # Not available
                'share_count': None,  # Not available
                'is_available': True,  # Assume available
                'is_featured': True,
                'is_urgent': True,
                'has_promotion': None,
                'discount_percentage': None,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
        
        return pd.DataFrame(fact_data)
    
   # Helper methods for key generation and lookups
    def _get_or_create_location_key(self, row) -> int:
        """Get or create location key"""
        location_hash = self._hash_location(row)
        if location_hash not in self.location_cache:
            self.location_cache[location_hash] = self.next_location_key
            self.next_location_key += 1
        return self.location_cache[location_hash]

    def _hash_location(self, row) -> str:
        """Create hash for location"""
        # Safe string conversion with null handling
        city = str(row.get('city', '')).strip() if pd.notna(row.get('city')) else ''
        district = str(row.get('district', '')).strip() if pd.notna(row.get('district')) else ''
        ward = str(row.get('ward', '')).strip() if pd.notna(row.get('ward')) else ''
        street = str(row.get('street', '')).strip() if pd.notna(row.get('street')) else ''
        
        location_str = f"{city}_{district}_{ward}_{street}"
        return hashlib.md5(location_str.encode()).hexdigest()[:16]

    def _get_or_create_property_key(self, row) -> int:
        """Get or create property key"""
        property_hash = self._hash_property(row)
        if property_hash not in self.property_cache:
            self.property_cache[property_hash] = self.next_property_key
            self.next_property_key += 1
        return self.property_cache[property_hash]

    def _hash_property(self, row) -> str:
        """Create hash for property"""
        # Safe string conversion with null handling
        property_type = str(row.get('property_type', '')).strip() if pd.notna(row.get('property_type')) else ''
        property_subtype = str(row.get('property_subtype', '')).strip() if pd.notna(row.get('property_subtype')) else ''
        
        property_str = f"{property_type}_{property_subtype}"
        return hashlib.md5(property_str.encode()).hexdigest()[:16]
    def _get_or_create_legal_key(self, row) -> int:
        """Get or create legal key"""
        legal_hash = self._hash_legal(row)
        if legal_hash not in self.legal_cache:
            self.legal_cache[legal_hash] = self.next_legal_key
            self.next_legal_key += 1
        return self.legal_cache[legal_hash]

    def _hash_legal(self, row) -> str:
        """Create hash for legal dimension"""
        legal_docs = str(row.get('legal_documents', '')).strip() if pd.notna(row.get('legal_documents')) else ''
        category = str(self._categorize_legal_status(legal_docs)).strip()
        loan_eligible = str(self._can_get_bank_loan(legal_docs)).strip()
        transferable = str(self._is_transferable(legal_docs)).strip()
        
        legal_str = f"{legal_docs}_{category}_{loan_eligible}_{transferable}"
        return hashlib.md5(legal_str.encode()).hexdigest()[:16]

    def _get_or_create_structure_key(self, row) -> int:
        """Get or create structure key"""
        structure_hash = self._hash_structure(row)
        if structure_hash not in self.structure_cache:
            self.structure_cache[structure_hash] = self.next_structure_key
            self.next_structure_key += 1
        return self.structure_cache[structure_hash]

    def _hash_structure(self, row) -> str:
        """Create hash for structure dimension"""
        direction = str(row.get('direction', '')).strip() if pd.notna(row.get('direction')) else ''
        balcony_dir = str(row.get('balcony_direction', '')).strip() if pd.notna(row.get('balcony_direction')) else ''
        # structure_type = str(self._get_structure_type(row)).strip()
        structure_type = random.choice(['concrete', 'wood', 'steel', 'brick'])  # Placeholder for actual structure type logic
        layout_type = str(self._get_layout_type(row)).strip()
        
        structure_str = f"{direction}_{balcony_dir}_{structure_type}_{layout_type}"
        return hashlib.md5(structure_str.encode()).hexdigest()[:16]

    def _get_or_create_amenity_key(self, amenity_name) -> int:
        """Get or create amenity key"""
        amenity_hash = self._hash_amenity(amenity_name)
        if amenity_hash not in self.amenity_cache:
            self.amenity_cache[amenity_hash] = self.next_amenity_key
            self.next_amenity_key += 1
        return self.amenity_cache[amenity_hash]

    def _hash_amenity(self, amenity_name) -> str:
        """Create hash for amenity dimension"""
        name = str(amenity_name).strip() if amenity_name else ''
        category = str(self._categorize_amenity(name)).strip()
        amenity_type = str(self._get_amenity_type(name)).strip()
        
        amenity_str = f"{name}_{category}_{amenity_type}"
        return hashlib.md5(amenity_str.encode()).hexdigest()[:16]

    def _get_or_create_contact_key(self, row) -> int:
        """Get or create contact key"""
        contact_hash = self._hash_contact(row)
        if contact_hash not in self.contact_cache:
            self.contact_cache[contact_hash] = self.next_contact_key
            self.next_contact_key += 1
        return self.contact_cache[contact_hash]

    def _hash_contact(self, row) -> str:
        """Create hash for contact dimension"""
        contact_type = str(row.get('contact_type', '')).strip() if pd.notna(row.get('contact_type')) else ''
        is_agent = str(self._is_agent(row)).strip()
        is_company = str(self._is_company(row)).strip()
        agent_level = str(self._get_agent_level(row)).strip()
        company_size = str(self._get_company_size(row)).strip()
        
        contact_str = f"{contact_type}_{is_agent}_{is_company}_{agent_level}_{company_size}"
        return hashlib.md5(contact_str.encode()).hexdigest()[:16]
    
    def _get_time_key(self, created_at: Optional[pd.Timestamp]) -> Optional[int]:
        """
        Get time dimension key based on created_at timestamp.
        
        Args:
            created_at: Timestamp of when the listing was created
            
        Returns:
            Time dimension key or None if invalid input
        """
        if pd.isna(created_at) or not isinstance(created_at, (pd.Timestamp, datetime)):
            return None
            
        # Assume time dimension table has keys based on date
        # Format: YYYYMMDD (e.g., 20250620 for June 20, 2025)
        try:
            return int(created_at.strftime('%Y%m%d'))
        except (ValueError, AttributeError):
            return None

    def _get_location_key_for_listing(self, row: pd.Series, oltp_data: Dict[str, pd.DataFrame]) -> Optional[int]:
        """
        Get location dimension key based on listing data.
        
        Args:
            row: Series containing listing data
            oltp_data: Dictionary of OLTP DataFrames
            
        Returns:
            Location dimension key or None if not found
        """
        # Assume location data is in raw_property_listings or related tables
        city = row.get('city')
        district = row.get('district')
        ward = row.get('ward')
        
        if pd.isna(city) or pd.isna(district):
            return None
            
        # Assume location dimension table exists in oltp_data
        location_df = oltp_data.get('dim_location', pd.DataFrame())
        if location_df.empty:
            return None
            
        # Match location based on city, district, ward
        condition = (
            (location_df['city'] == city) & 
            (location_df['district'] == district)
        )
        if not pd.isna(ward):
            condition &= (location_df['ward'] == ward)
            
        matching_locations = location_df[condition]
        if not matching_locations.empty:
            return matching_locations['location_key'].iloc[0]
        return None

    def _get_property_key_for_listing(self, row: pd.Series) -> Optional[int]:
        """
        Get property dimension key based on listing data.
        
        Args:
            row: Series containing listing data
            
        Returns:
            Property dimension key or None if not found
        """
        property_type = row.get('property_type')
        if pd.isna(property_type):
            return None
            
        # Assume property dimension table is maintained separately
        # Simple mapping for common property types (this could be a lookup table)
        property_type_map = {
            'house': 1,
            'apartment': 2,
            'condo': 3,
            'land': 4,
            'villa': 5
        }
        
        return property_type_map.get(property_type.lower(), None)

    def _get_legal_key_for_listing(self, row: pd.Series, oltp_data: Dict[str, pd.DataFrame]) -> Optional[int]:
        """
        Get legal dimension key based on listing data.
        
        Args:
            row: Series containing listing data
            oltp_data: Dictionary of OLTP DataFrames
            
        Returns:
            Legal dimension key or None if not found
        """
        legal_status = row.get('legal_status')
        if pd.isna(legal_status):
            return None
            
        # Assume legal dimension table exists in oltp_data
        legal_df = oltp_data.get('dim_legal', pd.DataFrame())
        if legal_df.empty:
            return None
            
        matching_legal = legal_df[legal_df['legal_status'] == legal_status]
        if not matching_legal.empty:
            return matching_legal['legal_key'].iloc[0]
        return None

    def _get_structure_key_for_listing(self, detail_row: Dict) -> Optional[int]:
        """
        Get structure dimension key based on property details.
        
        Args:
            detail_row: Dictionary containing property details
            
        Returns:
            Structure dimension key or None if not found
        """
        structure_type = detail_row.get('structure_type')
        if not structure_type:
            return None
            
        # Assume structure dimension table with simple mapping
        structure_type_map = {
            'detached': 1,
            'semi-detached': 2,
            'terraced': 3,
            'apartment': 4
        }
        
        return structure_type_map.get(structure_type.lower(), None)

    def _get_contact_key_for_listing(self, row: pd.Series, oltp_data: Dict[str, pd.DataFrame]) -> Optional[int]:
        """
        Get contact dimension key based on listing data.
        
        Args:
            row: Series containing listing data
            oltp_data: Dictionary of OLTP DataFrames
            
        Returns:
            Contact dimension key or None if not found
        """
        contact_id = row.get('contact_id')
        if pd.isna(contact_id):
            return None
            
        # Assume contact dimension table exists in oltp_data
        contact_df = oltp_data.get('dim_contact', pd.DataFrame())
        if contact_df.empty:
            return None
            
        matching_contact = contact_df[contact_df['contact_id'] == contact_id]
        if not matching_contact.empty:
            return matching_contact['contact_key'].iloc[0]
        return None

    def _convert_to_numeric(self, value) -> float:
        """Convert value to numeric, return 0 if conversion fails"""
        try:
            if pd.isna(value) or value == '' or value is None:
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def _safe_str(self, value) -> str:
        """Safely convert value to string, handle NaN and None"""
        if pd.isna(value) or value is None:
            return ''
        return str(value).strip()

    def _get_season(self, month: int) -> str:
        """Get season from month"""
        try:
            month = int(month) if pd.notna(month) else 1
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            else:
                return 'Fall'
        except (ValueError, TypeError):
            return 'Unknown'

    def _get_region(self, city: str) -> str:
        """Get region from city"""
        city = self._safe_str(city).lower()
        
        north_cities = ['hà nội', 'hai phong', 'hải phòng', 'quang ninh', 'quảng ninh', 'nam định', 'nam dinh']
        south_cities = ['tp.hcm', 'hồ chí minh', 'ho chi minh', 'can tho', 'cần thơ', 'vung tau', 'vũng tàu', 'dong nai', 'đồng nai', 'binh duong', 'bình dương']
        
        if any(nc in city for nc in north_cities):
            return 'Miền Bắc'
        elif any(sc in city for sc in south_cities):
            return 'Miền Nam'
        else:
            return 'Miền Trung'

    def _get_city_tier(self, city: str) -> str:
        """Get city tier"""
        city = self._safe_str(city).lower()
        
        tier1_cities = ['hà nội', 'tp.hcm', 'hồ chí minh', 'ho chi minh']
        tier2_cities = ['đà nẵng', 'da nang', 'hải phòng', 'hai phong', 'cần thơ', 'can tho']
        
        if any(t1 in city for t1 in tier1_cities):
            return 'Tier 1'
        elif any(t2 in city for t2 in tier2_cities):
            return 'Tier 2'
        else:
            return 'Tier 3'

    def _calculate_location_score(self, row) -> float:
        """Calculate location score based on various factors"""
        try:
            base_score = 50.0
            
            city = self._safe_str(row.get('city', '')).lower()
            
            # Major cities get higher scores
            if any(major_city in city for major_city in ['hà nội', 'tp.hcm', 'hồ chí minh', 'ho chi minh']):
                base_score += 30
            elif any(big_city in city for big_city in ['đà nẵng', 'da nang']):
                base_score += 20
            elif any(medium_city in city for medium_city in ['hải phòng', 'hai phong', 'cần thơ', 'can tho']):
                base_score += 15
            
            # District bonus (if available)
            district = self._safe_str(row.get('district', '')).lower()
            if any(central_district in district for central_district in ['quận 1', 'quan 1', 'ba đình', 'ba dinh', 'hoàn kiếm', 'hoan kiem']):
                base_score += 10
            
            return min(base_score, 100.0)
        except Exception as e:
            # Log error if needed
            return 50.0  # Default score

    def _calculate_infrastructure_score(self, row) -> float:
        """Calculate infrastructure score"""
        try:
            base_score = 50.0
            
            # Add scoring based on available infrastructure data
            city = self._safe_str(row.get('city', '')).lower()
            
            # Major cities have better infrastructure
            if any(major_city in city for major_city in ['hà nội', 'tp.hcm', 'hồ chí minh']):
                base_score += 25
            elif any(big_city in city for big_city in ['đà nẵng', 'hải phòng', 'cần thơ']):
                base_score += 15
            
            return min(base_score, 100.0)
        except Exception:
            return 75.0  # Default score

    def _get_property_category(self, property_type: str) -> str:
        """Categorize property type"""
        try:
            property_type = self._safe_str(property_type).lower()
            
            if any(x in property_type for x in ['chung cư', 'chung cu', 'căn hộ', 'can ho', 'apartment']):
                return 'Residential'
            elif any(x in property_type for x in ['shophouse', 'văn phòng', 'van phong', 'mặt bằng', 'mat bang', 'office', 'commercial']):
                return 'Commercial'
            elif any(x in property_type for x in ['đất', 'dat', 'land']):
                return 'Land'
            else:
                return 'Mixed'
        except Exception:
            return 'Mixed'

    def _get_property_segment(self, row) -> str:
        """Get property segment based on price"""
        try:
            price = self._convert_to_numeric(row.get('price_value', 0))
            if price > 10000000000:  # > 10 tỷ
                return 'Luxury'
            elif price > 3000000000:  # > 3 tỷ
                return 'Mid-range'
            else:
                return 'Affordable'
        except Exception:
            return 'Affordable'

    def _get_age_group(self, year_built) -> str:
        """Get age group of property"""
        try:
            if pd.isna(year_built) or year_built is None or year_built == '':
                return 'Unknown'
            
            current_year = datetime.now().year
            year_built = int(float(year_built)) if str(year_built).replace('.', '', 1).isdigit() else 0
            
            if year_built <= 0 or year_built > current_year:
                return 'Unknown'
            
            age = current_year - year_built
            
            if age <= 2:
                return 'Mới'
            elif age <= 10:
                return '2-10 năm'
            else:
                return '>10 năm'
        except (ValueError, TypeError):
            return 'Unknown'

    def _get_building_type(self, row) -> str:
        """Get building type"""
        try:
            floors = self._convert_to_numeric(row.get('floors', 0))
            if floors >= 10:
                return 'High-rise'
            elif floors >= 5:
                return 'Mid-rise'
            else:
                return 'Low-rise'
        except Exception:
            return 'Low-rise'
        
    def connect(self):
        """Establish database connection"""
        try:
            self.engine = sa.create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            logger.info(f"Connected to MySQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {str(e)}")
            raise
    
    def create_schema_if_not_exists(self):
        """Create schema if it doesn't exist"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS `{self.schema_name}`"))
                conn.commit()
                logger.info(f"Schema {self.schema_name} ready")
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise
    def _get_or_create_legal_key(self, row: pd.Series) -> str:
        """Tạo hoặc lấy legal key duy nhất"""
        legal_docs = str(row.get('legal_documents', ''))
        key_input = f"{legal_docs}"
        
        if key_input not in self.legal_cache:
            hash_obj = hashlib.md5(key_input.encode())
            self.legal_cache[key_input] = f"LEGAL_{hash_obj.hexdigest()[:8].upper()}"
        
        return self.legal_cache[key_input]
    
    def _categorize_legal_status(self, legal_docs: str) -> str:
        """Phân loại trạng thái pháp lý"""
        legal_docs = str(legal_docs).lower()
        
        if 'sổ đỏ' in legal_docs or 'sổ hồng' in legal_docs:
            return 'SỔ ĐỎ/HỒNG'
        elif 'sổ trắng' in legal_docs:
            return 'SỔ TRẮNG'
        elif 'giấy tờ hợp lệ' in legal_docs or 'đầy đủ' in legal_docs:
            return 'GIẤY TỜ ĐẦY ĐỦ'
        elif 'đang làm' in legal_docs or 'chờ cấp' in legal_docs:
            return 'ĐANG XỬ LÝ'
        else:
            return 'CHƯA RÕ'
    
    def _can_get_bank_loan(self, legal_docs: str) -> bool:
        """Kiểm tra có thể vay ngân hàng không"""
        legal_docs = str(legal_docs).lower()
        
        loan_keywords = ['sổ đỏ', 'sổ hồng', 'giấy tờ hợp lệ', 'đầy đủ']
        return any(keyword in legal_docs for keyword in loan_keywords)
    
    def _is_transferable(self, legal_docs: str) -> bool:
        """Kiểm tra có thể chuyển nhượng không"""
        legal_docs = str(legal_docs).lower()
        
        transferable_keywords = ['sổ đỏ', 'sổ hồng', 'chuyển nhượng được']
        non_transferable_keywords = ['hạn chế', 'không chuyển nhượng', 'tranh chấp']
        
        if any(keyword in legal_docs for keyword in non_transferable_keywords):
            return False
        
        return any(keyword in legal_docs for keyword in transferable_keywords)
    
    def _assess_legal_risk(self, legal_docs: str) -> str:
        """Đánh giá mức độ rủi ro pháp lý"""
        legal_docs = str(legal_docs).lower()
        
        high_risk_keywords = ['tranh chấp', 'không rõ nguồn gốc', 'giả mạo']
        medium_risk_keywords = ['sổ trắng', 'đang làm', 'chờ cấp']
        low_risk_keywords = ['sổ đỏ', 'sổ hồng', 'giấy tờ hợp lệ']
        
        if any(keyword in legal_docs for keyword in high_risk_keywords):
            return 'CAO'
        elif any(keyword in legal_docs for keyword in medium_risk_keywords):
            return 'TRUNG BÌNH'
        elif any(keyword in legal_docs for keyword in low_risk_keywords):
            return 'THẤP'
        else:
            return 'CHƯA XÁC ĐỊNH'
    
    def _calculate_legal_score(self, legal_docs: str) -> float:
        """Tính điểm pháp lý (0-100)"""
        legal_docs = str(legal_docs).lower()
        score = 50  # Điểm cơ bản
        
        # Điểm cộng
        if 'sổ đỏ' in legal_docs or 'sổ hồng' in legal_docs:
            score += 40
        elif 'giấy tờ hợp lệ' in legal_docs:
            score += 30
        elif 'sổ trắng' in legal_docs:
            score += 10
        
        # Điểm trừ
        if 'tranh chấp' in legal_docs:
            score -= 30
        elif 'không rõ' in legal_docs:
            score -= 20
        elif 'đang làm' in legal_docs:
            score -= 10
        
        return max(0, min(100, score))
    
    # ==================== STRUCTURE DIMENSION HELPERS ====================
    
    def _get_or_create_structure_key(self, row: pd.Series) -> str:
        """Tạo hoặc lấy structure key duy nhất"""
        direction = str(row.get('direction', ''))
        balcony_direction = str(row.get('balcony_direction', ''))
        structure_type = str(row.get('structure_type', ''))
        
        key_input = f"{direction}_{balcony_direction}_{structure_type}"
        
        if key_input not in self.structure_key_cache:
            hash_obj = hashlib.md5(key_input.encode())
            self.structure_key_cache[key_input] = f"STRUCT_{hash_obj.hexdigest()[:8].upper()}"
        
        return self.structure_key_cache[key_input]
    
    def _get_structure_type(self, row: pd.Series) -> str:
        """Xác định loại cấu trúc"""
        return 'PHONG NGU' # Placeholder for actual logic
        # Có thể dựa vào các thông tin khác trong row
        # bedrooms = row.get('bedrooms', 0)
        # bathrooms = row.get('bathrooms', 0)
        
        # if bedrooms == 1:
        #     return 'STUDIO/1PN'
        # elif bedrooms == 2:
        #     return '2 PHÒNG NGỦ'
        # elif bedrooms == 3:
        #     return '3 PHÒNG NGỦ'
        # elif bedrooms >= 4:
        #     return 'PENTHOUSE/VILLA'
        # else:
        #     return 'KHÔNG XÁC ĐỊNH'
    
    def _get_layout_type(self, row: pd.Series) -> str:
        """Xác định kiểu layout"""
        area = row.get('area', 0)
        bedrooms = row.get('bedrooms', 0)
        
        if area > 0 and bedrooms > 0:
            area_per_room = area / bedrooms
            if area_per_room > 25:
                return 'RỘNG RÃI'
            elif area_per_room > 15:
                return 'TIÊU CHUẨN'
            else:
                return 'COMPACT'
        
        return 'KHÔNG XÁC ĐỊNH'
    
    def _calculate_feng_shui_score(self, direction: str) -> float:
        """Tính điểm phong thủy dựa trên hướng (0-100)"""
        direction = str(direction).lower()
        
        feng_shui_scores = {
            'đông': 85,
            'đông nam': 90,
            'nam': 95,
            'tây nam': 75,
            'tây': 60,
            'tây bắc': 70,
            'bắc': 80,
            'đông bắc': 65
        }
        
        for dir_key, score in feng_shui_scores.items():
            if dir_key in direction:
                return score
        
        return 50  # Điểm mặc định
    
    def _calculate_ventilation_score(self, row: pd.Series) -> float:
        """Tính điểm thông gió (0-100)"""
        direction = str(row.get('direction', '')).lower()
        balcony_direction = str(row.get('balcony_direction', '')).lower()
        
        score = 50  # Điểm cơ bản
        
        # Hướng tốt cho thông gió
        good_directions = ['đông', 'đông nam', 'nam']
        if any(dir in direction for dir in good_directions):
            score += 20
        
        # Ban công tốt cho thông gió
        if any(dir in balcony_direction for dir in good_directions):
            score += 15
        
        # Nếu có nhiều hướng (góc)
        if 'góc' in direction or len([d for d in good_directions if d in direction]) > 1:
            score += 10
        
        return min(100, score)
    
    def _calculate_lighting_score(self, row: pd.Series) -> float:
        """Tính điểm ánh sáng (0-100)"""
        direction = str(row.get('direction', '')).lower()
        
        score = 50  # Điểm cơ bản
        
        # Hướng tốt cho ánh sáng
        if 'nam' in direction:
            score += 30
        elif 'đông' in direction or 'đông nam' in direction:
            score += 25
        elif 'tây nam' in direction:
            score += 15
        elif 'bắc' in direction:
            score -= 10
        
        return max(0, min(100, score))
    
    # ==================== AMENITY DIMENSION HELPERS ====================
    
    def _get_or_create_amenity_key(self, amenity_name: str) -> str:
        """Tạo hoặc lấy amenity key duy nhất"""
        amenity_name = str(amenity_name)
        
        if amenity_name not in self.amenity_key_cache:
            hash_obj = hashlib.md5(amenity_name.encode())
            self.amenity_key_cache[amenity_name] = f"AMEN_{hash_obj.hexdigest()[:8].upper()}"
        
        return self.amenity_key_cache[amenity_name]
    
    def _categorize_amenity(self, amenity_name: str) -> str:
        """Phân loại tiện ích"""
        amenity_name = str(amenity_name).lower()
        
        security_keywords = ['bảo vệ', 'an ninh', 'camera', 'thẻ từ']
        fitness_keywords = ['gym', 'hồ bơi', 'thể thao', 'sân tennis']
        convenience_keywords = ['siêu thị', 'cửa hàng', 'atm', 'cafe']
        transport_keywords = ['xe bus', 'tàu điện', 'bãi đỗ xe', 'thang máy']
        
        if any(keyword in amenity_name for keyword in security_keywords):
            return 'AN NINH'
        elif any(keyword in amenity_name for keyword in fitness_keywords):
            return 'THỂ THAO & GIẢI TRÍ'
        elif any(keyword in amenity_name for keyword in convenience_keywords):
            return 'TIỆN ÍCH'
        elif any(keyword in amenity_name for keyword in transport_keywords):
            return 'GIAO THÔNG'
        else:
            return 'KHÁC'
    
    def _get_amenity_type(self, amenity_name: str) -> str:
        """Xác định loại tiện ích"""
        amenity_name = str(amenity_name).lower()
        
        if 'nội khu' in amenity_name or 'trong tòa' in amenity_name:
            return 'NỘI KHU'
        elif 'gần' in amenity_name or 'xung quanh' in amenity_name:
            return 'BÊN NGOÀI'
        else:
            return 'CHUNG'
    
    def _get_amenity_value_impact(self, amenity_name: str) -> str:
        """Đánh giá tác động lên giá trị BDS"""
        amenity_name = str(amenity_name).lower()
        
        high_impact = ['hồ bơi', 'gym', 'an ninh 24/7', 'thang máy']
        medium_impact = ['siêu thị', 'trường học', 'bệnh viện']
        low_impact = ['cửa hàng', 'cafe', 'atm']
        
        if any(keyword in amenity_name for keyword in high_impact):
            return 'CAO'
        elif any(keyword in amenity_name for keyword in medium_impact):
            return 'TRUNG BÌNH'
        elif any(keyword in amenity_name for keyword in low_impact):
            return 'THẤP'
        else:
            return 'TRUNG BÌNH'
    
    def _calculate_amenity_popularity(self, amenity_name: str, amenities_df: pd.DataFrame) -> float:
        """Tính điểm phổ biến của tiện ích (0-100)"""
        if amenities_df.empty:
            return 50
        
        total_properties = len(amenities_df['property_id'].unique()) if 'property_id' in amenities_df.columns else len(amenities_df)
        amenity_count = len(amenities_df[amenities_df['amenity_name'] == amenity_name])
        
        if total_properties > 0:
            popularity = (amenity_count / total_properties) * 100
            return min(100, popularity)
        
        return 50
    
    def _estimate_maintenance_cost(self, amenity_name: str) -> str:
        """Ước tính chi phí bảo trì"""
        amenity_name = str(amenity_name).lower()
        
        high_cost = ['hồ bơi', 'thang máy', 'gym', 'sân tennis']
        medium_cost = ['bảo vệ', 'camera', 'điều hòa chung']
        low_cost = ['thẻ từ', 'cổng tự động', 'đèn led']
        
        if any(keyword in amenity_name for keyword in high_cost):
            return 'CAO'
        elif any(keyword in amenity_name for keyword in medium_cost):
            return 'TRUNG BÌNH'
        elif any(keyword in amenity_name for keyword in low_cost):
            return 'THẤP'
        else:
            return 'TRUNG BÌNH'
    
    # ==================== CONTACT DIMENSION HELPERS ====================
    
    def _get_or_create_contact_key(self, row: pd.Series) -> str:
        """Tạo hoặc lấy contact key duy nhất"""
        phone = str(row.get('phone', ''))
        email = str(row.get('email', ''))
        name = str(row.get('name', ''))
        
        key_input = f"{phone}_{email}_{name}"
        
        if key_input not in self.contact_key_cache:
            hash_obj = hashlib.md5(key_input.encode())
            self.contact_key_cache[key_input] = f"CONT_{hash_obj.hexdigest()[:8].upper()}"
        
        return self.contact_key_cache[key_input]
    
    def _is_agent(self, row: pd.Series) -> bool:
        """Kiểm tra có phải môi giới không"""
        contact_type = str(row.get('contact_type', '')).lower()
        name = str(row.get('name', '')).lower()
        
        agent_keywords = ['môi giới', 'agent', 'broker', 'sale']
        return any(keyword in contact_type for keyword in agent_keywords) or \
               any(keyword in name for keyword in agent_keywords)
    
    def _is_company(self, row: pd.Series) -> bool:
        """Kiểm tra có phải công ty không"""
        name = str(row.get('name', '')).lower()
        contact_type = str(row.get('contact_type', '')).lower()
        
        company_keywords = ['công ty', 'ctcp', 'tnhh', 'jsc', 'ltd', 'company']
        return any(keyword in name for keyword in company_keywords) or \
               'công ty' in contact_type
    
    def _get_agent_level(self, row: pd.Series) -> str:
        """Xác định cấp độ môi giới"""
        if not self._is_agent(row):
            return 'KHÔNG PHẢI MG'
        
        name = str(row.get('name', '')).lower()
        
        if 'senior' in name or 'trưởng' in name:
            return 'SENIOR'
        elif 'junior' in name or 'mới' in name:
            return 'JUNIOR'
        else:
            return 'STANDARD'
    
    def _get_company_size(self, row: pd.Series) -> str:
        """Ước tính quy mô công ty"""
        if not self._is_company(row):
            return 'CÁ NHÂN'
        
        name = str(row.get('name', '')).lower()
        
        if any(keyword in name for keyword in ['tập đoàn', 'group', 'holdings']):
            return 'LỚN'
        elif any(keyword in name for keyword in ['ctcp', 'jsc']):
            return 'TRUNG BÌNH'
        else:
            return 'NHỎ'
    
    def _estimate_experience_years(self, row: pd.Series) -> int:
        """Ước tính số năm kinh nghiệm"""
        # Đây là ước tính dựa trên thông tin có sẵn
        # Trong thực tế cần thêm dữ liệu về lịch sử hoạt động
        
        if self._is_company(row):
            company_size = self._get_company_size(row)
            if company_size == 'LỚN':
                return 10
            elif company_size == 'TRUNG BÌNH':
                return 5
            else:
                return 2
        elif self._is_agent(row):
            agent_level = self._get_agent_level(row)
            if agent_level == 'SENIOR':
                return 7
            elif agent_level == 'JUNIOR':
                return 1
            else:
                return 3
        else:
            return 1  # Chủ nhà thường ít kinh nghiệm
    
    def _estimate_listing_count(self, row: pd.Series) -> int:
        """Ước tính số tin đăng"""
        experience = self._estimate_experience_years(row)
        
        if self._is_company(row):
            return experience * 50  # Công ty có nhiều tin hơn
        elif self._is_agent(row):
            return experience * 20  # Môi giới cá nhân
        else:
            return 1  # Chủ nhà thường chỉ có 1 tin
    
    def _estimate_success_rate(self, row: pd.Series) -> float:
        """Ước tính tỷ lệ thành công (0-100)"""
        experience = self._estimate_experience_years(row)
        
        # Tỷ lệ thành công tăng theo kinh nghiệm nhưng có giới hạn
        base_rate = 30
        experience_bonus = min(experience * 5, 40)  # Tối đa 40 điểm từ kinh nghiệm
        
        if self._is_company(row):
            company_bonus = 15
        elif self._is_agent(row):
            company_bonus = 10
        else:
            company_bonus = 0
        
        return min(100, base_rate + experience_bonus + company_bonus)
    
    def _get_rating(self, row: pd.Series) -> float:
        """Ước tính rating (1-5 sao)"""
        success_rate = self._estimate_success_rate(row)
        
        # Chuyển đổi success rate thành rating 1-5
        if success_rate >= 80:
            return 5.0
        elif success_rate >= 70:
            return 4.5
        elif success_rate >= 60:
            return 4.0
        elif success_rate >= 50:
            return 3.5
        elif success_rate >= 40:
            return 3.0
        elif success_rate >= 30:
            return 2.5
        else:
            return 2.0
    def _calculate_amenity_score(self, listing_amenities: pd.DataFrame) -> float:
        """Tính điểm tổng thể cho tiện ích (0-100)"""
        if listing_amenities.empty:
            return 0.0
        
        total_score = 0
        amenity_count = len(listing_amenities)
        
        for _, amenity_row in listing_amenities.iterrows():
            amenity_name = str(amenity_row.get('amenity_name', '')).lower()
            
            # Điểm cơ bản cho mỗi tiện ích
            base_score = 5
            
            # Điểm thưởng cho tiện ích luxury
            luxury_bonus = 0
            for luxury_amenity in self.luxury_amenities:
                if luxury_amenity.lower() in amenity_name:
                    luxury_bonus = 15
                    break
            
            # Điểm thưởng cho tiện ích phổ biến/quan trọng
            important_bonus = 0
            important_amenities = ['thang máy', 'bảo vệ', 'điều hòa', 'bãi đỗ xe']
            for important_amenity in important_amenities:
                if important_amenity in amenity_name:
                    important_bonus = 8
                    break
            
            amenity_score = base_score + luxury_bonus + important_bonus
            total_score += amenity_score
        
        # Tính điểm trung bình và chuẩn hóa về thang 100
        if amenity_count > 0:
            avg_score = total_score / amenity_count
            # Chuẩn hóa: giả sử điểm tối đa mỗi tiện ích là 20
            normalized_score = min(100, (avg_score / 20) * 100)
            return round(normalized_score, 2)
        
        return 0.0
    
    def _has_amenity(self, listing_amenities: pd.DataFrame, amenity_type: str) -> bool:
        """Kiểm tra listing có tiện ích cụ thể hay không"""
        if listing_amenities.empty:
            return False
        
        keywords = self.amenity_keywords.get(amenity_type, [])
        
        for _, amenity_row in listing_amenities.iterrows():
            amenity_name = str(amenity_row.get('amenity_name', '')).lower()
            
            for keyword in keywords:
                if keyword.lower() in amenity_name:
                    return True
        
        return False
    
    def _count_luxury_amenities(self, listing_amenities: pd.DataFrame) -> int:
        """Đếm số lượng tiện ích luxury"""
        if listing_amenities.empty:
            return 0
        
        luxury_count = 0
        
        for _, amenity_row in listing_amenities.iterrows():
            amenity_name = str(amenity_row.get('amenity_name', '')).lower()
            
            for luxury_amenity in self.luxury_amenities:
                if luxury_amenity.lower() in amenity_name:
                    luxury_count += 1
                    break  # Tránh đếm trùng cho cùng một amenity
        
        return luxury_count
    
    def _count_basic_amenities(self, listing_amenities: pd.DataFrame) -> int:
        """Đếm số lượng tiện ích basic"""
        if listing_amenities.empty:
            return 0
        
        basic_count = 0
        
        for _, amenity_row in listing_amenities.iterrows():
            amenity_name = str(amenity_row.get('amenity_name', '')).lower()
            
            for basic_amenity in self.basic_amenities:
                if basic_amenity.lower() in amenity_name:
                    basic_count += 1
                    break  # Tránh đếm trùng cho cùng một amenity
        
        return basic_count
    
    def _calculate_amenity_premium(self, listing_amenities: pd.DataFrame) -> float:
        """Tính phần trăm tăng giá do tiện ích (%)"""
        if listing_amenities.empty:
            return 0.0
        
        premium_percentage = 0.0
        
        # Định nghĩa mức tăng giá cho từng loại tiện ích
        amenity_premiums = {
            'hồ bơi': 8.0,
            'gym': 5.0,
            'spa': 6.0,
            'sân tennis': 10.0,
            'thang máy': 3.0,
            'bảo vệ 24/7': 4.0,
            'bãi đỗ xe': 2.0,
            'điều hòa': 1.5,
            'ban công': 2.5,
            'sân vườn': 4.0,
            'view đẹp': 7.0,
            'gần trung tâm': 5.0
        }
        
        for _, amenity_row in listing_amenities.iterrows():
            amenity_name = str(amenity_row.get('amenity_name', '')).lower()
            
            # Tìm mức premium phù hợp
            for amenity_key, premium_value in amenity_premiums.items():
                if amenity_key.lower() in amenity_name:
                    premium_percentage += premium_value
                    break
            else:
                # Nếu không tìm thấy trong danh sách, gán premium mặc định
                premium_percentage += 1.0
        
        # Giới hạn premium tối đa là 50%
        return min(50.0, round(premium_percentage, 2))
    
    def _analyze_amenity_trends(self, listing_amenities: pd.DataFrame) -> Dict[str, Any]:
        """Phân tích xu hướng tiện ích (hàm bổ sung)"""
        if listing_amenities.empty:
            return {
                'most_common_amenity': None,
                'amenity_diversity_score': 0,
                'modern_amenities_ratio': 0
            }
        
        # Đếm tần suất xuất hiện của các tiện ích
        amenity_counts = {}
        modern_count = 0
        total_count = len(listing_amenities)
        
        modern_amenities = ['smart home', 'wifi', 'charging station', 'app control', 'iot']
        
        for _, amenity_row in listing_amenities.iterrows():
            amenity_name = str(amenity_row.get('amenity_name', '')).lower()
            
            # Đếm tần suất
            amenity_counts[amenity_name] = amenity_counts.get(amenity_name, 0) + 1
            
            # Đếm tiện ích hiện đại
            for modern_amenity in modern_amenities:
                if modern_amenity in amenity_name:
                    modern_count += 1
                    break
        
        # Tìm tiện ích phổ biến nhất
        most_common = max(amenity_counts.items(), key=lambda x: x[1])[0] if amenity_counts else None
        
        # Tính điểm đa dạng (số loại tiện ích khác nhau / tổng số tiện ích)
        diversity_score = len(amenity_counts) / total_count if total_count > 0 else 0
        
        # Tỷ lệ tiện ích hiện đại
        modern_ratio = modern_count / total_count if total_count > 0 else 0
        
        return {
            'most_common_amenity': most_common,
            'amenity_diversity_score': round(diversity_score, 3),
            'modern_amenities_ratio': round(modern_ratio, 3)
        }
    
    def _calculate_amenity_market_value(self, listing_amenities: pd.DataFrame, base_price: float = 0) -> float:
        """Tính giá trị thị trường của các tiện ích"""
        if listing_amenities.empty or base_price <= 0:
            return 0.0
        
        premium_percentage = self._calculate_amenity_premium(listing_amenities)
        return base_price * (premium_percentage / 100)
    
    def _get_amenity_category_distribution(self, listing_amenities: pd.DataFrame) -> Dict[str, int]:
        """Phân bố các category tiện ích"""
        if listing_amenities.empty:
            return {}
        
        categories = {
            'an_ninh': 0,
            'giai_tri': 0,
            'tien_ich': 0,
            'giao_thong': 0,
            'khac': 0
        }
        
        category_keywords = {
            'an_ninh': ['bảo vệ', 'camera', 'an ninh', 'thẻ từ'],
            'giai_tri': ['hồ bơi', 'gym', 'sân chơi', 'rạp phim'],
            'tien_ich': ['siêu thị', 'atm', 'nhà hàng', 'cafe'],
            'giao_thong': ['bãi đỗ xe', 'thang máy', 'xe bus', 'tàu điện']
        }
        
        for _, amenity_row in listing_amenities.iterrows():
            amenity_name = str(amenity_row.get('amenity_name', '')).lower()
            categorized = False
            
            for category, keywords in category_keywords.items():
                if any(keyword in amenity_name for keyword in keywords):
                    categories[category] += 1
                    categorized = True
                    break
            
            if not categorized:
                categories['khac'] += 1
        
        return categories
    
    def create_dimension_tables(self):
        """Create all dimension tables"""
        
        # Dimension Time
        dim_time = Table(
            'dim_time', self.metadata,
            Column('time_key', String(255), primary_key=True),
            Column('full_date', Date),
            Column('year', Integer),
            Column('quarter', Integer),
            Column('month', Integer),
            Column('month_name', String(255)),
            Column('week', Integer),
            Column('day_of_month', Integer),
            Column('day_of_week', Integer),
            Column('day_name', String(255)),
            Column('is_weekend', Boolean),
            Column('is_holiday', Boolean),
            Column('season', String(255)),
            Column('fiscal_year', Integer),
            Column('fiscal_quarter', Integer),
            schema=self.schema_name
        )
        
        # Dimension Location
        dim_location = Table(
            'dim_location', self.metadata,
            Column('location_key', String(255), primary_key=True),
            Column('city', String(255)),
            Column('district', String(255)),
            Column('ward', String(255)),
            Column('street', String(200)),
            Column('project_name', String(200)),
            Column('region', String(255)),
            Column('city_tier', String(255)),
            Column('location_score', DECIMAL(5, 2)),
            Column('infrastructure_score', DECIMAL(5, 2)),
            Column('full_address', Text),
            Column('latitude', DECIMAL(10, 8)),
            Column('longitude', DECIMAL(11, 8)),
            schema=self.schema_name
        )
        
        # Dimension Property
        dim_property = Table(
            'dim_property', self.metadata,
            Column('property_key',String(255), primary_key=True),
            Column('property_type', String(255)),
            Column('property_subtype', String(255)),
            Column('property_category', String(255)),
            Column('property_segment', String(255)),
            Column('age_group', String(255)),
            Column('building_type', String(255)),
            Column('ownership_type', String(255)),
            schema=self.schema_name
        )
        
        # Dimension Legal
        dim_legal = Table(
            'dim_legal', self.metadata,
            Column('legal_key', String(255), primary_key=True),
            Column('legal_status', String(200)),
            Column('legal_category', String(255)),
            Column('can_get_loan', Boolean),
            Column('transferable', Boolean),
            Column('legal_risk_level', String(255)),
            Column('legal_score', DECIMAL(5, 2)),
            schema=self.schema_name
        )
        
        # Dimension Structure
        dim_structure = Table(
            'dim_structure', self.metadata,
            Column('structure_key', String(255), primary_key=True),
            Column('direction', String(255)),
            Column('balcony_direction', String(255)),
            Column('structure_type', String(255)),
            Column('layout_type', String(255)),
            Column('feng_shui_score', DECIMAL(5, 2)),
            Column('ventilation_score', DECIMAL(5, 2)),
            Column('lighting_score', DECIMAL(5, 2)),
            schema=self.schema_name
        )
        
        # Dimension Amenity
        dim_amenity = Table(
            'dim_amenity', self.metadata,
            Column('amenity_key', String(255), primary_key=True),
            Column('amenity_name', String(200)),
            Column('amenity_category', String(255)),
            Column('amenity_type', String(255)),
            Column('value_impact', String(255)),
            Column('popularity_score', DECIMAL(5, 2)),
            Column('maintenance_cost', String(255)),
            schema=self.schema_name
        )
        
        # Dimension Contact
        dim_contact = Table(
            'dim_contact', self.metadata,
            Column('contact_key', String(255), primary_key=True),
            Column('contact_type', String(255)),
            Column('is_agent', Boolean),
            Column('is_company', Boolean),
            Column('agent_level', String(255)),
            Column('company_size', String(255)),
            Column('experience_years', Integer),
            Column('listing_count', Integer),
            Column('success_rate', DECIMAL(5, 2)),
            Column('rating', DECIMAL(3, 2)),
            schema=self.schema_name
        )
        
        # Dimension Source
        dim_source = Table(
            'dim_source', self.metadata,
            Column('source_key', String(255), primary_key=True),
            Column('source_name', String(200)),
            Column('source_type', String(255)),
            Column('reliability_score', DECIMAL(5, 2)),
            Column('data_quality_score', DECIMAL(5, 2)),
            Column('update_frequency', String(255)),
            Column('premium_source', Boolean),
            schema=self.schema_name
        )
        
        try:
            self.metadata.create_all(self.engine)
            logger.info("Dimension tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create dimension tables: {str(e)}")
            raise
    
    def create_fact_tables(self):
        """Create all fact tables"""
        
        # Fact Price Analysis
        fact_price_analysis = Table(
            'fact_price_analysis', self.metadata,
            Column('price_fact_id', Integer, primary_key=True, autoincrement=True),
            Column('listing_id', String(255)),
            Column('time_key', Integer),
            Column('location_key', Integer),
            Column('property_key', Integer),
            Column('legal_key', Integer),
            Column('price_value', DECIMAL(15, 2)),
            Column('price_per_m2', DECIMAL(12, 2)),
            Column('price_usd', DECIMAL(12, 2)),
            Column('price_category', String(255)),
            Column('price_change_pct', DECIMAL(8, 4)),
            Column('market_price_ratio', DECIMAL(8, 4)),
            Column('created_at', DateTime, default=datetime.now),
            Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now),
            schema=self.schema_name
        )
        
        # Fact Area Analysis
        fact_area_analysis = Table(
            'fact_area_analysis', self.metadata,
            Column('area_fact_id', Integer, primary_key=True, autoincrement=True),
            Column('listing_id', String(255)),
            Column('time_key', Integer),
            Column('location_key', Integer),
            Column('property_key', Integer),
            Column('structure_key', Integer),
            Column('area_value', DECIMAL(10, 2)),
            Column('bedrooms_count', Integer),
            Column('bathrooms_count', Integer),
            Column('floors_count', Integer),
            Column('width_value', DECIMAL(8, 2)),
            Column('length_value', DECIMAL(8, 2)),
            Column('road_width_value', DECIMAL(8, 2)),
            Column('house_front_value', DECIMAL(8, 2)),
            Column('area_efficiency_ratio', DECIMAL(6, 4)),
            Column('area_category', String(255)),
            Column('room_density', DECIMAL(8, 4)),
            Column('created_at', DateTime, default=datetime.now),
            Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now),
            schema=self.schema_name
        )
        
        # Fact Amenities Analysis
        fact_amenities_analysis = Table(
            'fact_amenities_analysis', self.metadata,
            Column('amenity_fact_id', String(255), primary_key=True),
            Column('listing_id', String(255)),
            Column('time_key', Integer),
            Column('location_key', Integer),
            Column('property_key', Integer),
            Column('amenity_key', Integer),
            Column('amenities_count', Integer),
            Column('amenity_score', DECIMAL(8, 2)),
            Column('has_parking', Boolean),
            Column('has_air_conditioning', Boolean),
            Column('has_balcony', Boolean),
            Column('has_elevator', Boolean),
            Column('has_security', Boolean),
            Column('has_gym', Boolean),
            Column('has_pool', Boolean),
            Column('has_garden', Boolean),
            Column('luxury_amenities_count', Integer),
            Column('basic_amenities_count', Integer),
            Column('amenity_price_premium', DECIMAL(8, 4)),
            Column('created_at', DateTime, default=datetime.now),
            Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now),
            schema=self.schema_name
        )
        
        # Fact Listing Analysis
        fact_listing_analysis = Table(
            'fact_listing_analysis', self.metadata,
            Column('listing_fact_id',Integer, primary_key=True, autoincrement=True),
            Column('listing_id', String(255), unique=True),
            Column('time_key', Integer),
            Column('location_key', Integer),
            Column('property_key', Integer),
            Column('contact_key', Integer),
            Column('source_key', Integer),
            Column('description_length', Integer),
            Column('description_word_count', Integer),
            Column('title_length', Integer),
            Column('days_on_market', Integer),
            Column('view_count', Integer),
            Column('contact_count', Integer),
            Column('listing_quality_score', DECIMAL(8, 2)),
            Column('has_photos', Boolean),
            Column('photos_count', Integer),
            Column('has_video', Boolean),
            Column('has_360_view', Boolean),
            Column('phone_reveal_count', Integer),
            Column('save_count', Integer),
            Column('share_count', Integer),
            Column('is_available', Boolean),
            Column('is_featured', Boolean),
            Column('is_urgent', Boolean),
            Column('has_promotion', Boolean),
            Column('discount_percentage', DECIMAL(5, 2)),
            Column('created_at', DateTime, default=datetime.now),
            Column('updated_at', DateTime, default=datetime.now, onupdate=datetime.now),
            schema=self.schema_name
        )
        
        try:
            self.metadata.create_all(self.engine)
            logger.info("Fact tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create fact tables: {str(e)}")
            raise
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        indexes = [
            # Dimension table indexes
            f"CREATE INDEX IF NOT EXISTS idx_dim_time_date ON `{self.schema_name}`.dim_time (full_date)",
            f"CREATE INDEX IF NOT EXISTS idx_dim_time_year_month ON `{self.schema_name}`.dim_time (year, month)",
            f"CREATE INDEX IF NOT EXISTS idx_dim_location_city ON `{self.schema_name}`.dim_location (city)",
            f"CREATE INDEX IF NOT EXISTS idx_dim_location_region ON `{self.schema_name}`.dim_location (region)",
            f"CREATE INDEX IF NOT EXISTS idx_dim_property_type ON `{self.schema_name}`.dim_property (property_type)",
            f"CREATE INDEX IF NOT EXISTS idx_dim_property_category ON `{self.schema_name}`.dim_property (property_category)",
            
            # Fact table indexes
            f"CREATE INDEX IF NOT EXISTS idx_fact_price_listing_id ON `{self.schema_name}`.fact_price_analysis (listing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_price_time_key ON `{self.schema_name}`.fact_price_analysis (time_key)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_price_location_key ON `{self.schema_name}`.fact_price_analysis (location_key)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_price_property_key ON `{self.schema_name}`.fact_price_analysis (property_key)",
            
            f"CREATE INDEX IF NOT EXISTS idx_fact_area_listing_id ON `{self.schema_name}`.fact_area_analysis (listing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_area_time_key ON `{self.schema_name}`.fact_area_analysis (time_key)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_area_location_key ON `{self.schema_name}`.fact_area_analysis (location_key)",
            
            f"CREATE INDEX IF NOT EXISTS idx_fact_amenities_listing_id ON `{self.schema_name}`.fact_amenities_analysis (listing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_amenities_time_key ON `{self.schema_name}`.fact_amenities_analysis (time_key)",
            
            f"CREATE INDEX IF NOT EXISTS idx_fact_listing_listing_id ON `{self.schema_name}`.fact_listing_analysis (listing_id)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_listing_time_key ON `{self.schema_name}`.fact_listing_analysis (time_key)",
            f"CREATE INDEX IF NOT EXISTS idx_fact_listing_location_key ON `{self.schema_name}`.fact_listing_analysis (location_key)",
        ]
        
        try:
            with self.engine.connect() as conn:
                for index_sql in indexes:
                    conn.execute(text(index_sql))
                conn.commit()
            logger.info("Indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to create indexes: {str(e)}")
            raise
    
    def load_data(self, olap_data: Dict[str, pd.DataFrame], batch_size: int = 1000):
        """
        Load OLAP data into MySQL warehouse
        
        Args:
            olap_data: Dictionary of DataFrames from WarehouseLoader
            batch_size: Batch size for bulk insert
        """
        if not self.engine:
            self.connect()
        
        # self.create_schema_if_not_exists()
        # Drop all tables in the metadata
        logger.info("Dropping all existing OLAP tables...")
        self.metadata.reflect(bind=self.engine, schema=self.schema_name)
        self.metadata.drop_all(bind=self.engine, checkfirst=True)
        logger.info("All OLAP tables dropped successfully.")

        # Clear metadata
        self.metadata.clear()

        # Recreate all tables
        logger.info("Creating fresh OLAP tables...")
        self.create_dimension_tables()
        self.create_fact_tables()
        # self.create_indexes()
        
        # Load dimension tables first (due to foreign key relationships)
        dimension_tables = [
            'dim_time', 'dim_location', 'dim_property', 'dim_legal',
            'dim_structure', 'dim_amenity', 'dim_contact', 'dim_source'
        ]
        
        for table_name in dimension_tables:
            if table_name in olap_data and not olap_data[table_name].empty:
                self._load_table_data(table_name, olap_data[table_name], batch_size)
        
        # Load fact tables
        fact_tables = [
            'fact_price_analysis', 'fact_area_analysis', 
            'fact_amenities_analysis', 'fact_listing_analysis'
        ]
        
        for table_name in fact_tables:
            if table_name in olap_data and not olap_data[table_name].empty:
                self._load_table_data(table_name, olap_data[table_name], batch_size)
        
        logger.info("OLAP data loaded successfully")
    
    def _load_table_data(self, table_name: str, df: pd.DataFrame, batch_size: int):
        """Load data for a specific table"""
        try:
            # Clean data before loading
            df_cleaned = self._clean_dataframe(df)
            
            
            # Use upsert strategy for dimension tables, insert for fact tables
            if table_name.startswith('dim_'):
                self._insert_data(table_name, df_cleaned, batch_size)
            else:
                self._insert_data(table_name, df_cleaned, batch_size)
                
            logger.info(f"Loaded {len(df_cleaned)} records into {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {str(e)}")
            raise
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame before loading"""
        df_clean = df.copy()
        
        # Replace NaN with None for SQL NULL
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        # Convert datetime columns
        for col in df_clean.columns:
            if df_clean[col].dtype == 'datetime64[ns]':
                df_clean[col] = pd.to_datetime(df_clean[col])
        
        return df_clean
    
    def _upsert_data(self, table_name: str, df: pd.DataFrame, batch_size: int):
        """Upsert data (insert or update on duplicate key)"""
        # For dimension tables, use REPLACE INTO or INSERT ... ON DUPLICATE KEY UPDATE
        
        # Get primary key column
        pk_col = self._get_primary_key_column(table_name)
        
        # Clear existing data for the keys we're about to insert
        if pk_col and pk_col in df.columns:
            keys_to_delete = df[pk_col].unique().tolist()
            keys_str = ','.join([f"'{str(k)}'" for k in keys_to_delete])  # <-- bọc giá trị trong dấu nháy

            with self.engine.connect() as conn:
                delete_sql = f"DELETE FROM `{self.schema_name}`.{table_name} WHERE {pk_col} IN ({keys_str})"
                conn.execute(text(delete_sql))
        
        # Insert new data
        self._insert_data(table_name, df, batch_size)
    
    def _insert_data(self, table_name: str, df: pd.DataFrame, batch_size: int):
        """Insert data in batches"""
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i + batch_size]
            batch_df.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.schema_name,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.debug(f"Inserted batch {i//batch_size + 1} ({len(batch_df)} rows) into {table_name}")
    
    def _get_primary_key_column(self, table_name: str) -> Optional[str]:
        """Get primary key column name for a table"""
        pk_mapping = {
            'dim_time': 'time_key',
            'dim_location': 'location_key',
            'dim_property': 'property_key',
            'dim_legal': 'legal_key',
            'dim_structure': 'structure_key',
            'dim_amenity': 'amenity_key',
            'dim_contact': 'contact_key',
            'dim_source': 'source_key',
            'fact_price_analysis': 'price_fact_id',
            'fact_area_analysis': 'area_fact_id',
            'fact_amenities_analysis': 'amenity_fact_id',
            'fact_listing_analysis': 'listing_fact_id'
        }
        return pk_mapping.get(table_name)
    
    def truncate_table(self, table_name: str):
        """Truncate a specific table"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE `{self.schema_name}`.{table_name}"))
                # conn.commit()
            logger.info(f"Truncated table {table_name}")
        except Exception as e:
            logger.error(f"Failed to truncate table {table_name}: {str(e)}")
            raise
    
    def truncate_all_tables(self):
        """Truncate all tables (fact tables first, then dimensions)"""
        fact_tables = [
            'fact_listing_analysis', 'fact_amenities_analysis',
            'fact_area_analysis', 'fact_price_analysis'
        ]
        
        dimension_tables = [
            'dim_source', 'dim_contact', 'dim_amenity', 'dim_structure',
            'dim_legal', 'dim_property', 'dim_location', 'dim_time'
        ]
        
        # Truncate fact tables first
        for table in fact_tables:
            self.truncate_table(table)
        
        # Then dimension tables
        for table in dimension_tables:
            self.truncate_table(table)
    
    def get_table_counts(self) -> Dict[str, int]:
        """Get record counts for all tables"""
        tables = [
            'dim_time', 'dim_location', 'dim_property', 'dim_legal',
            'dim_structure', 'dim_amenity', 'dim_contact', 'dim_source',
            'fact_price_analysis', 'fact_area_analysis',
            'fact_amenities_analysis', 'fact_listing_analysis'
        ]
        
        counts = {}
        try:
            with self.engine.connect() as conn:
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM `{self.schema_name}`.{table}"))
                    counts[table] = result.scalar()
            return counts
        except Exception as e:
            logger.error(f"Failed to get table counts: {str(e)}")
            raise
    
    def close_connection(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")

    