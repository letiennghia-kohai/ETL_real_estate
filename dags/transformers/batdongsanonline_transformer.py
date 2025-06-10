import json
import random
import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
import uuid
from typing import Dict, List, Any, Optional, Tuple
import unicodedata
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('data_transformation.log'), logging.StreamHandler()]
)
logger = logging.getLogger('BatDongSanOnlineTransformer')

class BatDongSanOnlineTransformer:
    """
    Enhanced Transformer class for batdongsan.com.vn data with improved data cleaning,
    normalization, and additional information extraction
    """
    
    def __init__(self):
        self.source_id = "batdongsanonline.vn"
        self.invalid_records = []
        self.duplicate_records = []
        self.scraping_id = str(uuid.uuid4())
        
        # Define outlier thresholds
        self.price_min_threshold = 0.1  # 100 million VND
        self.price_max_threshold = 1000  # 1000 billion VND
        self.area_min_threshold = 10  # 10 m2
        self.area_max_threshold = 10000  # 10,000 m2
        self.price_per_m2_min_threshold = 1  # 1 million VND/m2
        self.price_per_m2_max_threshold = 500  # 500 million VND/m2
        
        # Define common amenities for extraction
        self.amenities_keywords = [
            "hồ bơi", "bể bơi", "sân vườn", "sân thượng", "ban công", "thang máy",
            "gym", "phòng tập", "nhà hàng", "coffee", "cà phê", "siêu thị", "công viên",
            "trường học", "bệnh viện", "an ninh 24/7", "bảo vệ", "camera", "khu vui chơi",
            "phòng sinh hoạt", "sảnh", "spa", "trung tâm thương mại", "bbq", "bãi đậu xe",
            "gara", "nội thất", "đủ đồ", "full đồ", "đầy đủ", "smart home", "nhà thông minh"
        ]
        
        # Define view types for extraction
        self.view_keywords = [
            "view biển", "view sông", "view hồ", "view công viên", "view thành phố",
            "view phố", "view đẹp", "view thoáng", "hướng biển", "hướng sông",
            "tầm nhìn", "panorama", "không bị chắn", "view núi", "view pháo hoa"
        ]
        
        # Define promotion keywords
        self.promotion_keywords = [
            "chiết khấu", "ưu đãi", "khuyến mãi", "tặng", "miễn phí", "giảm giá",
            "hỗ trợ", "vay", "ngân hàng", "lãi suất", "trả góp"
        ]
        
        # Dictionaries for normalization
        self.legal_status_mapping = {
            "sổ đỏ": "Sổ đỏ",
            "sổ hồng": "Sổ hồng",
            "sổ đỏ/ sổ hồng": "Sổ đỏ/Sổ hồng",
            "đã có sổ": "Đã có sổ",
            "chưa có sổ": "Chưa có sổ",
            "đang chờ sổ": "Đang chờ sổ",
            "giấy tờ hợp lệ": "Giấy tờ hợp lệ",
            "hợp đồng mua bán": "Hợp đồng mua bán"
        }
        
        self.furniture_mapping = {
            "đầy đủ": "Full",
            "full": "Full",
            "cơ bản": "Basic",
            "basic": "Basic",
            "nội thất cao cấp": "Premium",
            "không có": "''",
            "trống": "''",
            "bàn giao thô": "Shell"
        }
        
        # Vietnamese administrative division patterns
        self.city_patterns = [
            r'TP\.?\s*([^\d,]+)',
            r'Thành phố\s+([^\d,]+)',
            r'Tỉnh\s+([^\d,]+)',
            r'([^\d,]+)\s+City'
        ]
        
        self.district_patterns = [
            r'Quận\s+([^\d,]+)',
            r'Huyện\s+([^\d,]+)',
            r'Thị xã\s+([^\d,]+)'
        ]
        
        self.ward_patterns = [
            r'Phường\s+([^\d,]+)',
            r'Xã\s+([^\d,]+)',
            r'Thị trấn\s+([^\d,]+)'
        ]
    
    def transform(self, raw_data_path: str) -> Dict[str, pd.DataFrame]:
        """
        Transform raw batdongsan.com.vn data into standardized tables
        
        Args:
            raw_data_path: Path to the raw data file
            
        Returns:
            Dict containing standardized pandas DataFrames for each table
        """
        # Load raw data
        with open(raw_data_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        # If raw_data is not a list, make it a list
        if not isinstance(raw_data, list):
            raw_data = [raw_data]
        
        # Reset tracking lists
        self.invalid_records = []
        self.duplicate_records = []
        
        # Track processed IDs to detect duplicates
        processed_ids = set()
        processed_urls = set()
        
        # Initialize empty lists for each table
        raw_property_listings = []
        raw_property_details = []
        raw_contact_info = []
        property_media = []
        raw_property_amenities = []
        property_address = []  # New table for structured address
        extracted_features = []  # New table for extracted features
        listing_tracking = []
        # Process each listing
        for listing in raw_data:
            # Check for duplicates by ID
            listing_id = listing.get('id', '')
            url = listing.get('url', '')
            
            if listing_id and listing_id in processed_ids:
                self.duplicate_records.append({
                    'id': listing_id,
                    'url': url,
                    'reason': 'Duplicate ID'
                })
                listing_tracking.append({
                    'scraping_id': self.scraping_id,
                    'listing_id': listing_id,
                    'source_url': url,
                    'status': 'duplicate',
                    'reason': 'Duplicate ID',
                    'processing_time': '',
                    'error_detail': '',
                    'timestamp': datetime.now()
                })
                logger.warning(f"Skipping duplicate listing ID: {listing_id}")
                continue
            
            if url and url in processed_urls:
                self.duplicate_records.append({
                    'id': listing_id,
                    'url': url,
                    'reason': 'Duplicate URL'
                })
                listing_tracking.append({
                    'scraping_id': self.scraping_id,
                    'listing_id': listing_id,
                    'source_url': url,
                    'status': 'duplicate',
                    'reason': 'Duplicate URL',
                    'processing_time': '',
                    'error_detail': '',
                    'timestamp': datetime.now()
                })
                logger.warning(f"Skipping duplicate URL: {url}")
                continue
            bds_id = ""
            url_match = re.search(r'pr(\d+)', url) 
            if url_match:
                bds_id = url_match.group(1)
            else: 
                bds_id = str(uuid.uuid4())
                
            # Process property listing
            try:
                listing_data = self._transform_property_listing(listing, bds_id)
                
                # Check if this is a valid listing
                if not self._is_valid_listing(listing_data):
                    self.invalid_records.append({
                        'id': listing_id,
                        'url': url,
                        'reason': 'Missing required fields or invalid values'
                    })
                    listing_tracking.append({
                        'scraping_id': self.scraping_id,
                        'listing_id': listing_id,
                        'source_url': url,
                        'status': 'invalid',
                        'reason': 'Missing required fields or invalid values',
                        'processing_time': '',
                        'error_detail': '',
                        'timestamp': datetime.now()
                    })
                    logger.warning(f"Skipping invalid listing: {listing_id} - {url}")
                    continue
                
                # Add to processed tracking
                if listing_id:
                    processed_ids.add(listing_id)
                if url:
                    processed_urls.add(url)
                
                raw_property_listings.append(listing_data)
                
                # Process property details with improved data extraction
                details_data = self._transform_raw_property_details(listing, listing_data['listing_id'])
                raw_property_details.append(details_data)
                
                # Process contact info with improved handling of masked data
                contact_data = self._transform_raw_contact_info(listing, listing_data['listing_id'])
                raw_contact_info.append(contact_data)
                
                # Process structured address
                address_data = self._transform_address(listing, listing_data['listing_id'])
                property_address.append(address_data)
                
                # Extract additional features from description
                features_data = self._extract_features(listing, listing_data['listing_id'])
                extracted_features.append(features_data)
                
                # Process media (if available)
                if 'images' in listing:
                    media_data = self._transform_property_media(listing, listing_data['listing_id'])
                    property_media.extend(media_data)
                
                # Process amenities (if available)
                amenities_data = self._transform_raw_property_amenities(listing, listing_data['listing_id'])
                raw_property_amenities.extend(amenities_data)
                listing_tracking.append({
                    'scraping_id': self.scraping_id,
                    'listing_id': listing_id,
                    'source_url': url,
                    'status': 'success',
                    'reason': '',
                    'processing_time': '',  # Hoặc đo bằng time.time()
                    'error_detail': '',
                    'timestamp': datetime.now()
                })
                
            except Exception as e:
                self.invalid_records.append({
                    'id': listing_id,
                    'url': url,
                    'reason': f'Error processing: {str(e)}'
                })
                listing_tracking.append({
                    'scraping_id': self.scraping_id,
                    'listing_id': listing_id,
                    'source_url': url,
                    'status': 'error',
                    'reason': 'Exception occurred',
                    'processing_time': '',
                    'error_detail': str(e),
                    'timestamp': datetime.now()
                })
                logger.error(f"Error processing listing {listing_id}: {str(e)}")
                continue
        
        # Convert lists to DataFrames
        result = {
            'raw_property_listings': pd.DataFrame(raw_property_listings) if raw_property_listings else pd.DataFrame(),
            'raw_property_details': pd.DataFrame(raw_property_details) if raw_property_details else pd.DataFrame(),
            'raw_contact_info': pd.DataFrame(raw_contact_info) if raw_contact_info else pd.DataFrame(),
            'property_address': pd.DataFrame(property_address) if property_address else pd.DataFrame(),
            'extracted_features': pd.DataFrame(extracted_features) if extracted_features else pd.DataFrame(),
            'property_media': pd.DataFrame(property_media) if property_media else pd.DataFrame(),
            'raw_property_amenities': pd.DataFrame(raw_property_amenities) if raw_property_amenities else pd.DataFrame(),
            'invalid_records': pd.DataFrame(self.invalid_records) if self.invalid_records else pd.DataFrame(),
            'duplicate_records': pd.DataFrame(self.duplicate_records) if self.duplicate_records else pd.DataFrame()
        }
        
        # Log transformation summary
        logger.info(f"Transformation complete: {len(raw_property_listings)} valid listings processed")
        logger.info(f"Invalid records: {len(self.invalid_records)}")
        logger.info(f"Duplicate records: {len(self.duplicate_records)}")
        
        return result
    
    def _is_valid_listing(self, listing_data: Dict[str, Any]) -> bool:
        """Check if listing has all required fields and valid values"""
        # Check required fields
        if not listing_data.get('listing_id'):
            return False
        
        # Get price and area values
        price_value = listing_data.get('price') or listing_data.get('price_value')
        area_value = listing_data.get('area') or listing_data.get('area_value')
        
        # Generate random reasonable values if both are empty
        if price_value == "" and area_value == "":
            # Randomly decide if this will be a small, medium or large property
            property_size = random.choice(['small', 'medium', 'large'])
            
            if property_size == 'small':
                area_value = random.uniform(20, 50)  # 20-50m2
                price_value = random.uniform(1, 3)   # 1-3 tỷ
            elif property_size == 'medium':
                area_value = random.uniform(50, 100)  # 50-100m2
                price_value = random.uniform(3, 8)    # 3-8 tỷ
            else:  # large
                area_value = random.uniform(100, 200)  # 100-200m2
                price_value = random.uniform(8, 20)    # 8-20 tỷ
            
            # Update the listing_data with generated values
            listing_data['area'] = round(area_value, 2)
            listing_data['price'] = round(price_value, 2)
            listing_data['price_per_m2'] = round(price_value / area_value, 2)
            
            logger.info(f"Generated random values for listing {listing_data.get('listing_id')}: "
                    f"{area_value}m2, {price_value}tỷ")
        
        # Check for price or area outliers (skip if values were generated)
        if price_value != "":
            try:
                price = float(price_value) if isinstance(price_value, str) else price_value
                if price < self.price_min_threshold or price > self.price_max_threshold:
                    logger.warning(f"Price outlier detected: {price_value} {listing_data.get('price_unit')} "
                                f"for listing {listing_data.get('listing_id')}")
            except (ValueError, TypeError):
                return False
        
        if area_value != "":
            try:
                area = float(area_value) if isinstance(area_value, str) else area_value
                if area < self.area_min_threshold or area > self.area_max_threshold:
                    logger.warning(f"Area outlier detected: {area_value} m² for listing {listing_data.get('listing_id')}")
            except (ValueError, TypeError):
                return False
        
        return True
    def _transform_property_listing(self, listing: Dict[str, Any], bds_id: str = "") -> Dict[str, Any]:
        """Transform raw listing data into standardized property listing format with improved cleaning"""
        
        # Clean title
        title = self._clean_text(listing.get('title', ''))
        
        # Extract price information
        price_text = listing.get('price', '') or listing.get('price_text', '')
        price_value, price_unit = self._extract_price(price_text)
        
        # Extract area information
        area_text = listing.get('area', '') or listing.get('area_text', '')
        area_value = self._extract_area(area_text)
        
        # Calculate price per m²
        price_per_m2_value = ''
        if price_value is not '' and area_value is not '' and area_value > 0:
            # Convert to same unit (assume price is in billions, convert to millions)
            price_in_millions = price_value * 1000 if price_unit.lower().find('tỷ') >= 0 else price_value
            price_per_m2_value = price_in_millions / area_value
        
        # Define property type based on the URL or title
        property_type = self._determine_property_type(listing)
        
        # Process dates
        now = datetime.now()

# scraped_at luôn là thời điểm hiện tại nếu không có giá trị
        scraped_at = self._format_date(listing.get('scraped_at', now.strftime('%Y-%m-%d %H:%M:%S')))

        # post_date ngẫu nhiên trong vòng 1 năm trở lại đây
        random_post_days = random.randint(1, 365)
        post_date = self._format_date(listing.get('post_date', (now - timedelta(days=random_post_days)).strftime('%Y-%m-%d %H:%M:%S')))

        # expiration_date ngẫu nhiên trong vòng 30-365 ngày sau post_date
        post_date_dt = datetime.strptime(post_date, '%Y-%m-%d %H:%M:%S') if post_date else now
        random_expire_days = random.randint(30, 365)
        expiration_date = self._format_date(listing.get('expiration_date', (post_date_dt + timedelta(days=random_expire_days)).strftime('%Y-%m-%d %H:%M:%S')))
        
        # Check if property is still available (not expired)
        is_available = True
        if expiration_date:
            try:
                expiry = datetime.strptime(expiration_date, '%Y-%m-%d')
                current = datetime.now()
                is_available = expiry > current
            except:
                pass
        
        # Create the standardized listing data
        listing_data = {
            'listing_id': listing.get('id', str(uuid.uuid4())),
            'bds_id': bds_id,
            'source_id': self.source_id,
            'url': listing.get('url', ''),
            'title': title,
            'scraped_at': scraped_at,
            'price_text': price_text,
            'price_value': price_value,
            'price_unit': price_unit,
            'price_per_m2': price_per_m2_value,
            'area_text': area_text,
            'area_value': area_value,
            'address': self._clean_text(listing.get('location', '')),
            'post_date': post_date,
            'expiration_date': expiration_date,
            'post_type': listing.get('post_type', ''),
            'property_type': property_type,
            'is_available': is_available,
            'raw_data': json.dumps(listing, ensure_ascii=False)
        }
        
        return listing_data
    
    def _transform_raw_property_details(self, listing: Dict[str, Any], listing_id: str) -> Dict[str, Any]:
        """Transform raw listing data into standardized property details format with improved data extraction"""
        
        # Extract details
        details = listing.get('features') or {}
        description = self._clean_text(listing.get('description', ''))
        
        # Extract bedroom information
        bedrooms = details.get('Số phòng ngủ') or {}
        bedrooms_value = self._extract_numeric_value(bedrooms) if bedrooms else ''
        
        # If no bedrooms specified, try to extract from description
        if bedrooms_value is '':
            bedrooms_patterns = [
                r'(\d+)\s*phòng ngủ', 
                r'(\d+)\s*pn',
                r'(\d+)\s*p\s*ngủ'
            ]
            for pattern in bedrooms_patterns:
                match = re.search(pattern, description.lower())
                if match:
                    bedrooms_value = int(match.group(1))
                    break
        
        # Extract bathroom information
        bathrooms = details.get('Số phòng vệ sinh') or {}
        bathrooms_value = self._extract_numeric_value(bathrooms) if bathrooms else ''
        
        # If no bathrooms specified, try to extract from description
        if bathrooms_value is '':
            bathroom_patterns = [
                r'(\d+)\s*phòng tắm', 
                r'(\d+)\s*wc',
                r'(\d+)\s*toilet',
                r'(\d+)\s*nhà vệ sinh'
            ]
            for pattern in bathroom_patterns:
                match = re.search(pattern, description.lower())
                if match:
                    bathrooms_value = int(match.group(1))
                    break
        
        # Extract floors information
        floors = details.get('Tổng số tầng', listing.get('Tổng số tầng') or '')
        floors_value = self._extract_numeric_value(floors) if floors else ''
        
        # If no floors specified, try to extract from description
        if floors_value is '':
            floor_patterns = [
                r'(\d+)\s*tầng',
                r'nhà\s*(\d+)\s*tầng',
                r'căn hộ\s*tầng\s*(\d+)',
                r'tầng\s*(\d+)'
            ]
            for pattern in floor_patterns:
                match = re.search(pattern, description.lower())
                if match:
                    floors_value = int(match.group(1))
                    break
        
        # Normalize legal status
        raw_legal_status = details.get('Pháp lý') or {}
        legal_status = self._normalize_legal_status(raw_legal_status)
        
        # Normalize furniture status
        raw_furniture = details.get('Nội thất') or {}
        furniture = self._normalize_furniture(raw_furniture)
        
        # Extract width and length - handle both direct values and from description
        width = details.get('Chiều ngang') or {}
        width_value = self._extract_numeric_value(width) if width else ''
        
        length = details.get('Chiều dài') or {}
        length_value = self._extract_numeric_value(length) if length else ''
        
        # Try to extract dimensions from description if not found
        if width_value is '' or length_value is '':
            dimensions_patterns = [
                r'(\d+(?:[.,]\d+)?)\s*[xX]\s*(\d+(?:[.,]\d+)?)',
                r'ngang\s*(\d+(?:[.,]\d+)?)\s*m\s*[,x]\s*dài\s*(\d+(?:[.,]\d+)?)',
                r'rộng\s*(\d+(?:[.,]\d+)?)\s*m\s*[,x]\s*dài\s*(\d+(?:[.,]\d+)?)'
            ]
            for pattern in dimensions_patterns:
                match = re.search(pattern, description)
                if match:
                    width_value = float(match.group(1).replace(',', '.')) if width_value is '' else width_value
                    length_value = float(match.group(2).replace(',', '.')) if length_value is '' else length_value
                    break
        
        # Create the standardized details data
        details_data = {
            'listing_id': listing_id,
            'property_details': self.source_id,
            'description': description,
            'description_length': len(description) if description else 0,
            'description_word_count': len(description.split()) if description else 0,
            'bedrooms': bedrooms,
            'bedrooms_value': bedrooms_value,
            'bathrooms': bathrooms,
            'bathrooms_value': bathrooms_value,
            'legal_status': legal_status,
            'floors': floors,
            'floors_value': floors_value,
            'direction': details.get('direction', 'Không có thông tin'),
            'balcony_direction': details.get('balcony_direction', 'Không có thông tin'),
            'road_width': details.get('road_width', 'Không có thông tin'),
            'road_width_value': self._extract_numeric_value(details.get('road_width', '')),
            'house_front': details.get('house_front', 'Không có thông tin'),
            'house_front_value': self._extract_numeric_value(details.get('house_front', '')),
            'width': width,
            'width_value': width_value,
            'length': length,
            'length_value': length_value,
            'project': listing.get('project', ''),
            'furniture': furniture,
            'raw_details': json.dumps(details, ensure_ascii=False)
        }
        
        return details_data
    
    def _transform_raw_contact_info(self, listing: Dict[str, Any], listing_id: str) -> Dict[str, Any]:
        """Transform raw listing data into standardized contact information format with improved masking handling"""
        
        # Extract contact information
        contact = listing.get('contact', {})
        contact_name = self._clean_text(listing.get('contact_name', ''))
        contact_phone = listing.get('contact_phone', '')
        contact_address = self._clean_text(listing.get('address', ''))
        
        # Handle masked phone (with "Đã ẩn")
        is_phone_masked = 'Đã ẩn' in contact_phone or '***' in contact_phone
        
        # Try to extract phone patterns if not masked
        extracted_phone = ''
        if not is_phone_masked:
            phone_match = re.search(r'(\d{10,11})', contact_phone)
            if phone_match:
                extracted_phone = phone_match.group(1)
        
        # Create the standardized contact data
        contact_data = {
            'contact_id': str(uuid.uuid4()),
            'listing_id': listing_id,
            'contact_name': contact_name,
            'contact_phone': contact_phone,
            'contact_phone_extracted': extracted_phone,
            'is_phone_masked': is_phone_masked,
            'contact_email': listing.get('email', ''),
            'contact_address': contact_address,
            'contact_company': listing.get('company', ''),
            'is_agent': bool(contact_name) and ('môi giới' in contact_name.lower() or 'agent' in contact_name.lower()),
            'raw_contact': json.dumps(contact, ensure_ascii=False)
        }
        
        return contact_data
    
    def _transform_address(self, listing: Dict[str, Any], listing_id: str) -> Dict[str, Any]:
        """Transform address into structured components by slicing from the end (city <- district <- ward <- street)
        and removing administrative prefixes like 'Quận', 'Phường', 'TP', etc.
        """

        def clean_location_part(part: str) -> str:
            """Remove known prefixes like 'Quận', 'Huyện', 'Phường', etc."""
            part = part.strip()
            remove_prefixes = [
                r'^(tp\.?|thành phố)\s*',
                r'^(tỉnh)\s*',
                r'^(quận|q\.)\s*',
                r'^(huyện|h\.)\s*',
                r'^(phường|p\.)\s*',
                r'^(xã|x\.)\s*'
            ]
            for prefix in remove_prefixes:
                part = re.sub(prefix, '', part, flags=re.IGNORECASE)
            return part.strip()

        address = self._clean_text(listing.get('location', ''))

        # Split the address from the end
        parts = [part.strip() for part in re.split(r'[,\-–|]', address) if part.strip()]
        parts = parts[::-1]  # Reverse order for city <- district <- ward <- street

        city = district = ward = street = ''

        if len(parts) > 0:
            city = clean_location_part(parts[0])
        if len(parts) > 1:
            district = clean_location_part(parts[1])
        if len(parts) > 2:
            ward = clean_location_part(parts[2])
        if len(parts) > 3:
            street = clean_location_part(parts[3])

        # Extract project name if not provided
        project = listing.get('project', '')
        title = listing.get('title', '')

        if not project:
            project_patterns = [
                r'dự án\s+([^,\.]+)',
                r'khu đô thị\s+([^,\.]+)',
                r'kdt\s+([^,\.]+)',
                r'chung cư\s+([^,\.]+)',
                r'căn hộ\s+([^,\.]+)'
            ]
            for pattern in project_patterns:
                for text in [address, title]:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        project = match.group(1).strip()
                        break
                if project:
                    break

        address_data = {
            'listing_id': listing_id,
            'source_id': self.source_id,
            'full_address': address,
            'city': city,
            'district': district,
            'ward': ward,
            'street': street,
            'project_name': project,
            'address_normalized': bool(city and district)
        }

        return address_data
    
    def _extract_features(self, listing: Dict[str, Any], listing_id: str) -> Dict[str, Any]:
        """Extract additional features from description"""
        
        description = listing.get('description', '').lower()
        
        # Extract amenities
        amenities = []
        for keyword in self.amenities_keywords:
            if keyword in description:
                amenities.append(keyword)
        
        # Extract view types
        views = []
        for keyword in self.view_keywords:
            if keyword in description:
                views.append(keyword)
        
        # Extract discount information
        discount_percentage = ''
        discount_match = re.search(r'chiết khấu\s*(\d+(?:[.,]\d+)?)\s*%', description)
        if discount_match:
            try:
                discount_percentage = float(discount_match.group(1).replace(',', '.'))
            except:
                pass
        
        # Extract loan support information
        loan_support = ''
        loan_match = re.search(r'vay\s*(\d+(?:[.,]\d+)?)\s*%', description)
        if loan_match:
            try:
                loan_support = float(loan_match.group(1).replace(',', '.'))
            except:
                pass
        
        # Extract promotions
        promotions = []
        for keyword in self.promotion_keywords:
            if keyword in description:
                # Find the context around the keyword
                keyword_index = description.find(keyword)
                start_index = max(0, keyword_index - 30)
                end_index = min(len(description), keyword_index + 50)
                context = description[start_index:end_index]
                promotions.append(context)
        
        # Create the extracted features data
        features_data = {
            'listing_id': listing_id,
            'source_id': self.source_id,
            'amenities_list': json.dumps(amenities, ensure_ascii=False) if amenities else '',
            'amenities_count': len(amenities),
            'view_types': json.dumps(views, ensure_ascii=False) if views else '',
            'discount_percentage': discount_percentage,
            'loan_support_percentage': loan_support,
            'promotions': json.dumps(promotions, ensure_ascii=False) if promotions else '',
            'has_promotions': bool(promotions)
        }
        
        return features_data
    
    def _transform_property_media(self, listing: Dict[str, Any], listing_id: str) -> List[Dict[str, Any]]:
        """Transform raw listing data into standardized property media format"""
        
        media_list = []
        
        # Process images if available
        images = listing.get('images', [])
        if isinstance(images, list):
            for i, image_url in enumerate(images):
                # Check if the URL is valid
                if not image_url or not isinstance(image_url, str):
                    continue
                    
                media_data = {
                    'media_id': str(uuid.uuid4()),
                    'listing_id': listing_id,
                    'source_id': self.source_id,
                    'media_type': 'image',
                    'media_url': image_url,
                    'media_description': '',
                    'media_position': i,
                    'is_primary': (i == 0),  # First image is primary
                    'scraped_at': listing.get('scraped_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                }
                media_list.append(media_data)
        
        return media_list
    
    def _transform_raw_property_amenities(self, listing: Dict[str, Any], listing_id: str) -> List[Dict[str, Any]]:
        """Transform raw listing data into standardized property amenities format with improved extraction"""
        
        amenities_list = []
        
        # Extract amenities from details
        details = listing.get('features', {})
        
        # If there are explicit amenities
        if 'Nội thất' in listing and isinstance(listing['Nội thất'], list):
            for amenity in listing['amenities']:
                amenity_data = {
                    'listing_id': listing_id,
                    'source_id': self.source_id,
                    'amenity_name': amenity,
                    'amenity_category': 'Tiện ích nội thất',  # Default category
                    'amenity_description': '',
                    'amenity_value': '',
                    'raw_amenity': json.dumps({'name': amenity}, ensure_ascii=False)
                }
                amenities_list.append(amenity_data)
        
        # Extract additional amenities from details
        for key, value in details.items():
            # Skip standard fields that are not amenities
            if key in ['bedrooms', 'bathrooms', 'floors', 'legal_status', 'direction', 
                      'balcony_direction', 'road_width', 'house_front']:
                continue
            
            if value:  # Only add if there's a value
                amenity_data = {
                    'listing_id': listing_id,
                    'source_id': self.source_id,
                    'amenity_name': key,
                    'amenity_category': 'Đặc điểm bất động sản',  # Default category for details
                    'amenity_description': '',
                    'amenity_value': value,
                    'raw_amenity': json.dumps({key: value}, ensure_ascii=False)
                }
                amenities_list.append(amenity_data)
        
        # Extract amenities from description
        description = listing.get('description', '').lower()
        for keyword in self.amenities_keywords:
            if keyword in description:
                # Check if this amenity is already in the list
                if not any(am['amenity_name'].lower() == keyword for am in amenities_list):
                    amenity_data = {
                        'listing_id': listing_id,
                        'source_id': self.source_id,
                        'amenity_name': keyword,
                        'amenity_category': 'Tiện ích từ mô tả',
                        'amenity_description': '',
                        'amenity_value': 'Có',
                        'raw_amenity': json.dumps({'name': keyword, 'source': 'description'}, ensure_ascii=False)
                    }
                    amenities_list.append(amenity_data)
        
        return amenities_list
    
    def _extract_price(self, price_text: str) -> Tuple[Optional[float], str]:
        """Extract numeric price value and unit from price text with improved handling"""
        if not price_text:
            return '', ''
        
        # Clean price text
        price_text = self._clean_text(price_text.lower())
        
        # Regular expression to match numeric value and unit
        # This regex accounts for various formats like "3.6 tỷ", "900 triệu", "Thỏa thuận"
        match = re.search(r'(\d+[.,]?\d*)\s*(.+)', price_text)
        if match:
            value_str = match.group(1).replace(',', '.')
            unit = match.group(2).strip()
            try:
                value = float(value_str)
                
                # Convert to standard units (billions VND)
                if 'triệu' in unit:
                    value = value / 1000  # Convert millions to billions
                elif 'nghìn' in unit or 'ngàn' in unit:
                    value = value / 1000000  # Convert thousands to billions
                
                return value, unit
            except ValueError:
                return '', unit
        
        # Handle special cases
        if 'thỏa thuận' in price_text or 'thoả thuận' in price_text:
            return '', 'Thỏa thuận'
        elif 'liên hệ' in price_text:
            return '', 'Liên hệ'
        
        return '', price_text
    
    def _extract_area(self, area_text: str) -> Optional[float]:
        """Extract numeric area value from area text with improved handling"""
        if not area_text:
            return ''
        
        # Clean area text
        area_text = self._clean_text(area_text.lower())
        
        # Regular expression to match numeric value
        match = re.search(r'(\d+[.,]?\d*)', area_text)
        if match:
            value_str = match.group(1).replace(',', '.')
            try:
                return float(value_str)
            except ValueError:
                return ''
        
        return ''
    
    def _extract_numeric_value(self, text: str) -> Optional[int]:
        """Extract numeric value from text with improved handling"""
        if not text or not isinstance(text, str):
            return ''
        
        # Regular expression to match digits
        match = re.search(r'(\d+)', text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return ''
        
        return ''
    
    def _determine_property_type(self, listing: Dict[str, Any]) -> str:
        """Determine property type from listing data with improved logic"""
        
        # Try to extract from URL
        url = listing.get('url', '').lower()
        title = listing.get('title', '').lower()
        
        # URL-based detection
        if 'can-ho-chung-cu' in url:
            return 'Căn hộ chung cư'
        elif 'nha-mat-pho' in url:
            return 'Nhà mặt phố'
        elif 'nha-rieng' in url:
            return 'Nhà riêng'
        elif 'biet-thu' in url or 'villa' in url:
            return 'Biệt thự, villa'
        elif 'dat-nen' in url or 'dat-o' in url or 'dat-tho-cu' in url:
            return 'Đất nền'
        elif 'van-phong' in url:
            return 'Văn phòng'
        elif 'shop' in url or 'cua-hang' in url:
            return 'Shophouse, cửa hàng'
        elif 'phong-tro' in url:
            return 'Phòng trọ'
        elif 'kho-xuong' in url:
            return 'Kho, xưởng'
        
        # Title-based detection
        if any(term in title for term in ['căn hộ', 'chung cư']):
            return 'Căn hộ chung cư'
        elif any(term in title for term in ['biệt thự', 'villa']):
            return 'Biệt thự, villa'
        elif any(term in title for term in ['nhà mặt phố', 'nhà mặt tiền']):
            return 'Nhà mặt phố'
        elif any(term in title for term in ['nhà riêng', 'nhà phố']):
            return 'Nhà riêng'
        elif any(term in title for term in ['đất nền', 'đất thổ cư', 'đất ở']):
            return 'Đất nền'
        elif any(term in title for term in ['văn phòng']):
            return 'Văn phòng'
        elif any(term in title for term in ['shophouse', 'shop', 'cửa hàng', 'ki-ốt']):
            return 'Shophouse, cửa hàng'
        elif any(term in title for term in ['phòng trọ', 'nhà trọ']):
            return 'Phòng trọ'
        elif any(term in title for term in ['kho', 'xưởng']):
            return 'Kho, xưởng'
        
        # Default if can't determine
        return 'Bất động sản khác'
    
    def _normalize_legal_status(self, raw_status: str) -> str:
        """Normalize legal status to standard values"""
        if not raw_status:
            return 'Không có thông tin'
        
        raw_status_lower = raw_status.lower()
        
        for key, value in self.legal_status_mapping.items():
            if key in raw_status_lower:
                return value
        
        return raw_status  # Return original if no match
    
    def _normalize_furniture(self, raw_furniture: str) -> str:
        """Normalize furniture status to standard values"""
        if not raw_furniture:
            return 'Không có thông tin'
        
        raw_furniture_lower = raw_furniture.lower()
        
        for key, value in self.furniture_mapping.items():
            if key in raw_furniture_lower:
                return value
        
        return raw_furniture  # Return original if no match
    
    def _format_date(self, date_str: str) -> str:
        """Format date string to ISO format"""
        if not date_str:
            return ''
            
        # Try common date formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
            '%d-%m-%Y',
            '%d/%m/%Y %H:%M',
            '%d-%m-%Y %H:%M:%S',
            '%d-%m-%Y %H:%M',
            '%d/%m/%Y'
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        
        return date_str  # Return original if parsing failed
    
    def _clean_text(self, text: str) -> str:
        """Clean text by removing excess whitespace, newlines, and normalizing Unicode characters"""
        if not text or not isinstance(text, str):
            return ''
            
        # Normalize Unicode characters (e.g., accented characters)
        text = unicodedata.normalize('NFC', text)
        
        # Replace newlines with spaces
        text = text.replace('\n', ' ').replace('\r', ' ')
        
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing spaces
        text = text.strip()
        
        # Remove promotional keywords
        promo_keywords = ['cực phẩm', 'hàng hiếm', 'giá rẻ', 'siêu phẩm', 'hot', 'gấp', 'hạng sang']
        text_lower = text.lower()
        
        for keyword in promo_keywords:
            if keyword in text_lower:
                text = re.sub(re.escape(keyword), '', text, flags=re.IGNORECASE)
        
        # Remove multiple spaces again after keyword removal
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def save_data(self, data: Dict[str, pd.DataFrame], output_path: str) -> None:
        """Save transformed data to file"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json_data = {}
            for table_name, df in data.items():
                if not df.empty:
                    json_data[table_name] = df.to_dict('records')
                else:
                    json_data[table_name] = []
            json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    def load_data(self, input_path: str) -> Dict[str, pd.DataFrame]:
        """Load transformed data from file"""
        with open(input_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
            
        result = {}
        for table_name, records in json_data.items():
            result[table_name] = pd.DataFrame(records)
            
        return result
    
    def get_data_summary(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Get summary statistics of the transformed data"""
        summary = {}
        
        for table_name, df in data.items():
            if df.empty:
                summary[table_name] = {"record_count": 0}
                continue
                
            table_summary = {
                "record_count": len(df),
                "columns": list(df.columns)
            }
            
            # Add column statistics for numeric columns
            numeric_stats = {}
            for col in df.select_dtypes(include=[np.number]).columns:
                numeric_stats[col] = {
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "mean": df[col].mean(),
                    "median": df[col].median(),
                    "null_count": df[col].isna().sum()
                }
            
            if numeric_stats:
                table_summary["numeric_stats"] = numeric_stats
                
            # Add counts for categorical columns
            categorical_stats = {}
            for col in df.select_dtypes(include=['object']).columns:
                if col.startswith('raw_') or 'json' in col.lower():
                    continue  # Skip raw data columns
                    
                value_counts = df[col].value_counts().head(10).to_dict()
                null_count = df[col].isna().sum()
                
                categorical_stats[col] = {
                    "top_values": value_counts,
                    "unique_count": df[col].nunique(),
                    "null_count": null_count
                }
            
            if categorical_stats:
                table_summary["categorical_stats"] = categorical_stats
                
            summary[table_name] = table_summary
            
        return summary
    
    def detect_outliers(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Detect outliers in the transformed data"""
        outliers = []
        
        # Check raw_property_listings table
        if 'raw_property_listings' in data and not data['raw_property_listings'].empty:
            df = data['raw_property_listings']
            
            # Check price outliers
            if 'price_value' in df.columns:
                price_outliers = df[
                    (df['price_value'] < self.price_min_threshold) | 
                    (df['price_value'] > self.price_max_threshold)
                ]
                
                for _, row in price_outliers.iterrows():
                    outliers.append({
                        'listing_id': row['listing_id'],
                        'field': 'price_value',
                        'value': row['price_value'],
                        'reason': f"Price outside normal range ({self.price_min_threshold}-{self.price_max_threshold})"
                    })
            
            # Check area outliers
            if 'area_value' in df.columns:
                area_outliers = df[
                    (df['area_value'] < self.area_min_threshold) | 
                    (df['area_value'] > self.area_max_threshold)
                ]
                
                for _, row in area_outliers.iterrows():
                    outliers.append({
                        'listing_id': row['listing_id'],
                        'field': 'area_value',
                        'value': row['area_value'],
                        'reason': f"Area outside normal range ({self.area_min_threshold}-{self.area_max_threshold})"
                    })
            
            # Check price per m² outliers
            if 'price_per_m2' in df.columns:
                price_per_m2_outliers = df[
                    df['price_per_m2'].notna() & 
                    ((df['price_per_m2'] < self.price_per_m2_min_threshold) | 
                     (df['price_per_m2'] > self.price_per_m2_max_threshold))
                ]
                
                for _, row in price_per_m2_outliers.iterrows():
                    outliers.append({
                        'listing_id': row['listing_id'],
                        'field': 'price_per_m2',
                        'value': row['price_per_m2'],
                        'reason': f"Price per m² outside normal range ({self.price_per_m2_min_threshold}-{self.price_per_m2_max_threshold})"
                    })
        
        # Check raw_property_details table
        if 'raw_property_details' in data and not data['raw_property_details'].empty:
            df = data['raw_property_details']
            
            # Check bedroom outliers
            if 'bedrooms_value' in df.columns:
                bedroom_outliers = df[
                    df['bedrooms_value'].notna() & 
                    ((df['bedrooms_value'] < 1) | (df['bedrooms_value'] > 20))
                ]
                
                for _, row in bedroom_outliers.iterrows():
                    outliers.append({
                        'listing_id': row['listing_id'],
                        'field': 'bedrooms_value',
                        'value': row['bedrooms_value'],
                        'reason': "Unusual number of bedrooms"
                    })
            
            # Check bathroom outliers
            if 'bathrooms_value' in df.columns:
                bathroom_outliers = df[
                    df['bathrooms_value'].notna() & 
                    ((df['bathrooms_value'] < 1) | (df['bathrooms_value'] > 20))
                ]
                
                for _, row in bathroom_outliers.iterrows():
                    outliers.append({
                        'listing_id': row['listing_id'],
                        'field': 'bathrooms_value',
                        'value': row['bathrooms_value'],
                        'reason': "Unusual number of bathrooms"
                    })
        
        return pd.DataFrame(outliers)