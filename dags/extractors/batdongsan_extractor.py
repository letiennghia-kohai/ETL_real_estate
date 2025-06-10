"""
BatDongSan Extractor Module
Extracts real estate data from batdongsan.com.vn
"""

import os
import time
import json
import re
import logging
import requests
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyvirtualdisplay import Display
from airflow.exceptions import AirflowSkipException

from .base_extractor import BaseExtractor


class BatDongSanExtractor(BaseExtractor):
    """
    Extractor for batdongsan.com.vn
    """
    
    def __init__(self, 
             base_url: str = "https://batdongsan.com.vn", 
             start_path: str = "/nha-dat-ban",
             max_pages: int = 2,
             data_path: str = "/opt/airflow/data/raw",
             headless: bool = False,
             use_selenium: bool = True,
             driver_port: int = 9516,
             *args, **kwargs):
        """
        Initialize the BatDongSan extractor
        
        Args:
            base_url: Base URL for batdongsan.com.vn
            start_path: Path to start extraction from
            max_pages: Maximum number of pages to extract
            data_path: Path where extracted data will be stored
            headless: Whether to run browser in headless mode
            use_selenium: Whether to use Selenium (True) or requests/BS4 (False)
            *args, **kwargs: Additional arguments for parent class
        """
        super().__init__(
            source_name="batdongsan",
            base_url=base_url,
            max_items=max_pages * 20,  # Assuming 20 listings per page
            data_path=data_path,
            *args, **kwargs
        )
        
        self.start_path = start_path
        self.full_url = f"{base_url}{start_path}"
        self.max_pages = max_pages
        self.headless = headless
        self.use_selenium = use_selenium
        self.driver = None
        self.wait = None
        self.display = None
        self.session = requests.Session()
        self.driver_port = driver_port
        
        # Set up headers for requests
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
        }
        
        # Initialize last processed ID tracking
        self.last_id_file = os.path.join(self.data_path, f"{self.source_name}_last_id.txt")
        self.last_processed_id = self._load_last_processed_id()
        
    def _setup_driver(self):
        """Set up Selenium WebDriver if needed"""
        if not self.use_selenium or self.driver is not None:
            return True

        try:
            from selenium import webdriver
            from pyvirtualdisplay import Display
            import time
            import random
            self.logger.info("Setting up Selenium WebDriver")
            from selenium.webdriver.chrome.service import Service
            
            # Start virtual display
            # Thêm delay ngẫu nhiên trước khi khởi động để tránh xung đột
            time.sleep(random.uniform(1, 3))
            
            # Start virtual display với ID khác nhau
            display_id = random.randint(1000, 9999)
            self.display = Display(visible=0, size=(1920, 1080))
            self.display.start()
            self.logger.info(f"Virtual display started with ID: {display_id}")

            options = webdriver.ChromeOptions()
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--remote-debugging-port=9222")
            # # Thêm tùy chọn chống phát hiện
            # options.add_argument("--disable-blink-features=AutomationControlled")
            # options.add_experimental_option("excludeSwitches", ["enable-automation"])
            # options.add_experimental_option("useAutomationExtension", False)
            
            # Sử dụng cổng riêng
            service = Service(port=self.driver_port)
            
            # Try undetected-chromedriver first
            try:
                import undetected_chromedriver as uc
                self.driver = uc.Chrome(options=options,service=service,auto_install=True)
                self.logger.info("Using undetected-chromedriver")
            except ImportError:
                self.driver = webdriver.Chrome(options=options)
                self.logger.info("Using standard Selenium Chrome driver")
                
            self.wait = WebDriverWait(self.driver, 10)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set up WebDriver: {str(e)}")
            if hasattr(self, 'display'):
                self.display.stop()
            return False

    def _close_driver(self):
        """Close the Selenium WebDriver if it's open"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                self.wait = None
            except Exception as e:
                self.logger.error(f"Error closing WebDriver: {str(e)}")
        if hasattr(self, 'display'):
            try:
                self.display.stop()
            except Exception as e:
                self.logger.error(f"Error stopping display: {str(e)}")

    def _load_last_processed_id(self) -> Optional[str]:
        """Load last processed ID from file"""
        try:
            if os.path.exists(self.last_id_file):
                with open(self.last_id_file, 'r') as f:
                    return f.read().strip()
        except Exception as e:
            self.logger.warning(f"Error loading last processed ID: {str(e)}")
        return None

    def _save_last_processed_id(self, last_id: str) -> None:
        """Save last processed ID to file"""
        try:
            os.makedirs(os.path.dirname(self.last_id_file), exist_ok=True)
            with open(self.last_id_file, 'w') as f:
                f.write(str(last_id))
            self.logger.info(f"Saved last processed ID: {last_id}")
        except Exception as e:
            self.logger.error(f"Error saving last processed ID: {str(e)}")
    
    def extract(self) -> List[Dict[str, Any]]:
        """
        Main extraction method with ID checkpointing
        """
        try:
            self.metrics["start_time"] = datetime.now()
            
            if self.use_selenium and not self._setup_driver():
                raise Exception("Failed to set up WebDriver")
            
            all_listings = []
            results = []
            page_urls = [f"{self.base_url}{self.start_path}/p{page}" for page in range(1, self.max_pages + 1)]
            page_urls[0] = f"{self.base_url}{self.start_path}"  # First page
            
            self.logger.info(f"Last processed ID from previous run: {self.last_processed_id}")

            for page_idx, page_url in enumerate(page_urls, 1):
                try:
                    self.logger.info(f"Extracting from page {page_idx}/{len(page_urls)}: {page_url}")
                    
                    listings = self.get_items_list(page_url)
                    self.metrics["total_items_found"] += len(listings)
                    
                    # Check if we've reached the last processed ID
                    if self.last_processed_id:
                        current_ids = [item['id'] for item in listings if 'id' in item]
                        if self.last_processed_id in current_ids:
                            self.logger.info(f"Found last processed ID {self.last_processed_id} in current page. Stopping extraction.")
                            return []
                    
                    all_listings.extend(listings)
                    
                    for listing_idx, listing in enumerate(listings):
                        if self.metrics["items_extracted"] >= self.max_items:
                            self.logger.info(f"Reached maximum items limit ({self.max_items})")
                            break
                            
                        self.logger.info(f"Processing listing {listing_idx+1}/{len(listings)}: {listing.get('id', 'N/A')}")
                        
                        detail_data = self.extract_item_details(listing)
                        
                        if detail_data and "error" not in detail_data:
                            if "id" in detail_data and detail_data["id"]:
                                self.save_item_to_json(detail_data, detail_data["id"])
                                # Update last processed ID
                                self._save_last_processed_id(detail_data["id"])
                            
                            results.append(detail_data)
                            self.metrics["items_extracted"] += 1
                        else:
                            self.metrics["failures"] += 1
                        
                        self.random_sleep(2, 4)
                        
                    self.random_sleep(3, 6)
                    
                except AirflowSkipException:
                    raise  # Re-raise the skip exception
                except Exception as e:
                    self.logger.error(f"Error processing page {page_url}: {str(e)}")
                    self.metrics["failures"] += 1
                    continue
            
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.save_to_json(results, batch_id)
            
            if self.use_selenium:
                self._close_driver()
                
            self.metrics["end_time"] = datetime.now()
            self.log_extraction_report()
            
            return results
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {str(e)}")
            if self.use_selenium:
                self._close_driver()
            raise
            
    
    def get_items_list(self, page_url: str = None) -> List[Dict[str, Any]]:
        """
        Get a list of property listings from a page
        
        Args:
            page_url: URL of the page to extract listings from
            
        Returns:
            List of basic listing information
        """
        if page_url is None:
            page_url = f"{self.base_url}{self.start_path}"
            
        listing_items = []
        
        try:
            if self.use_selenium:
                # Use Selenium for JavaScript-heavy pages
                self.driver.get(page_url)
                self.random_sleep(3, 5)  # Wait for the page to load
                
                # Wait for listing container
                listing_container = self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".re__card-full"))
                )
                
                # Get all listing elements
                listing_elements = self.driver.find_elements(By.CSS_SELECTOR, ".re__card-full")
                
                for element in listing_elements:
                    try:
                        # Get basic information
                        listing_id = element.get_attribute("prid")
                        link_element = element.find_element(By.CSS_SELECTOR, "a.js__product-link-for-product-id")
                        url = link_element.get_attribute("href")
                        title = link_element.get_attribute("title")
                        vip_type = element.get_attribute("vtp")
                        
                        if url and listing_id:
                            listing_info = {
                                "id": listing_id,
                                "url": url,
                                "title": title,
                                "vip_type": vip_type
                            }
                            listing_items.append(listing_info)
                    except Exception as e:
                        self.logger.warning(f"Error extracting listing info: {str(e)}")
                        continue
            else:
                # Use requests + BeautifulSoup for simpler pages
                response = self.session.get(page_url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                listing_elements = soup.select(".re__card-full")
                
                for element in listing_elements:
                    try:
                        listing_id = element.get("prid")
                        link_element = element.select_one("a.js__product-link-for-product-id")
                        if link_element:
                            url = link_element.get("href")
                            title = link_element.get("title")
                            vip_type = element.get("vtp")
                            
                            if url and listing_id:
                                if not url.startswith("http"):
                                    url = f"{self.base_url}{url}"
                                    
                                listing_info = {
                                    "id": listing_id,
                                    "url": url,
                                    "title": title,
                                    "vip_type": vip_type
                                }
                                listing_items.append(listing_info)
                    except Exception as e:
                        self.logger.warning(f"Error extracting listing info: {str(e)}")
                        continue
                        
            self.logger.info(f"Found {len(listing_items)} listings on {page_url}")
            return listing_items
            
        except Exception as e:
            self.logger.error(f"Error getting listing links from {page_url}: {str(e)}")
            return []
    
    def extract_item_details(self, item_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract detailed information for a specific property listing
        
        Args:
            item_info: Basic information about the listing
            
        Returns:
            Dictionary with detailed listing information
        """
        if not item_info or "url" not in item_info:
            return {"error": "Invalid item info"}
            
        url = item_info["url"]
        
        try:
            property_data = {
                **item_info,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            if self.use_selenium:
                # Use Selenium
                self.driver.get(url)
                self.random_sleep(3, 5)  # Wait for the page to load
                
                # Extract property details
                property_data["title"] = self._get_text_selenium("h1.re__pr-title")
                property_data["address"] = self._get_text_selenium("span.re__pr-short-description")
                
                # Extract price
                price_text = self._get_text_selenium("div.re__pr-short-info-item:nth-child(1) span.value")
                price_ext = self._get_text_selenium("div.re__pr-short-info-item:nth-child(1) span.ext")
                property_data["price_text"] = price_text
                property_data["price_value"] = self._extract_number(price_text)
                property_data["price_per_m2"] = price_ext
                
                # Extract area
                area_text = self._get_text_selenium("div.re__pr-short-info-item:nth-child(2) span.value")
                area_ext = self._get_text_selenium("div.re__pr-short-info-item:nth-child(2) span.ext")
                property_data["area_text"] = area_text
                property_data["area_value"] = self._extract_area(area_text)
                property_data["floors"] = area_ext
                
                # Extract bedrooms
                bedrooms = self._get_text_selenium("div.re__pr-short-info-item:nth-child(3) span.value")
                property_data["bedrooms"] = bedrooms
                property_data["bedrooms_value"] = self._extract_number(bedrooms)
                
                # Extract description
                property_data["description"] = self._get_text_selenium("div.re__detail-content")
                
                # Extract other details
                property_data["details"] = {}
                fields_to_extract = [
                    ("bedrooms", "Số phòng ngủ"),
                    ("floors", "Số tầng"),
                    ("bathrooms", "Số phòng tắm"),
                    ("legal_status", "Pháp lý"),
                    ("furniture", "Nội thất"),
                    ("direction", "Hướng nhà"),
                    ("balcony_direction", "Hướng ban công"),
                    ("road_width", "Đường vào"),
                    ("house_front", "Mặt tiền")
                ]
                
                for field_name, field_text in fields_to_extract:
                    value = self._get_xpath_contains_selenium(field_text)
                    property_data["details"][field_name] = value
                    if field_name == "bathrooms":
                        property_data["details"]["bathrooms_value"] = self._extract_number(value)
                
                # Extract project information
                property_data["project"] = self._get_text_selenium("div.re__project-title")
                
                # Extract date fields
                date_fields = [
                    ("post_date", "Ngày đăng"),
                    ("expiration_date", "Ngày hết hạn"),
                    ("post_type", "Loại tin"),
                    ("post_id", "Mã tin")
                ]
                
                for field_name, field_text in date_fields:
                    property_data[field_name] = self._get_xpath_contains_selenium(field_text)
                
                # Extract contact info
                try:
                    property_data["contact"] = {
                        "name": self._get_text_selenium(".re__contact-name"),
                        "phone": "*** (Đã ẩn)",  # For privacy compliance
                    }
                except:
                    property_data["contact"] = {}
                    
            else:
                # Use requests + BeautifulSoup
                response = self.session.get(url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract property details
                property_data["title"] = self._get_text_bs4(soup, "h1.re__pr-title")
                property_data["address"] = self._get_text_bs4(soup, "span.re__pr-short-description")
                
                # Extract price
                price_text = self._get_text_bs4(soup, "div.re__pr-short-info-item:nth-child(1) span.value")
                price_ext = self._get_text_bs4(soup, "div.re__pr-short-info-item:nth-child(1) span.ext")
                property_data["price_text"] = price_text
                property_data["price_value"] = self._extract_number(price_text)
                property_data["price_per_m2"] = price_ext
                
                # Extract area
                area_text = self._get_text_bs4(soup, "div.re__pr-short-info-item:nth-child(2) span.value")
                area_ext = self._get_text_bs4(soup, "div.re__pr-short-info-item:nth-child(2) span.ext")
                property_data["area_text"] = area_text
                property_data["area_value"] = self._extract_area(area_text)
                property_data["floors"] = area_ext
                
                # Extract bedrooms
                bedrooms = self._get_text_bs4(soup, "div.re__pr-short-info-item:nth-child(3) span.value")
                property_data["bedrooms"] = bedrooms
                property_data["bedrooms_value"] = self._extract_number(bedrooms)
                
                # Extract description
                property_data["description"] = self._get_text_bs4(soup, "div.re__detail-content")
                
                # Extract other details
                property_data["details"] = {}
                # The rest would require XPath-like functionality in BS4 which is more complex
                # For simplicity, we'll focus on the main details here
                
                # Extract project information
                property_data["project"] = self._get_text_bs4(soup, "div.re__project-title")
                
                # Extract contact info
                try:
                    property_data["contact"] = {
                        "name": self._get_text_bs4(soup, ".re__contact-name"),
                        "phone": "*** (Đã ẩn)",  # For privacy compliance
                    }
                except:
                    property_data["contact"] = {}
            
            self.logger.info(f"Successfully extracted details for listing {property_data.get('id', 'N/A')}")
            return property_data
            
        except Exception as e:
            error_message = f"Error extracting details from {url}: {str(e)}"
            self.logger.error(error_message)
            return {"error": error_message, "url": url}
    
    def _get_text_selenium(self, selector, default=""):
        """Get text from element using Selenium"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return self._clean_text(element.text)
        except NoSuchElementException:
            return default
        except Exception as e:
            self.logger.warning(f"Error getting text from {selector}: {str(e)}")
            return default
    def save_data(self, data, filepath):
        """
        Save data to a JSON file, appending to existing data rather than overwriting.
        If records have the same ID but different timestamps, both will be preserved.
        
        Args:s
            data: The data to save (list of dictionaries)
            filepath: Path where the data should be saved
        
        Returns:
            str: Path to the saved file
        """
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        existing_data = []
        
        # Load existing data if file exists
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                self.logger.info(f"Loaded {len(existing_data)} existing records from {filepath}")
            except Exception as e:
                self.logger.warning(f"Error loading existing data from {filepath}: {str(e)}")
                # If file exists but can't be read, create a backup
                if os.path.getsize(filepath) > 0:
                    backup_path = f"{filepath}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    try:
                        os.rename(filepath, backup_path)
                        self.logger.info(f"Created backup of corrupted file: {backup_path}")
                    except Exception as be:
                        self.logger.error(f"Failed to create backup: {str(be)}")
        
        # Check if data is a list
        if not isinstance(data, list):
            data = [data]
        
        # Create an index of existing items by ID and timestamp
        existing_index = {}
        for item in existing_data:
            if 'id' in item and 'scraped_at' in item:
                key = f"{item['id']}_{item['scraped_at']}"
                existing_index[key] = True
        
        # Filter out exact duplicates (same ID and same timestamp)
        new_items = []
        for item in data:
            should_add = True
            if 'id' in item and 'scraped_at' in item:
                key = f"{item['id']}_{item['scraped_at']}"
                if key in existing_index:
                    should_add = False
                    
            if should_add:
                new_items.append(item)
        
        # Merge existing and new data
        combined_data = existing_data + new_items
        
        # Save combined data
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Successfully saved {len(combined_data)} records to {filepath} (added {len(new_items)} new items)")
            return filepath
        except Exception as e:
            self.logger.error(f"Error saving data to {filepath}: {str(e)}")
            raise
    def _get_xpath_contains_selenium(self, contains_text, element_type="span", get_next_sibling=True):
        """Get text from element based on contained text using Selenium"""
        try:
            xpath = f"//{element_type}[contains(text(),'{contains_text}')]"
            if get_next_sibling:
                xpath += "/following-sibling::span"
            
            element = self.driver.find_element(By.XPATH, xpath)
            return self._clean_text(element.text)
        except NoSuchElementException:
            return ""
        except Exception as e:
            self.logger.warning(f"Error getting element containing {contains_text}: {str(e)}")
            return ""
    
    def _get_text_bs4(self, soup, selector, default=""):
        """Get text from element using BeautifulSoup"""
        try:
            element = soup.select_one(selector)
            if element:
                return self._clean_text(element.text)
            return default
        except Exception as e:
            self.logger.warning(f"Error getting text from {selector}: {str(e)}")
            return default
    
    def _clean_text(self, text):
        """Clean text, removing extra whitespace"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text).strip()
    
    def _extract_number(self, text):
        """Extract number from text (e.g., price)"""
        if not text:
            return None
        
        # Handle billion (tỷ)
        match_ty = re.search(r'([\d,.]+)\s*t(?:ỷ|ỉ)', text)
        if match_ty:
            return float(match_ty.group(1).replace(',', '.'))
        
        # Handle million (triệu)
        match_trieu = re.search(r'([\d,.]+)\s*tr(?:i[ệê]u)', text)
        if match_trieu:
            return float(match_trieu.group(1).replace(',', '.')) / 1000  # Convert to billions
        
        # Handle plain numbers
        match_number = re.search(r'(\d+)', text)
        if match_number:
            return int(match_number.group(1))
        
        return None
    
    def _extract_area(self, text):
        """Extract area from text"""
        if not text:
            return None
        
        match = re.search(r'([\d,.]+)\s*m(?:\s|²|2)', text)
        if match:
            return float(match.group(1).replace(',', '.'))
        
        return None