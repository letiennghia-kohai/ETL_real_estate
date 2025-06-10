import os
import time
import json
import re
import requests
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from typing import List, Dict, Any, Optional
from datetime import datetime
from pyvirtualdisplay import Display
from airflow.exceptions import AirflowSkipException
from .base_extractor import BaseExtractor

class BatDongSanOnlineExtractor(BaseExtractor):
    """
    Extractor for batdongsanonline.vn
    """
    
    def __init__(self, 
                 base_url: str = "https://batdongsanonline.vn", 
                 start_path: str = "/mua-ban-nha",
                 max_pages: int = 2,
                 data_path: str = "/opt/airflow/data/raw",
                 headless: bool = True,
                 use_selenium: bool = True,
                 driver_port: int = 9515,  # Thêm tham số cổng
                 *args, **kwargs):
        """
        Initialize the BatDongSanOnline extractor
        
        Args:
            base_url: Base URL for batdongsanonline.vn
            start_path: Path to start extraction from
            max_pages: Maximum number of pages to extract
            data_path: Path where extracted data will be stored
            headless: Whether to run browser in headless mode
            use_selenium: Whether to use Selenium or requests/BS4
        """
        super().__init__(
            source_name="batdongsanonline",
            base_url=base_url,
            max_items=max_pages * 20,  # Assuming ~20 listings per page
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
            self.logger.info("Setting up Selenium WebDriver")
            import time
            import random            
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
            options.add_argument("--remote-debugging-port=9223")
            # Thêm tùy chọn chống phát hiện
            # options.add_argument("--disable-blink-features=AutomationControlled")
            # options.add_experimental_option("excludeSwitches", ["enable-automation"])
            # options.add_experimental_option("useAutomationExtension", False)
                        # Sử dụng cổng riêng
            service = Service(port=self.driver_port)
            
            # Try undetected-chromedriver first
            try:
                import undetected_chromedriver as uc
                self.driver = uc.Chrome(options=options,service=service)
                self.logger.info("Using undetected-chromedriver")
            except ImportError:
                self.driver = webdriver.Chrome(options=options)
                self.logger.info("Using standard Selenium Chrome driver")
                
            self.wait = WebDriverWait(self.driver, 10)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set up WebDriver: {str(e)}")
            if hasattr(self, 'display') and self.display:
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
        if hasattr(self, 'display') and self.display:
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
            page_url: URL of the page to extract from
            
        Returns:
            List of basic listing information
        """
        if page_url is None:
            page_url = self.full_url
            
        listing_items = []
        
        try:
            if self.use_selenium:
                # Use Selenium for JavaScript-heavy pages
                self.driver.get(page_url)
                self.random_sleep(2, 4)  # Wait for the page to load
                
                # Wait for listings to load
                self.wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul.ul_td_bds li"))
                )
                
                listings = self.driver.find_elements(By.CSS_SELECTOR, "ul.ul_td_bds li")
                
                for listing in listings:
                    try:
                        link_element = listing.find_element(By.CSS_SELECTOR, "div.titleTindang a")
                        url = link_element.get_attribute("href")
                        title = link_element.text.strip()
                        
                        # Extract ID from URL if possible
                        listing_id = None
                        id_match = re.search(r'/([^/]+)\.html', url)
                        if id_match:
                            listing_id = id_match.group(1)
                        
                        listing_items.append({
                            "id": listing_id,
                            "url": url,
                            "title": title,
                            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                    except NoSuchElementException:
                        continue
            else:
                # Use requests + BeautifulSoup for simpler pages
                response = self.session.get(page_url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                listings = soup.select("ul.ul_td_bds li")
                
                for listing in listings:
                    try:
                        link_element = listing.select_one("div.titleTindang a")
                        if link_element:
                            url = link_element.get("href")
                            if not url.startswith("http"):
                                url = f"{self.base_url}{url}"
                            title = link_element.text.strip()
                            
                            # Extract ID from URL if possible
                            listing_id = None
                            id_match = re.search(r'/([^/]+)\.html', url)
                            if id_match:
                                listing_id = id_match.group(1)
                            
                            listing_items.append({
                                "id": listing_id,
                                "url": url,
                                "title": title,
                                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            })
                    except Exception as e:
                        self.logger.warning(f"Error extracting listing info: {str(e)}")
                        continue
                
            self.logger.info(f"Found {len(listing_items)} listings on page")
            return listing_items
            
        except Exception as e:
            self.logger.error(f"Error getting listings from {page_url}: {str(e)}")
            return []
    
    def extract_item_details(self, item_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract detailed information for a specific listing
        
        Args:
            item_info: Basic information about the listing
            
        Returns:
            Dictionary with detailed listing information
        """
        if not item_info or "url" not in item_info:
            return {"error": "Invalid item info"}
            
        url = item_info["url"]
        
        try:
            listing_data = {
                **item_info,
                "price": "",
                "area": "",
                "location": "",
                "post_date": "",
                "listing_type": "",
                "description": "",
                "features": {},
                "contact_name": "",
                "contact_phone": "",
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            if self.use_selenium:
                self.driver.get(url)
                self.random_sleep(2, 4)
                
                # Extract basic info
                try:
                    listing_data["title"] = self._get_text_selenium("h1.title_D")
                except:
                    pass
                    
                # Extract listing ID
                try:
                    id_text = self._get_xpath_selenium("//div[contains(text(), 'Mã tin :')]")
                    listing_data["id"] = id_text.split("#")[1].strip() if "#" in id_text else ""
                except:
                    pass
                    
                # Extract post date
                try:
                    date_text = self._get_xpath_selenium("//div[contains(text(), 'Ngày đăng :')]")
                    listing_data["post_date"] = date_text.replace("Ngày đăng :", "").strip()
                except:
                    pass
                    
                # Extract listing type
                try:
                    type_text = self._get_xpath_selenium("//div[contains(text(), 'Loại tin :')]")
                    listing_data["listing_type"] = type_text.replace("Loại tin :", "").strip()
                except:
                    pass
                    
                # Extract description
                try:
                    listing_data["description"] = self._get_text_selenium("div.f-detail-content1")
                except:
                    pass
                # Extract price
                try:
                    price_element = self.driver.find_element(
                        By.CSS_SELECTOR, ".short-detail-wrap .amount.cl-red strong")
                    listing_data["price"] = price_element.text.strip()
                except NoSuchElementException:
                    pass

                # Extract area
                try:
                    area_element = self.driver.find_element(
                        By.XPATH, "//span[@class='sp1' and contains(text(), 'Diện tích')]/following-sibling::span/strong")
                    listing_data["area"] = area_element.text.strip()
                except NoSuchElementException:
                    pass
                    
                # Extract features
                try:
                    feature_items = self.driver.find_elements(By.CSS_SELECTOR, "ul.listTienich li")
                    for item in feature_items:
                        try:
                            feature_name = item.find_element(By.CSS_SELECTOR, "span").text.strip()
                            feature_value = item.find_element(By.CSS_SELECTOR, "div.text-right").text.strip()
                            listing_data["features"][feature_name] = feature_value
                            
                        except:
                            continue
                except:
                    pass
                    
                # Extract location
                try:
                    location = self._get_text_selenium(".address")
                    listing_data["location"] = location.replace("Địa chỉ:", "").strip()
                except:
                    pass
                    
                # Extract contact info
                try:
                    listing_data["contact_name"] = self._get_text_selenium("span.name")
                except:
                    pass
                
                try:
                    phone_element = self.driver.find_element(By.CSS_SELECTOR, "a.tag-phone")
                    listing_data["contact_phone"] = phone_element.get_attribute("data-phone")
                except:
                    pass
            else:
                # Use requests + BeautifulSoup
                response = self.session.get(url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract basic info
                try:
                    listing_data["title"] = self._get_text_bs4(soup, "h1.title")
                except:
                    pass
                
                # Extract ID if not already present
                if not listing_data.get("id"):
                    try:
                        id_element = soup.select_one("div.code")
                        if id_element:
                            id_text = id_element.text.strip()
                            id_match = re.search(r'#(\w+)', id_text)
                            if id_match:
                                listing_data["id"] = id_match.group(1)
                    except:
                        pass
                
                # Extract description
                try:
                    listing_data["description"] = self._get_text_bs4(soup, "div.f-detail-content1")
                except:
                    pass
                
                # Extract features
                try:
                    feature_items = soup.select("ul.listTienich li")
                    for item in feature_items:
                        try:
                            feature_name = item.select_one("span").text.strip()
                            feature_value = item.select_one("div.text-right").text.strip()
                            listing_data["features"][feature_name] = feature_value
                            
                            # Extract area and price from features if available
                            if "Diện tích" in feature_name:
                                listing_data["area"] = feature_value
                            if "Giá" in feature_name:
                                listing_data["price"] = feature_value
                        except:
                            continue
                except:
                    pass
                
                # Extract location
                try:
                    location_element = soup.select_one(".address")
                    if location_element:
                        listing_data["location"] = location_element.text.replace("Địa chỉ:", "").strip()
                except:
                    pass
                
                # Extract contact info
                try:
                    listing_data["contact_name"] = self._get_text_bs4(soup, "span.name")
                except:
                    pass
                
            self.logger.info(f"Extracted details for listing {listing_data.get('id', 'N/A')}")
            return listing_data
            
        except Exception as e:
            error_msg = f"Error extracting details from {url}: {str(e)}"
            self.logger.error(error_msg)
            return {"error": error_msg, "url": url}
    
    def _get_text_selenium(self, selector: str, default: str = "") -> str:
        """Helper to get text from element using Selenium"""
        try:
            element = self.driver.find_element(By.CSS_SELECTOR, selector)
            return element.text.strip()
        except:
            return default
    
    def _get_xpath_selenium(self, xpath: str, default: str = "") -> str:
        """Helper to get text from XPath using Selenium"""
        try:
            element = self.driver.find_element(By.XPATH, xpath)
            return element.text.strip()
        except:
            return default
            
    def _get_text_bs4(self, soup, selector, default=""):
        """Helper to get text from element using BeautifulSoup"""
        try:
            element = soup.select_one(selector)
            if element:
                return element.text.strip()
            return default
        except:
            return default
            
    def save_data(self, data, filepath):
        """
        Save data to a JSON file, appending to existing data rather than overwriting.
        If records have the same ID but different timestamps, both will be preserved.
        
        Args:
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