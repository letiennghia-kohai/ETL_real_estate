from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
import undetected_chromedriver as uc
import time
import logging
import os
import traceback
from pyvirtualdisplay import Display

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("chrome_test.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def start_display():
    """Start virtual display"""
    display = Display(visible=0, size=(1920, 1080))
    display.start()
    return display

def test_undetected_chrome(**kwargs):
    """Test function to verify undetected_chromedriver is working"""
    driver = None
    display = None
    try:
        # Start virtual display
        display = start_display()
        logger.info("Virtual display started")
        
        logger.info("Initializing undetected_chromedriver...")
        
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        
        # Running in headless mode inside container
        # options.add_argument("--headless=new")
        
        driver = uc.Chrome(options=options)
        logger.info("Successfully initialized undetected_chromedriver")
        
        test_url = "https://batdongsan.com.vn"
        logger.info(f"Attempting to access {test_url}")
        driver.get(test_url)
        
        time.sleep(5)
        
        page_title = driver.title
        logger.info(f"Page title: {page_title}")
        current_url = driver.current_url
        logger.info(f"Current URL: {current_url}")
        
        screenshot_path = "/opt/airflow/logs/batdongsan_test.png"
        driver.save_screenshot(screenshot_path)
        logger.info(f"Screenshot saved to {screenshot_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error testing undetected_chromedriver: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        if driver:
            driver.quit()
            logger.info("Driver closed successfully")
        if display:
            display.stop()
            logger.info("Virtual display stopped")

# ... [rest of your DAG definition remains the same]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': days_ago(1),
}
def test_with_specific_version(**kwargs):
    """Test with a specific version of Chrome/ChromeDriver"""
    try:
        logger.info("Testing with specific version...")
        
        # Specify Chrome version - adjust to match your installed version
        driver = uc.Chrome(version_main=117)  # Change to your Chrome version
        
        driver.get("https://batdongsan.com.vn")
        time.sleep(5)
        driver.save_screenshot("batdongsan_specific_version.png")
        driver.quit()
        logger.info("Test with specific version completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error with specific version test: {str(e)}")
        raise
with DAG(
    'test_undetected_chromedriver',
    default_args=default_args,
    description='A DAG to test undetected_chromedriver',
    schedule_interval=None,  # Run manually for testing
    catchup=False,
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Task to test undetected_chromedriver with automatic version
    test_chrome_task = PythonOperator(
        task_id='test_undetected_chrome',
        python_callable=test_undetected_chrome,
        provide_context=True,
    )
# Task to test with specific Chrome version (runs if first task fails)
    test_specific_version_task = PythonOperator(
        task_id='test_with_specific_version',
        python_callable=test_with_specific_version,
        provide_context=True,
    )

    # End task
    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> test_chrome_task >> end
    # test_chrome_task >> test_specific_version_task >> end

    # Conditional logic to skip specific version test if first test succeeds
    def check_test_result(**kwargs):
        ti = kwargs['ti']
        if ti.xcom_pull(task_ids='test_undetected_chrome'):
            raise AirflowSkipException("First test succeeded, skipping specific version test")

    from airflow.operators.python_operator import ShortCircuitOperator
    check_result = ShortCircuitOperator(
        task_id='check_test_result',
        python_callable=check_test_result,
        provide_context=True,
    )