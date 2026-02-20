import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Configuration constants
BASE_URL = "https://u24.gov.ua/reports"
TARGET_DIR = r"H:\ua-aid-intelligence-hub\data\raw\united24"


def initialize_headless_driver():
    """
    Initializes a Chrome WebDriver with headless settings for server-side
    or background execution without a GUI.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Emulate a standard browser to avoid basic bot detection
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def download_reports():
    """
    Orchestrates the scraping and downloading of PDF reports from the
    dynamically rendered United24 platform.
    """
    if not os.path.exists(TARGET_DIR):
        os.makedirs(TARGET_DIR)
        print(f"Directory created: {TARGET_DIR}")

    driver = initialize_headless_driver()

    try:
        print(f"Requesting target URL: {BASE_URL}")
        driver.get(BASE_URL)

        # Wait for the JavaScript engine to finish rendering the DOM
        # Note: 5 seconds is a safe buffer; consider WebDriverWait for efficiency
        time.sleep(5)

        # Capture the fully rendered HTML source
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Identify all anchor tags containing PDF references
        pdf_anchors = soup.find_all('a', href=True)
        raw_links = [link['href'] for link in pdf_anchors if '.pdf' in link['href'].lower()]

        # Deduplicate and normalize relative URLs
        unique_urls = list(set(raw_links))
        processed_links = [
            link if link.startswith('http') else f"https://u24.gov.ua{link}"
            for link in unique_urls
        ]

        print(f"Discovered {len(processed_links)} potential report links.")

        for url in processed_links:
            # Filter specifically for 'report' assets to avoid downloading static site assets
            if "report" not in url.lower():
                continue

            # Parse filename and sanitize from URL parameters
            filename = os.path.basename(url).split('?')[0]
            local_path = os.path.join(TARGET_DIR, filename)

            if os.path.exists(local_path):
                print(f"Skipping: {filename} (File already present)")
                continue

            print(f"Downloading: {filename}...")
            try:
                # Use requests for the actual file stream to improve memory efficiency
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()

                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=16384):
                        f.write(chunk)

                # Polite delay to prevent triggering rate-limiting on the server
                time.sleep(1)
            except requests.RequestException as e:
                print(f"Failed to download {filename}: {e}")

    finally:
        # Ensure the driver process is terminated to free system resources
        driver.quit()
        print("Scraping session finalized.")


if __name__ == "__main__":
    download_reports()