import concurrent.futures
import random
import time
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from retrying import retry
import requests
import re
import datetime
from requests.exceptions import HTTPError
import os

class Scraper:
    def __init__(self, log_file, aziende_file):
        self.user_agent = UserAgent()
        self.session = requests.Session()
        self.log_file = log_file
        self.aziende_file = aziende_file

    @retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=10)
    def get_page(self, url):
        try:
            headers = {'User-Agent': self.user_agent.random}
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except HTTPError as errh:
            print(f"HTTP Error: {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            print(f"Error: {err}")
        return None

    def get_all_pages(self, base_url, last_page, code):
        for page_number in range(1, last_page + 1):
            page_url = f"{base_url}?page={page_number}&ordering=-ultimo_fatturato"
            page_source = self.get_page(page_url)
            self.process_page(base_url, page_number, page_source, code)

    def find_number_of_aziende(self, soup):
        h2_tag = soup.find("h2", style="font-size:1.1em")
        if h2_tag:
            aziende_match = re.search(r'([\d.]+)\s+aziende', h2_tag.get_text(strip=True), re.IGNORECASE)
            if aziende_match:
                aziende_str = aziende_match.group(1).replace('.', '')
                return int(aziende_str)
        return 1

    def find_last_page_number(self, soup, code):
        total_rows = self.find_number_of_aziende(soup)
        print(f"TOTAL AZIENDE FOR ATECO {code}= {total_rows}")

        rows_per_page_guess = (total_rows + 9)
        last_page =  (rows_per_page_guess // 20) + 1
        return min(last_page, 200)

    def extract_data_from_table(self, table):
        data = []
        rows = table.find_all("tr")
        for row in rows[1:]:
            columns = row.find_all("td")
            azienda = columns[0].find("a", class_="azienda-table").get_text(strip=True)
            azienda_url = columns[0].find("a", class_="azienda-table").get("href", "")
            citta = columns[1].find("a", class_="nhref").get_text(strip=True)
            provincia = columns[2].find("a", class_="nhref").get_text(strip=True)
            fatturato = columns[3].get_text(strip=True)
            data.append({
                "azienda": azienda,
                "azienda_url": azienda_url,
                "citta": citta,
                "provincia": provincia,
                "fatturato": fatturato,
            })
        return data

    def process_page(self, base_url, page_number, page_source, code):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{timestamp} - Ateco: {code}, Page: {page_number}\n"
        self.log_file.write(log_message)

        if page_source:
            soup = BeautifulSoup(page_source, "html.parser")
            table = soup.find("table", id="t2")

            if table:
                data = self.extract_data_from_table(table)
                for item in data:
                    aziende_log_message = f"Azienda: {item['azienda']}, Città: {item['citta']}, Provincia: {item['provincia']}, Fatturato: {item['fatturato']}, Ateco: {code}, Numero Pagina: {page_number}\n"
                    print(aziende_log_message)
                    self.aziende_file.write(aziende_log_message)
            else:
                log_message = f"Table with id 't2' not found (Ateco: {code}) on page {page_number}. URL: {base_url}\n"
                print(log_message)
                self.log_file.write(log_message)
        else:
            log_message = f"Failed to retrieve (Ateco: {code}) on page {page_number}. URL: {base_url}\n"
            print(log_message)
            self.log_file.write(log_message)

    def run_scraper(self, code):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"{timestamp} - Starting scraper for Ateco: {code}\n"
        print(log_message)
        self.log_file.write(log_message)

        url_base = f"https://registroaziende.it/ateco/{code}"

        base_url = url_base
        self.session = requests.Session()

        page_source = self.get_page(base_url)

        if page_source:
            soup = BeautifulSoup(page_source, 'html.parser')
            last_page = self.find_last_page_number(soup, code)
            log_message = f"Found last_page: {last_page}\n"
            print(log_message)
            self.log_file.write(log_message)

            log_message = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Starting asynchronous calls for {url_base}\n"
            print(log_message)
            self.log_file.write(log_message)

            self.get_all_pages(base_url, last_page, code)

        log_message = f"Scraper completed for Ateco: {code}\n"
        print(log_message)
        self.log_file.write(log_message)

    def close_driver(self):
        self.log_file.close()
        self.aziende_file.close()

def run_scraper_for_code(code):
    log_folder_path = f"/scraping/ATECO_{code}_{current_date}-{datetime.datetime.now().strftime('%H-%M-%S')}"
    os.makedirs(log_folder_path, exist_ok=True)

    log_file_path = os.path.join(log_folder_path, "scraper_logs.txt")
    aziende_file_path = os.path.join(log_folder_path, "aziende_data.txt")

    with open(log_file_path, "w") as log_file, open(aziende_file_path, "w") as aziende_file:
        scraper = Scraper(log_file, aziende_file)
        scraper.run_scraper(code)
        scraper.close_driver()

if __name__ == "__main__":
    with open('./ATECO.txt', 'r') as codes_file:
        codes = [line.strip() for line in codes_file.readlines()]

    current_date = datetime.datetime.now().strftime("%Y-%m-%d")

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(run_scraper_for_code, codes)
