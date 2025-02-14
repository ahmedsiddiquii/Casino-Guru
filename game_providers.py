from lxml import html
from concurrent.futures import ThreadPoolExecutor
import random
import threading
import concurrent.futures
import json
import time
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class GameProviderScraper:
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0'
        ]

        self.headers_pool = [
            {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Pragma': 'no-cache',
                'Cache-Control': 'no-cache',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"'
            } for _ in range(5)
        ]

        self.proxies = {}
        self.proxy = self._get_proxy()

        self.setup_session()
        self.providers_processed = 0
        self.total_requests = 0
        self.data_lock = threading.Lock()
        self.file_lock = threading.Lock()
        self.session_lock = threading.Lock()
        self.thread_count_lock = threading.Lock()
        self.active_threads = 0
        self.processed_urls_file = 'processed_providers_urls.txt'
        self.data_file = 'game_providers_data.json'

        self.processed_urls = self.get_processed_urls()

    def _verify_proxy(self, proxy_url: str, timeout: int = 10) -> bool:
        """Verify if a proxy is working by testing it against multiple URLs."""
        url = 'https://casinoguru-en.com'

        proxies = {
            'http': proxy_url,
            'https': proxy_url
        }

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

        try:
            response = requests.get(
                url,
                proxies=proxies,
                headers=headers,
                timeout=timeout,
                verify=True,
                allow_redirects=True,
            )
            if response.status_code == 200:
                print(f"Proxy {proxy_url} working - Status: {response.status_code}")
                return True
            else:
                print(f"Proxy {proxy_url} failed - Status: {response.status_code}")

        except Exception as e:
            print(f"Proxy {proxy_url} verification failed: {str(e)}")
        return False

    def _get_proxy(self) -> str:
        proxies_file = 'formatted_proxies.txt'

        try:
            if os.path.exists(proxies_file):
                with open(proxies_file, 'r') as file:
                    proxies = file.read().splitlines()

                for _ in range(5):
                    proxy_url = random.choice(proxies)
                    print("\nWeryfikowanie serwera proxy:", proxy_url)
                    try:
                        # Verify proxy
                        if self._verify_proxy(proxy_url):
                            print("Znaleziono działający serwer proxy: ", proxy_url)
                            self.proxies = {
                                "http": proxy_url,
                                "https": proxy_url,
                            }
                            return proxy_url
                    except Exception as e:
                        print("Błąd weryfikacji serwera proxy: ", str(e))

                print("Sprawdzono 5 różne serwery proxy, ale żaden nie działa")
            else:
                print("Brak pliku z serwerami proxy: ", proxies_file)
        except Exception as e:
            print("Błąd pobierania serwerów proxy: ", str(e))
        return None

    def setup_session(self):
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "POST"]
        )

        self.session = requests.Session()

        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers = random.choice(self.headers_pool)

        if self.proxies:
            print("Setting up session with proxy:", self.proxy)
            self.session.proxies.update(self.proxies)

    def get_processed_urls(self):
        processed_urls = set()
        try:
            if os.path.exists(self.processed_urls_file):
                with open(self.processed_urls_file, 'r', encoding='utf-8') as f:
                    processed_urls = set(line.strip() for line in f if line.strip())
        except Exception as e:
            print(f"Error loading processed URLs: {e}")
        return processed_urls

    def save_processed_url(self, url):
        with self.file_lock:
            try:
                with open(self.processed_urls_file, 'a', encoding='utf-8') as f:
                    f.write(f"{url}\n")
            except Exception as e:
                print(f"Error saving URL: {e}")

    def make_request(self, url, method="GET", headers=None, max_retries=5):
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(2, 5))

                with self.session_lock:
                    self.session.headers = headers or random.choice(self.headers_pool)

                    if self.total_requests > 0 and self.total_requests % 100 == 0:
                        print("\nRefreshing session...")
                        self.session.close()
                        self.setup_session()
                        print("Session refreshed")

                    response = self.session.request(
                        method,
                        url,
                        timeout=30,
                        allow_redirects=True,
                        verify=True
                    )

                    if response.status_code == 403:
                        print(f"Received 403 for {url}. Waiting before retry...")
                        time.sleep(random.uniform(10, 15))
                        continue

                    self.total_requests += 1
                    return response

            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(random.uniform(5, 10))

            except requests.exceptions.ProxyError as e:
                print(f"Proxy error: {str(e)}")
                self.proxy = self._get_proxy()
                self.session.close()
                self.setup_session()

    def get_game_providers(self):
        def fetch_page(page_num):
            try:
                url = f"https://casinoguru-en.com/frontendService/gameProvidersFilterServiceMore?page={page_num}&initialPage=1"
                headers = {
                    'accept': 'application/json, text/plain, /',
                    'accept-language': 'pl,pl-PL;q=0.9',
                    'cache-control': 'no-cache',
                    'content-type': 'application/x-www-form-urlencoded',
                    'cookie': 'eSortDesc=false; visitorIdIgnore=false; firstSessionLandingPageCode=gameDetail-19155; firstSessionLandingPageType=gameDetail; firstSessionLandingPageCategory=gameDetail; adwTraffic=false; tZone=Europe/Zurich; firstHit=1739315741766; restCSSIsCached=true; _ga=GA1.1.1709600112.1739377843; landingPageCode=gameList; landingPageType=gameList; landingPageCategory=gameList; abTest=t-31#b|t-32#c|t-35#b|t-34#b; returnIn30Days=true; _ga_87PKW81MD7=GS1.1.1739400911.6.0.1739400911.0.0.0; _ga_ZP4V1V9Y4X=GS1.1.1739377843.1.1.1739400912.59.0.0; visitorId=1718217012037803; loggingUserErrors=false; mouseFlow=false; userscore-refresh-counter=0; userscore={%22points%22:178%2C%22ranking%22:1%2C%22casinosVisited%22:0%2C%22bonusesVisited%22:0%2C%22playFreeVisited%22:0%2C%22showMoreVisited%22:0%2C%22focusTime%22:0%2C%22struggling%22:false%2C%22game%22:4369}; landingPageBeforeRedirect=https://casinoguru-en.com/game-providers; cookies_policy_alert_showed=true; lastHit=1739482321546; JSESSIONID=C6AAE11E396D43093DA8443F600309F6',
                    'origin': 'https://casinoguru-en.com',
                    'pragma': 'no-cache',
                    'priority': 'u=1, i',
                    'referer': 'https://casinoguru-en.com/game-providers',
                    'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36'
                }

                print(f"Fetching game providers - Page: {page_num}")
                response = self.make_request(
                    url,
                    method="POST",
                    headers=headers,
                )
                print("Response status code:", response.status_code)
                tree = html.fromstring(response.text)
                links = tree.xpath('//a[@class="game-item-name"]/@href')

                print(f"Found {len(links)} links on page {page_num}")  # Debug print
                return links

            except Exception as e:
                print(f"Error fetching page {page_num}: {str(e)}")
                return None

        providers = []
        max_workers = 5    # Number of concurrent threads
        current_page = 1
        end_reached = False
        batch_results = {}  # Use dictionary to maintain order

        try:
            while not end_reached:
                print(f"\nProcessing batch starting at page {current_page}")
                batch_results.clear()  # Clear at start of batch

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Create futures for the current batch
                    futures = {
                        executor.submit(fetch_page, page_num): page_num
                        for page_num in range(current_page, current_page + max_workers)
                    }

                    # Process completed futures
                    for future in concurrent.futures.as_completed(futures):
                        page_num = futures[future]
                        try:
                            links = future.result()
                            if links:
                                batch_results[str(page_num)] = links
                                print(f"Successfully processed page {page_num}, found {len(links)} providers")
                            else:
                                end_reached = True
                                print(f"No more providers found after page {page_num-1}")
                        except Exception as e:
                            print(f"Error processing page {page_num}: {str(e)}")
                            end_reached = True
                            break

                # Sort batch results by page number and add to providers
                for page_num in sorted(batch_results.keys()):
                    providers.extend(batch_results[page_num])
                    print(f"Added {len(batch_results[page_num])} providers from page {page_num}")

                print(f"Total providers found so far: {len(providers)}")

                if not end_reached:
                    current_page += max_workers
                    time.sleep(random.uniform(2, 4))  # Delay between batches

        except Exception as e:
            print(f"Unexpected error in get_game_providers: {str(e)}")

        return providers

    def save_providers_to_json(self, providers_data):
        with self.file_lock:
            try:
                current_data = []

                # Load current chunk file if it exists
                if os.path.exists(self.data_file):
                    with open(self.data_file, 'r', encoding='utf-8') as f:
                        try:
                            current_data = json.load(f)
                        except json.JSONDecodeError:
                            current_data = []

                # Add new provider data
                current_data.append(providers_data)

                with open(self.data_file, 'w', encoding='utf-8') as f:
                    json.dump(current_data, f, ensure_ascii=False, indent=2)

            except Exception as e:
                print(f"Error saving to file: {e}")

    def extract_provider_info(self, tree):
        try:
            # Find the main info container
            info_container = tree.xpath('//div[@class="homepage-section"]')
            if not info_container:
                return None

            # Extract all text content from the typography div
            typography_div = info_container[0].xpath('.//div[contains(@class, "typography")]')
            if not typography_div:
                return None

            # Get all text content while preserving structure
            info_sections = {}
            current_section = None

            # Get all direct children of typography div
            elements = typography_div[0].getchildren()

            for element in elements:
                # If we find a heading, start a new section
                if element.tag in ['h3', 'h2']:
                    current_section = element.text.strip()
                    info_sections[current_section] = {
                        'text': [],
                        'note': ""
                    }

                # If we're in a section, process the content
                elif current_section is not None:
                    if element.tag == 'p':
                        # Add text content
                        text = element.text_content().strip()
                        if text:
                            info_sections[current_section]['text'].append(text)

                    elif element.tag == 'div':
                        if element.get('class').lower() == 'note':
                            # Add note content
                            note_content = element.xpath('.//div[@class="note-content"]')
                            note = note_content[0].text_content().strip() if note_content else None
                            if note:
                                info_sections[current_section]['note'] = note.replace("Note: ", "")

            # Clean up sections: Join text arrays and remove empty sections
            cleaned_sections = {}
            for section_name, content in info_sections.items():
                if content['text'] or content['note']:
                    cleaned_sections[section_name] = {
                        'text': '\n'.join(filter(bool, content['text'])),
                        'note': content['note']
                    }

            return cleaned_sections

        except Exception as e:
            print(f"Error extracting provider info: {e}")
            return None

    def get_provider_data(self, html_content):
        try:
            tree = html.fromstring(html_content)

            overview = tree.xpath('//div[@class="game-provider-info-panel"]')
            h5 = overview[0].xpath('.//h5/text()')
            title = h5[0].strip() if h5 else None

            if not title:
                return None # No title found

            src = overview[0].xpath('.//img/@src')
            logo = src[0] if src and not src[0].startswith('data:') else None

            provider_info = self.extract_provider_info(tree)

            provider_data = {
                "title": title,
                "logo": logo,
                "provider_info": provider_info
            }

            return provider_data

        except Exception as e:
            print(f"Error extracting data: {e}")
            return None

    def process_provider(self, url):
        if url in self.processed_urls:
            return None

        try:
            with self.thread_count_lock:
                self.active_threads += 1

            response = self.make_request(url)
            provider_data = self.get_provider_data(response.text)

            if provider_data:
                with self.data_lock:
                    self.providers_processed += 1
                    url_number = self.providers_processed + len(self.processed_urls)
                    print(f"\nProcessing URL number: {url_number}")
                    print(f"Found data for: {provider_data['title']}")
                    print(f"Processed providers: {self.providers_processed}")
                    print(f"Total requests: {self.total_requests}")
                    print(f"Active threads: {self.active_threads}")

                self.save_providers_to_json(provider_data)
                self.save_processed_url(url)

            return True

        except Exception as e:
            print(f"Error processing provider {url}: {e}")
            return None
        finally:
            with self.thread_count_lock:
                self.active_threads -= 1

    def start(self):
        try:
            all_urls = self.get_game_providers()

            # Remove already processed URLs
            providers_to_process = [url for url in all_urls if url not in self.processed_urls]

            print(f"\nKonfiguracja scrapera:")
            print(f"Znaleziono łącznie URL-i: {len(all_urls)}")
            print(f"URL-i do przetworzenia: {len(providers_to_process)}")
            print(f"Już przetworzonych: {len(self.processed_urls)}")

            if providers_to_process:
                print("\nStarting threads...")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = [executor.submit(self.process_provider, provider) for provider in providers_to_process]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            result = future.result()
                        except Exception as e:
                            print(f"Thread error: {e}")
                print(f"\nZakończono pobieranie. Łącznie przetworzono {self.providers_processed} nowych gier.")
            else:
                print("Brak nowych URL-i do przetworzenia.")
        except Exception as e:
            print(f"Error starting scraper: {e}")

if __name__ == "__main__":
    scraper = GameProviderScraper()
    scraper.start()