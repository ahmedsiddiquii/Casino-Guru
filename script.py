import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import xml.etree.ElementTree as ET
from lxml import html
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import os
import random


def normalize_key(key):
    key = key.lower().replace(' ', '_')
    return key


def normalize_to_slug(text):
    text = text.strip().lower()
    text = text.replace(' ', '-')
    return text


class GameScraper:
    def __init__(self, start_from_url=12000):
        self.start_from_url = start_from_url
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

        self.setup_session()
        self.games_processed = 0
        self.total_requests = 0
        self.data_lock = threading.Lock()
        self.file_lock = threading.Lock()
        self.session_lock = threading.Lock()
        self.thread_count_lock = threading.Lock()
        self.active_threads = 0
        self.processed_urls_file = 'processed_urls.txt'
        self.data_file = 'data_3.json'

        # Upewniamy się, że pliki istnieją
        for file in [self.processed_urls_file, self.data_file]:
            if not os.path.exists(file):
                with open(file, 'w', encoding='utf-8') as f:
                    if file == self.data_file:
                        json.dump([], f)

    def setup_session(self):
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"]
        )

        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=10)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.headers = random.choice(self.headers_pool)

    def get_url_number(self, url):
        try:
            # Extract the last part of the URL and convert to integer
            parts = url.rstrip('/').split('/')
            for part in reversed(parts):
                if part.isdigit():
                    return int(part)
            return 0
        except (ValueError, IndexError):
            print(f"Error extracting number from URL: {url}")
            return 0

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

    def make_request(self, url, max_retries=3):
        for attempt in range(max_retries):
            try:
                time.sleep(random.uniform(2, 5))

                with self.session_lock:
                    self.session.headers = random.choice(self.headers_pool)

                    if self.total_requests > 0 and self.total_requests % 100 == 0:
                        print("\nRefreshing session...")
                        self.session.close()
                        self.setup_session()
                        print("Session refreshed")

                    response = self.session.get(
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

    def save_game_to_json(self, game_data):
        with self.file_lock:
            try:
                current_data = []
                if os.path.exists(self.data_file):
                    with open(self.data_file, 'r', encoding='utf-8') as f:
                        try:
                            current_data = json.load(f)
                        except json.JSONDecodeError:
                            current_data = []

                current_data.append(game_data)

                with open(self.data_file, 'w', encoding='utf-8') as f:
                    json.dump(current_data, f, ensure_ascii=False, indent=2)

            except Exception as e:
                print(f"Error saving to file: {e}")

    def validate_game_data(self, game_data):
        required_fields = ['title', 'game_provider', 'game_src']
        return all(field in game_data and game_data[field] is not None for field in required_fields)

    def extract_game_review(self, tree):
        try:
            # Find the main review container
            review_container = tree.xpath('//div[@class="col-game-review-pad"]')
            if not review_container:
                return None

            # Extract all text content from the typography div
            typography_div = review_container[0].xpath('.//div[@class="typography"]')
            if not typography_div:
                return None

            # Get all text content while preserving structure
            review_sections = {}
            current_section = None

            # Get all direct children of typography div
            elements = typography_div[0].getchildren()

            for element in elements:
                # If we find a heading, start a new section
                if element.tag == 'h3':
                    current_section = element.text.strip()
                    review_sections[current_section] = {
                        'text': [],
                        'media': []
                    }

                # If we're in a section, process the content
                elif current_section is not None:
                    if element.tag == 'p':
                        # Add text content
                        text = element.text_content().strip()
                        if text:
                            review_sections[current_section]['text'].append(text)

                    elif element.tag == 'figure':
                        print("figure element found")
                        # Handle images
                        images = element.xpath('./div/a')
                        for img in images:
                            if img.get('href'):
                                media_item = {
                                    'type': 'image',
                                    'url': img.get('href'),
                                    'alt': img.get('title', ''),
                                    'caption': ''
                                }

                                # Get caption if exists
                                caption = element.xpath('./figcaption/text()')
                                if caption:
                                    media_item['caption'] = caption[0].strip()

                                review_sections[current_section]['media'].append(media_item)

                        # Handle videos
                        videos = element.xpath('./div/iframe')
                        for video in videos:
                            print("Video attributes:", video.attrib)
                            # Try getting src attribute in different ways
                            video_src = video.get('src') or video.get('data-src')

                            print(f"video source: {video_src}")  # Debug print
                            if video_src:
                                media_item = {
                                    'type': 'video',
                                    'url': video_src,
                                    'caption': ''
                                }

                                # Get caption if exists
                                caption = element.xpath('./figcaption/text()')
                                if caption:
                                    media_item['caption'] = caption[0].strip()

                                review_sections[current_section]['media'].append(media_item)

            # Clean up sections: Join text arrays and remove empty sections
            cleaned_sections = {}
            for section_name, content in review_sections.items():
                if content['text'] or content['media']:
                    cleaned_sections[section_name] = {
                        'text': '\n'.join(content['text']),
                        'media': content['media']
                    }

            return cleaned_sections

        except Exception as e:
            print(f"Error extracting game review: {e}")
            return None

    def get_game_data(self, html_content):
        try:
            tree = html.fromstring(html_content)

            body = tree.xpath('//div[@class="col-game-review-pad"]')
            overview = tree.xpath('//div[@class="game-detail-main-overview"]')
            h2 = overview[0].xpath('./h2/text()')
            title = h2[0].strip() if h2 else None
            # if title:
            #     title = title.replace("Play in Demo Mode", "").strip()

            game_src = None
            embed_button = tree.xpath('//div[@id="game-embed-button"]/@data-embed-content')
            if embed_button:
                game_src = embed_button[0]

            images = []
            imgs = tree.xpath('//div[@class="section-game-review js-section-game-review"]//img/@src')
            if imgs:
                images = [src for src in imgs if src and not src.startswith('data:')]

            game_tags = []
            tags = tree.xpath('//div[@class="game-detail-main-themes-wrapper"]/a')
            for tag in tags:
                try:
                    title_tag = tag.text_content().strip()
                    game_tags.append({
                        "title": title_tag,
                        "slug": normalize_to_slug(title_tag)
                    })
                except Exception as e:
                    print(f"Error processing tag: {e}")
                    continue

            rating = tree.xpath(
                '//span[contains(@class, "game-detail-main-quick-verdict-heading-score-number")]/text()')
            rating_value = rating[0].strip() if rating else None

            thumbnail = tree.xpath('//div[@class="game-detail-main-info"]//img/@src')
            thumbnail_url = thumbnail[0] if thumbnail else None

            provider_name = tree.xpath('//div[@class="game-provider-info-panel"]//h5/text()')
            provider_info = {}
            if provider_name:
                provider_name = provider_name[0].strip()
                provider_info = {
                    "title": provider_name,
                    "slug": normalize_to_slug(provider_name)
                }

            game_stats = {}
            stats_cards = tree.xpath('//div[@class="stats-cards"]/div[@class="stats-card stats-card-dark"]')

            for card in stats_cards:
                try:
                    label = card.xpath('.//label/text()')[0].strip().lower().replace(' ', '_')
                    if label != 'max_win':
                        value = card.xpath('.//div[@class="flex items-center"]/b/text()')[0].strip()
                        game_stats[label] = value
                except Exception as e:
                    print(f"Error processing stats card: {e}")
                    continue

            rows = tree.xpath('//table/tbody/tr')
            game_informations = {}

            for row in rows:
                try:
                    name_elements = row.xpath('.//td[1]/text()')
                    if not name_elements:
                        continue
                    name = name_elements[0].strip()
                    name = normalize_key(name)

                    span_classes = row.xpath('.//td[2]/span/@class')
                    if span_classes:
                        value = True if 'bullet-green' in span_classes[0] else False
                    else:
                        value_text = row.xpath('.//td[2]/text()')
                        value = value_text[0].strip() if value_text else None

                    if name:
                        game_informations[name] = value

                except Exception as e:
                    print(f"Error processing table row: {e}")
                    continue

            about = tree.xpath('//div[@class="game-detail-main-about"]//p')
            game_review = self.extract_game_review(tree)
            game_data = {
                "title": title,
                "thumbnail": thumbnail_url,
                "rating": float(rating_value) if rating_value else None,
                "images": images,
                "overview": overview[0].text_content().strip() if overview else None,
                "about_game": about[0].text_content().strip() if about else None,
                "game_informations": game_informations,
                "game_provider": provider_info,
                "game_stats": game_stats,
                "game_tags": game_tags,
                "game_src": game_src,
                "game_review": game_review
            }

            return game_data if self.validate_game_data(game_data) else None

        except Exception as e:
            print(f"Error extracting data: {e}")
            return None

    def process_game(self, url):
        if url in self.processed_urls:
            return None

        try:
            with self.thread_count_lock:
                self.active_threads += 1

            response = self.make_request(url)
            game_data = self.get_game_data(response.text)

            if game_data:
                with self.data_lock:
                    self.games_processed += 1
                    url_number = self.get_url_number(url)
                    print(f"\nProcessing URL number: {url_number}")
                    print(f"Found data for: {game_data['title']}")
                    print(f"Processed games: {self.games_processed}")
                    print(f"Total requests: {self.total_requests}")
                    print(f"Active threads: {self.active_threads}")

                self.save_game_to_json(game_data)
                self.save_processed_url(url)

            return game_data

        except Exception as e:
            print(f"Error processing game {url}: {e}")
            return None
        finally:
            with self.thread_count_lock:
                self.active_threads -= 1

    def analyze_sitemap(self, sitemap_url, max_workers=3):
        try:
            self.processed_urls = self.get_processed_urls()

            response = self.make_request(sitemap_url)
            print("Response status:", response.status_code)
            root = ET.fromstring(response.content)
            namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            # Get all URLs from sitemap
            all_urls = [loc.text for loc in root.findall('.//ns:loc', namespace)]
            print(f"\nZnaleziono łącznie URL-i: {len(all_urls)}")

            # Starting from index 10460
            urls_to_process = all_urls[:15]

            # Remove already processed URLs
            locations_to_process = [url for url in urls_to_process if url not in self.processed_urls]

            print(f"\nKonfiguracja scrapera:")
            print(f"Całkowita liczba URL-i: {len(all_urls)}")
            print(f"Pomijam pierwszych {self.start_from_url} URL-i")
            print(f"URL-i do przetworzenia: {len(urls_to_process)}")
            print(f"Już przetworzonych: {len(self.processed_urls)}")
            print(f"Pozostało do przetworzenia: {len(locations_to_process)}")

            if locations_to_process:
                print(f"\nPierwsze 5 URL-i do przetworzenia:")
                for url in locations_to_process[:5]:
                    print(f"- {url}")

            if locations_to_process:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(self.process_game, url) for url in locations_to_process]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            result = future.result()
                        except Exception as e:
                            print(f"Thread error: {e}")

            print(f"\nZakończono pobieranie. Łącznie przetworzono {self.games_processed} nowych gier.")

        except Exception as e:
            print(f"An error occurred: {e}")


if __name__ == "__main__":
    sitemap_url = "https://casino.guru/games-sitemap.xml"
    scraper = GameScraper(start_from_url=14200)  # Ustawienie początkowego URL-a
    scraper.analyze_sitemap(sitemap_url)