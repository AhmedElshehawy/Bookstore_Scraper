import re
from typing import Generator
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from decimal import Decimal
import logging
from word2number import w2n

logger = logging.getLogger(__name__)

class BookScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def get_soup(self, url: str) -> BeautifulSoup:
        """
        Get the soup object from the url
        """
        try:
            logger.debug(f"Fetching URL: {url}")
            response = requests.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching URL {url}: {e}", exc_info=True)
            raise e
        
    def extract_one_book_info(self, book_url: str) -> dict:
        """
        Extract the book info from the book url
        """
        try:
            logger.info(f"Extracting book information from: {book_url}")
            soup = self.get_soup(book_url)
            book_page = soup.select_one('.product_main')
            if not book_page:
                logger.error(f"Book page structure not found at {book_url}")
                raise ValueError("Book page not found")
            
            title = book_page.select_one('h1').text.strip()
            price = Decimal(self._extract_numbers(book_page.select_one('.price_color').text.strip())[0])
            rating = book_page.select_one('p.star-rating').attrs['class'][1]
            rating = w2n.word_to_num(rating)
            description = soup.select_one('#product_description ~ p').text.strip()
            category = soup.select('.breadcrumb li')[2].text.strip()
            image_url = urljoin(self.base_url, soup.select_one('.item.active img')['src'])
            upc = soup.select_one("td").text.strip()

            availability = book_page.select_one('.availability').text.strip()
            untis_number = self._extract_numbers(availability)[0]
            if untis_number:
                stock = int(untis_number)
            else:
                stock = 0
            
            book_info = { ## TODO: use Pydantic later on
                'title': title,
                'price': price,
                'rating': rating,
                'description': description,
                'category': category,
                'upc': upc,
                'num_available_units': stock,
                'image_url': image_url,
                'book_url': book_url
            }
            logger.debug(f"Successfully extracted book info: {title}")
            return book_info
        except (KeyError, AttributeError) as e:
            logger.error(f"Error extracting book info from {book_url}: {e}", exc_info=True)
            raise e
    
    def get_all_book_urls(self) -> Generator[str, None, None]:
        """
        Get all the book urls from the base url
        """
        page_number = 1
        while True:
            page_url = f"{self.base_url}catalogue/page-{page_number}.html"
            try:
                logger.info(f"Fetching book list from page {page_number}")
                soup = self.get_soup(page_url)
                book_containers = soup.select('.product_pod')
                if not book_containers:
                    logger.info("No more books found. Stopping pagination.")
                    break
                for book_container in book_containers:
                    book_url = f"{self.base_url}/catalogue/{book_container.select_one('h3 a')['href']}"
                    logger.debug(f"Found book URL: {book_url}")
                    yield book_url
                page_number += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching page {page_url}: {e}", exc_info=True)
                break
    
    def _extract_numbers(self,s: str) -> list[str]:
        return re.findall(r"\d+\.\d+|\d+", s)  # Finds decimal numbers first, then integers

