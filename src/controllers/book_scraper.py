from decimal import Decimal
import logging
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from word2number import w2n

logger = logging.getLogger(__name__)

class BookScraper:
    def __init__(self, base_url):
        self.base_url = base_url
        self.catalogue_url = urljoin(base_url, "catalogue/")

    async def get_all_book_urls(self, session):
        """
        Asynchronously fetch all book URLs from all pages
        """
        book_urls = []
        page_num = 1
        while True:
            page_url = f"{self.catalogue_url}page-{page_num}.html"
            try:
                async with session.get(page_url) as response:
                    if response.status == 404:
                        break
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find all book links on the current page
                    book_containers = soup.select('.product_pod')
                    if not book_containers:
                        break
                    
                    # Extract and store book URLs
                    for book in book_containers:
                        book_url = book.select_one('h3 a')['href']
                        if book_url:
                            full_url = urljoin(self.catalogue_url, book_url)
                            book_urls.append(full_url)
                    
                    logger.info(f"Collected URLs from page {page_num}")
                    page_num += 1
            
            except Exception as e:
                logger.error(f"Error fetching page {page_num}: {e}")
                break

        logger.info(f"Total books found: {len(book_urls)}")
        return book_urls

    async def extract_one_book_info(self, session, book_url):
        """
        Asynchronously extract information from a single book page
        """
        try:
            async with session.get(book_url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                try:
                    book_page = soup.select_one('.product_main')
                    if not book_page:
                        raise ValueError("Could not find product main section")
                    
                    # Extract book information
                    title = book_page.select_one('h1').text.strip()
                    price = Decimal(self._extract_numbers(book_page.select_one('p.price_color').text.strip())[0])
                    rating = book_page.select_one('p.star-rating')['class'][1]
                    rating = w2n.word_to_num(rating)
                    category = soup.select('.breadcrumb li')[2].text.strip()
                    image_url = urljoin(self.base_url, soup.select_one('.item.active img')['src'])
                    upc = soup.select_one("td").text.strip()
                    
                    # Extract availability and stock
                    availability = book_page.select_one('.availability').text.strip()
                    units_number = self._extract_numbers(availability)[0]
                    stock = int(units_number) if units_number else 0
                    
                    # Extract description
                    description = ''
                    product_description = soup.select_one('#product_description')
                    if product_description:
                        description = product_description.find_next_sibling('p').text.strip()
                    
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
                    return book_info
                    
                except (AttributeError, IndexError, ValueError) as e:
                    logger.error(f"Error parsing book data at {book_url}: {str(e)}")
                    return None
                
        except Exception as e:
            logger.error(f"Error fetching book page {book_url}: {str(e)}")
            return None
        
    def _extract_numbers(self, s: str) -> list[str]:
        return re.findall(r"\d+\.\d+|\d+", s)  # Finds decimal numbers first, then integers
