"""
This module provides functionality to scrape book data from an online catalogue.
It uses asynchronous requests along with BeautifulSoup to extract information about each book,
such as title, price, rating, category, UPC, availability, and description.

Classes:
    BookScraper: A scraper that collects all book URLs from the catalogue and extracts information
                 from individual book pages.
                 
Dependencies:
    - asyncio-compatible HTTP session library (e.g., aiohttp)
    - BeautifulSoup for HTML parsing
    - word2number for converting word numbers to numeric values
    - pydantic for validating and constructing URL objects
    - decimal for handling prices as Decimal values
"""

from decimal import Decimal
import logging
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from pydantic import HttpUrl
from word2number import w2n
from book_model import Book

logger = logging.getLogger(__name__)

class BookScraper:
    """
    A scraper that fetches book URLs from a catalogue and extracts detailed information from each book's page.

    Attributes:
        base_url (str): The base URL of the website.
        catalogue_url (str): The full URL to the catalogue, built from the base URL.
    """
    def __init__(self, base_url: str):
        """
        Initialize the BookScraper with the given base URL.

        Args:
            base_url (str): The base URL of the site to scrape. This URL is used to
                            construct URLs for the book catalogue and individual book pages.
        """
        self.base_url = base_url
        self.catalogue_url = urljoin(base_url, "catalogue/")

    async def get_all_book_urls(self, session) -> list[str]:
        """
        Asynchronously fetch and return all book URLs from the paginated catalogue.

        The method iterates over catalogue pages by incrementing the page number until a page returns a 404 status
        or no book containers are found in the HTML response. It extracts book URLs found in the '.product_pod' elements.

        Args:
            session (aiohttp.ClientSession): An asynchronous HTTP session used to make GET requests.

        Returns:
            list[str]: A list of fully-qualified URLs pointing to individual book pages.

        Raises:
            Logs any exceptions that occur while fetching pages without throwing them onward.
        """
        book_urls = []
        page_num = 1
        while True:
            page_url = f"{self.catalogue_url}page-{page_num}.html"
            try:
                async with session.get(page_url) as response:
                    if response.status == 404:
                        logger.info(f"Page {page_num} not found (404). Ending catalogue traversal.")
                        break
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find all book containers on the current page
                    book_containers = soup.select('.product_pod')
                    if not book_containers:
                        logger.info(f"No book containers found on page {page_num}. Ending catalogue traversal.")
                        break
                    
                    # Extract and store full book URLs
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

    async def extract_one_book_info(self, session, book_url: str) -> Book | None:
        """
        Asynchronously extract detailed book information from a single book page.

        This method makes an asynchronous GET request to the book page URL and uses BeautifulSoup to parse the 
        returned HTML. It extracts various details:
           - Title, Price, and Rating (converted from words to numbers)
           - Category (from the breadcrumb navigation)
           - UPC (unique product code)
           - Availability details (including stock count)
           - Book description (if available)
           - Image URL
        All extracted data is then used to construct and return a `Book` instance.

        Args:
            session (aiohttp.ClientSession): An asynchronous HTTP session for making GET requests.
            book_url (str): The URL of the book page to extract details from.

        Returns:
            Book | None: A Book object with the extracted details if parsing is successful; otherwise, None.

        Raises:
            Logs parsing or HTTP errors without propagating exceptions.
        """
        try:
            async with session.get(book_url) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                try:
                    book_page = soup.select_one('.product_main')
                    if not book_page:
                        raise ValueError("Could not find product main section")
                    
                    # Extract title from the book page
                    title = book_page.select_one('h1').text.strip()
                    
                    # Extract price and convert it to a Decimal
                    price_text = book_page.select_one('p.price_color').text.strip()
                    price_number = self._extract_numbers(price_text)[0]
                    price = Decimal(price_number)
                    
                    # Extract rating from the class (e.g., "star-rating Three")
                    rating_class = book_page.select_one('p.star-rating')['class'][1]
                    rating = w2n.word_to_num(rating_class)
                    
                    # Extract category from the breadcrumb navigation
                    category = soup.select('.breadcrumb li')[2].text.strip()
                    
                    # Extract image URL and validate it as an HttpUrl using pydantic
                    image_src = soup.select_one('.item.active img')['src']
                    image_url = HttpUrl(urljoin(self.base_url, image_src))
                    
                    # Extract UPC from the first table cell found
                    upc = soup.select_one("td").text.strip()
                    
                    # Extract availability details and determine the number of available units
                    availability = book_page.select_one('.availability').text.strip()
                    units_number = self._extract_numbers(availability)[0]
                    stock = int(units_number) if units_number else 0
                    
                    # Extract product description if available
                    description = ''
                    product_description = soup.select_one('#product_description')
                    if product_description:
                        description = product_description.find_next_sibling('p').text.strip()
                    
                    # Create and return a Book instance with the scraped values
                    book_info = Book(
                        title=title,
                        price=price,
                        rating=rating,
                        description=description,
                        category=category,
                        upc=upc,
                        num_available_units=stock,
                        image_url=image_url,
                        book_url=HttpUrl(book_url)
                    )
                    return book_info
                    
                except (AttributeError, IndexError, ValueError) as e:
                    logger.error(f"Error parsing book data at {book_url}: {str(e)}")
                    return None
                
        except Exception as e:
            logger.error(f"Error fetching book page {book_url}: {str(e)}")
            return None
        
    def _extract_numbers(self, s: str) -> list[str]:
        """
        Extracts all numeric values (integers and decimals) from a given string.

        The method uses a regular expression that first looks for decimal numbers then integers. This is useful
        for extracting price values and stock numbers from strings.

        Args:
            s (str): The input string potentially containing numbers.

        Returns:
            list[str]: A list of string representations of the numbers found in the input string.
        """
        return re.findall(r"\d+\.\d+|\d+", s)
