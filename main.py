import json
import logging
import asyncio
import os
import aiohttp
from typing import List, Tuple, Dict, Any
from .book_scraper import BookScraper
from .book_model import Book

# Add logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = os.getenv("BASE_URL")
DB_URL_UPSERT = os.getenv("DB_URL_UPSERT")
CONCURRENT_DB_OPS = int(os.getenv("CONCURRENT_DB_OPS", "10"))

async def process_book(session: aiohttp.ClientSession, book_url: str, scraper: BookScraper) -> Tuple[Dict, str]:
    """Process a single book URL and return the book info or error."""
    try:
        book_info = await scraper.extract_one_book_info(session, book_url)
        logger.debug(f"Processed book {book_url}: {book_info.title}")
        return book_info, None
    except Exception as e:
        logger.error(f"Failed to process book {book_url}: {e}")
        return None, book_url

async def upsert_book(session: aiohttp.ClientSession, book: Book) -> Dict[str, Any]:
    """Upsert a single book to database."""
    try:
        book_info = json.loads(book.model_dump_json())
        async with session.post(DB_URL_UPSERT, json=book_info) as response:
            if response.status != 200:
                raise Exception(f"Failed to upsert book: {await response.text()}")
            return {'processed': 1, 'error': None}
    except Exception as e:
        return {
            'processed': 0,
            'error': {'book_url': book_info.get('book_url'), 'error': str(e)}
        }

async def process_books_batch(session: aiohttp.ClientSession, book_urls: List[str], scraper: BookScraper) -> Tuple[List, List]:
    """Process a batch of book URLs concurrently."""
    tasks = [process_book(session, url, scraper) for url in book_urls]
    results = await asyncio.gather(*tasks)
    
    successful_books = []
    failed_urls = []
    
    for result, failed_url in results:
        if result:
            successful_books.append(result)
        if failed_url:
            failed_urls.append(failed_url)
            
    return successful_books, failed_urls

async def upsert_books_concurrent(session: aiohttp.ClientSession, books: List[Dict]) -> Dict[str, Any]:
    """Upsert books concurrently with rate limiting."""
    db_status = {'processed': 0, 'errors': []}
    
    # Process books in chunks to avoid overwhelming the database
    semaphore = asyncio.Semaphore(CONCURRENT_DB_OPS)
    
    async def upsert_with_semaphore(book):
        async with semaphore:
            return await upsert_book(session, book)
    
    tasks = [upsert_with_semaphore(book) for book in books]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        db_status['processed'] += result['processed']
        if result.get('error'):
            db_status['errors'].append(result['error'])
    
    return db_status

async def main() -> Dict:
    """Main function to orchestrate the scraping and database operations."""
    db_status = {'processed': 0, 'errors': [], 'success': True}
    
    async with aiohttp.ClientSession() as session:
        try:
            # Initialize scraper and get all book URLs
            scraper = BookScraper(BASE_URL)
            book_urls = await scraper.get_all_book_urls(session)
            
            # Process all books
            all_scraped_books, all_failed_books = await process_books_batch(session, book_urls, scraper)
            
            # Upload to database concurrently
            if all_scraped_books:
                db_result = await upsert_books_concurrent(session, all_scraped_books)
                db_status.update(db_result)
            
            logger.info(f"Processing completed. Processed {len(all_scraped_books)} books, {len(all_failed_books)} failed.")
            if all_failed_books:
                logger.info(f"Failed books: {all_failed_books}")
            logger.info(f"Database uploads: {db_status['processed']} successful, {len(db_status['errors'])} failed.")
            
        except Exception as e:
            db_status['success'] = False
            db_status['errors'].append({'error': str(e)})
            logger.error(f"Main process failed: {e}")
    
    return {
        'statusCode': 200 if db_status['success'] else 500,
        'body': json.dumps({
            'processed_books': len(all_scraped_books),
            'failed_books': len(all_failed_books),
            'db_status': db_status,
        })
    }

def lambda_handler(event: Dict, context: Any) -> Dict:
    """AWS Lambda handler function"""
    return asyncio.run(main())
