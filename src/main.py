import json
import logging
import asyncio
import aiohttp
from controllers import BookScraper
import time

start_time = time.time()

# Add logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

base_url = "https://books.toscrape.com/"

async def process_book(session, book_url, scraper):
    try:
        book_info = await scraper.extract_one_book_info(session, book_url)
        logger.info(f"Processed book {book_url}: {book_info['title']}\n--------------------------------")
        return book_info, None
    except Exception as e:
        logger.error(f"Failed to process book {book_url}: {e}")
        return None, book_url

async def main():
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session:
        scraper = BookScraper(base_url)
        book_urls = await scraper.get_all_book_urls(session)
        
        # Create tasks for all books
        tasks = [process_book(session, book_url, scraper) for book_url in book_urls]
        
        # Run tasks concurrently
        results = await asyncio.gather(*tasks)
        
        # Process results
        processed_books = 0
        failed_books = []
        
        for result, failed_url in results:
            if result:
                processed_books += 1
            if failed_url:
                failed_books.append(failed_url)
        
        end_time = time.time()
        logger.info(f"Processing completed. Processed {processed_books} books, {len(failed_books)} failed.")
        logger.info(f"Failed books: {failed_books}")
        logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
