import json
import logging
import asyncio
import aiohttp
from book_scraper import BookScraper
import time
from database_model import DatabaseHandler


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
        logger.info(f"Processed book {book_url}: {book_info.title}")
        return book_info, None
    except Exception as e:
        logger.error(f"Failed to process book {book_url}: {e}")
        return None, book_url

async def main(event, context):
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
        scraped_books = []
        
        for result, failed_url in results:
            if result:
                processed_books += 1
                scraped_books.append(result)
            if failed_url:
                failed_books.append(failed_url)
        
        end_time = time.time()
        scrape_time = end_time - start_time
        logger.info(f"Processing completed. Processed {processed_books} books, {len(failed_books)} failed.")
        logger.info(f"Failed books: {failed_books}")
        logger.info(f"Total time taken: {scrape_time:.2f} seconds")
        
    # Save the results to a Database
    start_time = time.time()
    db_handler = DatabaseHandler()
    db_status = {
        'processed': 0,
        'errors': [],
        'success': True,
        'execution_time': 0
    }
    try:
        db_handler.connect()
        
        for book in scraped_books:
            try:
                db_handler.process_book(book)
                db_status['processed'] += 1
                
            except Exception as e:
                db_status['errors'].append({
                    'book_url': book.get('book_url'),
                    'error': str(e)
                })
        end_time = time.time()
        logger.info(f"Database processing completed. Processed {db_status['processed']} books, {len(db_status['errors'])} failed.")
        logger.info(f"Failed books: {db_status['errors']}")
        logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
        db_status['execution_time'] = end_time - start_time
    except Exception as e:
        db_status['success'] = False
        db_status['errors'].append({'error': str(e)})
    finally:
        db_handler.close()
    
    # Prepare Lambda response
    response = {
        'statusCode': 200 if db_status['success'] else 500,
        'body': json.dumps({
            'processed_books': processed_books,
            'failed_books': len(failed_books),
            'scrape_time': scrape_time,
            'db_status': db_status,
        })
    }
    return response

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    return asyncio.run(main(event, context))
