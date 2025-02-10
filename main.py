import json
import logging
import asyncio
import os
import aiohttp
from typing import List, Tuple, Dict, Any
from .book_scraper import BookScraper
from .book_model import Book

"""
Main module for asynchronous book scraping and database upsert operations.

This module performs the following tasks:
1. Retrieves all book URLs using a BookScraper instance.
2. Processes each book URL concurrently to extract book information.
3. Upserts the successfully scraped book data into a database in batches.
4. Handles concurrent database operations using an asyncio semaphore.
5. Provides an AWS Lambda compatible handler via the lambda_handler function.
"""

# Configure logging to include the timestamp, module name, log level, and message.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = os.getenv("BASE_URL")  # The base URL used for scraping book information.
DB_URL_UPSERT_BATCH = os.getenv("DB_URL_UPSERT_BATCH")  # Endpoint for batch upsert operations in the database.
CONCURRENT_DB_OPS = int(os.getenv("CONCURRENT_DB_OPS", "5"))  # Maximum number of concurrent database operations.
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "25"))  # Maximum number of books to process in a single database upsert batch.

async def process_book(session: aiohttp.ClientSession, book_url: str, scraper: BookScraper) -> Tuple[Book, str]:
    """
    Process a single book URL using the provided scraper.

    This function fetches and extracts book information asynchronously. If extraction is successful,
    it returns a tuple where the first element is the Book object and the second element is None.
    In case of an error, the function logs the error and returns a tuple with None for the Book
    and the book URL to indicate failure.

    Parameters:
      - session (aiohttp.ClientSession): The HTTP session for making requests.
      - book_url (str): The URL of the book to be processed.
      - scraper (BookScraper): An instance of BookScraper to extract book information.

    Returns:
      - Tuple[Book, str]: A tuple where:
          * The first element is the Book object (or None if an error occurred).
          * The second element is an error indicator (None if processing was successful).
    """
    try:
        book_info = await scraper.extract_one_book_info(session, book_url)
        logger.debug(f"Processed book {book_url}: {book_info.title}")
        return book_info, None
    except Exception as e:
        logger.error(f"Failed to process book {book_url}: {e}")
        return None, book_url

async def process_books_batch(session: aiohttp.ClientSession, book_urls: List[str], scraper: BookScraper) -> Tuple[List[Book], List[str]]:
    """
    Process a batch of book URLs concurrently.

    This function leverages asynchronous tasks to process a list of book URLs in parallel.
    It aggregates the results into two lists: one containing all successfully processed Book objects,
    and another containing the URLs that encountered errors during processing.

    Parameters:
      - session (aiohttp.ClientSession): The HTTP session used to perform the requests.
      - book_urls (List[str]): A list of book URLs to be processed.
      - scraper (BookScraper): An instance of BookScraper used for extracting book details.

    Returns:
      - Tuple[List[Book], List[str]]: A tuple where the first element is a list of Book objects that were
        processed successfully and the second is a list of URLs that failed during processing.
    """
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

async def upsert_books_batch(session: aiohttp.ClientSession, books: List[Book]) -> Dict[str, Any]:
    """
    Upsert books into the database in concurrent batches.

    This function divides the list of books into smaller batches, each containing a maximum of BATCH_SIZE items.
    Each batch is then upserted to the database concurrently, constrained by a semaphore that limits
    the number of concurrent database operations to CONCURRENT_DB_OPS. Each Book object is first converted
    to a dictionary form via its model_dump_json() method.

    In case an upsert operation for a batch fails (i.e., response.status != 200), corresponding error
    messages are aggregated for each book in the batch.

    Parameters:
      - session (aiohttp.ClientSession): The HTTP session used for sending requests to the database endpoint.
      - books (List[Book]): A list of Book objects to upsert into the database.

    Returns:
      - Dict[str, Any]: A dictionary containing:
          * 'processed': The total number of successfully upserted books.
          * 'errors': A list of error dictionaries with details on upsert failures.
    """
    db_status = {'processed': 0, 'errors': []}
    
    # Partition books into batches according to BATCH_SIZE.
    batches = [books[i:i+BATCH_SIZE] for i in range(0, len(books), BATCH_SIZE)]
    semaphore = asyncio.Semaphore(CONCURRENT_DB_OPS)
    
    async def upsert_batch(batch: List[Book]) -> Tuple[int, List[Dict]]:
        """
        Helper function to upsert a single batch of books.

        Parameters:
          - batch (List[Book]): A subset of books for the current upsert operation.

        Returns:
          - Tuple[int, List[Dict]]: A tuple where:
              * The first element is the count of books successfully processed in this batch.
              * The second element is a list of error details (if any) encountered during this batch operation.
        """
        # Convert each Book to its dictionary representation.
        payload = [json.loads(book.model_dump_json()) for book in batch]
        async with semaphore:
            try:
                async with session.post(DB_URL_UPSERT_BATCH, json=payload) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        errors = [{'book_url': b.get('book_url', 'unknown'), 'error': error_text} for b in payload]
                        return 0, errors
                    else:
                        return len(batch), []
            except Exception as e:
                errors = [
                    {'book_url': json.loads(book.model_dump_json()).get('book_url', 'unknown'), 'error': str(e)} 
                    for book in batch
                ]
                return 0, errors
    
    tasks = [upsert_batch(batch) for batch in batches]
    results = await asyncio.gather(*tasks)
    
    for processed, errors in results:
        db_status['processed'] += processed
        db_status['errors'].extend(errors)
        
    return db_status

async def main() -> Dict:
    """
    Main asynchronous function that orchestrates book scraping and database operations.

    The function performs the following steps:
      1. Creates an aiohttp session.
      2. Instantiates a BookScraper using BASE_URL and retrieves all book URLs.
      3. Processes the list of book URLs concurrently to extract book information.
      4. Upserts the successfully scraped books into the database in batches.
      5. Logs a summary of the process, including successes and failures.

    If any unexpected error occurs, it is caught and logged, and the resulting status reflects the failure.

    Returns:
      - Dict: A dictionary resembling an HTTP response with:
          * 'statusCode': 200 for success, 500 for failures.
          * 'body': A JSON string detailing:
              - processed_books: Count of successfully processed books.
              - failed_books: Count of books that failed processing.
              - db_status: The upsert status from the database operation.
    """
    db_status = {'processed': 0, 'errors': [], 'success': True}
    
    async with aiohttp.ClientSession() as session:
        try:
            # Initialize the scraper and retrieve all book URLs.
            scraper = BookScraper(BASE_URL)
            book_urls = await scraper.get_all_book_urls(session)
            
            # Concurrently process each book URL.
            all_scraped_books, all_failed_books = await process_books_batch(session, book_urls, scraper)
            
            # Upsert the successfully scraped books, if any.
            if all_scraped_books:
                db_result = await upsert_books_batch(session, all_scraped_books)
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
            'processed_books': len(all_scraped_books) if 'all_scraped_books' in locals() else 0,
            'failed_books': len(all_failed_books) if 'all_failed_books' in locals() else 0,
            'db_status': db_status,
        })
    }

def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    AWS Lambda handler function.

    This function acts as the entry point for AWS Lambda execution. It triggers the execution
    of the main asynchronous function using asyncio.run and returns its outcome.

    Parameters:
      - event (Dict): The event payload provided by AWS Lambda.
      - context (Any): The runtime information provided by AWS Lambda.

    Returns:
      - Dict: A dictionary representing the result of the main function, following an HTTP-like response structure.
    """
    return asyncio.run(main())
