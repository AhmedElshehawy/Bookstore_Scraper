# Bookstore_Scraper

You can try the chatbot [here](https://huggingface.co/spaces/elshehawy/BookBot)

## How It Fits Within the Bookstore Project

This repository is one of four interconnected components within the overall Bookstore project:

- [**Bookstore Assistant**](https://github.com/AhmedElshehawy/Bookstore-Assistant):  
  Manages the chat interface, processes natural language queries, and interacts with other parts of the system.

- [**Bookstore DB**](https://github.com/AhmedElshehawy/Bookstore-DB):  
  Responsible for storing and querying the book database.

- [**Bookstore Scraper**](https://github.com/AhmedElshehawy/Bookstore_Scraper):  
  Scrapes data from various web sources and feeds it into the database.

- [**Bookstore Frontend (BookBot)**](https://huggingface.co/spaces/elshehawy/BookBot):  
  Provides a chat interface for users to interact with the bookstore chatbot.

*Note:* Ensure that the required environment variables are set in your environment before running the script.

### AWS Lambda Deployment

This repository is designed to be AWS Lambda friendly. The `lambda_handler` function (defined in `main.py`) serves as the entry point when deployed on AWS Lambda. Make sure to configure your Lambda function with the appropriate environment variables and handler.

## Code Structure

- **main.py:**  
  Contains the entry point for asynchronous scraping, processing of book URLs, and managing concurrent database upserts.

- **book_scraper.py:**  
  Implements the `BookScraper` class with methods to retrieve all book URLs and extract detailed book information from individual book pages.

- **book_model.py:**  
  Defines the `Book` model using Pydantic, which is used to validate and structure scraped data.

- **requirements.txt:**  
  Lists all necessary dependencies for the project.


## Logging

The scraper is configured to use Python's logging module with a standardized format that includes timestamps, module names, log levels, and messages. You can adjust the logging level and format in `main.py` as needed.

---

Happy scraping!

> **Note:** When testing locally, ensure you manually set the same environment variables (e.g., via a `.env` file or your shell) as those provided in the AWS Lambda environment.

