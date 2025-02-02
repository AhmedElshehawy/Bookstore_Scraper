import os
import psycopg2
from psycopg2.extras import DictCursor
from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime
from models import Book

class DatabaseHandler:
    """
    Handles all database operations for books, including connection management
    and CRUD operations with proper error handling.
    """
    def __init__(self):
        # Database connection parameters should be stored in environment variables
        self.db_params = {
            'dbname': os.environ['DB_NAME'],
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD'],
            'host': os.environ['DB_HOST'],
            'port': os.environ['DB_PORT']
        }
        self.conn = None
        self.cur = None

    def connect(self):
        """Establishes database connection and creates a cursor."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.cur = self.conn.cursor(cursor_factory=DictCursor)
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to database: {str(e)}")

    def close(self):
        """Closes database connection and cursor safely."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def book_exists(self, upc: str) -> Optional[dict]:
        """
        Checks if a book exists in the database and returns its current data if found.
        
        Args:
            upc: The UPC of the book to check
            
        Returns:
            dict: Book data if found, None otherwise
        """
        try:
            self.cur.execute("""
                SELECT title, price, rating, description, category, 
                       upc, num_available_units, image_url, book_url
                FROM books WHERE upc = %s
            """, (upc,))
            result = self.cur.fetchone()
            return dict(result) if result else None
        except psycopg2.Error as e:
            self.conn.rollback()
            raise Exception(f"Database error while checking book existence: {str(e)}")

    def insert_book(self, book: Dict):
        """
        Inserts a new book into the database.
        
        Args:
            book: Book object containing the data to insert
        """
        try:
            self.cur.execute("""
                INSERT INTO books (
                    title, price, rating, description, category,
                    upc, num_available_units, image_url, book_url,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                book['title'], book['price'], book['rating'], book['description'],
                book['category'], book['upc'], book['num_available_units'],
                book['image_url'], book['book_url'],
                datetime.now(datetime.UTC), datetime.now(datetime.UTC)
            ))
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            raise Exception(f"Failed to insert book: {str(e)}")

    def update_book(self, book: Dict):
        """
        Updates an existing book in the database.
        
        Args:
            book: Book object containing the updated data
        """
        try:
            self.cur.execute("""
                UPDATE books SET
                    title = %s,
                    price = %s,
                    rating = %s,
                    description = %s,
                    category = %s,
                    num_available_units = %s,
                    image_url = %s,
                    book_url = %s,
                    updated_at = %s
                WHERE upc = %s
            """, (
                book['title'], book['price'], book['rating'], book['description'],
                book['category'], book['num_available_units'], book['image_url'],
                book['book_url'], datetime.now(datetime.UTC), book['upc']
            ))
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            raise Exception(f"Failed to update book: {str(e)}")

    def books_are_different(self, existing_book: Dict, new_book: Dict) -> bool:
        """
        Compares an existing book with a new book to determine if they have different values.
        
        Args:
            existing_book: Dictionary containing current book data from database
            new_book: New Book object to compare against
            
        Returns:
            bool: True if books have different values, False if they're identical
        """
        return any([
            existing_book['title'] != new_book['title'],
            Decimal(str(existing_book['price'])) != new_book['price'],
            existing_book['rating'] != new_book['rating'],
            existing_book['description'] != new_book['description'],
            existing_book['category'] != new_book['category'],
            existing_book['num_available_units'] != new_book['num_available_units'],
            existing_book['image_url'] != new_book['image_url'],
            existing_book['book_url'] != new_book['book_url']
        ])

    def process_book(self, book: Book):
        """
        Main method to process a book - handles the logic for inserting new books
        and updating existing ones only when there are changes.
        
        Args:
            book: Book object to process
        """
        book = book.model_dump()
        book['price'] = float(book['price'])
        book['rating'] = int(book['rating'])
        book['num_available_units'] = int(book['num_available_units'])
        book['image_url'] = str(book['image_url'])
        book['book_url'] = str(book['book_url'])
        try:
            existing_book = self.book_exists(book.upc)
            
            if not existing_book:
                # Book doesn't exist, insert it
                self.insert_book(book)
            elif self.books_are_different(existing_book, book):
                # Book exists but has different values, update it
                self.update_book(book)
            # If book exists and is identical, do nothing (skip)
            
        except Exception as e:
            raise Exception(f"Failed to process book {book.upc}: {str(e)}")