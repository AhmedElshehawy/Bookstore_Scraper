"""
Module: book_model
------------------
This module defines the `Book` model using Pydantic's `BaseModel`. The `Book` model
represents a book entity with various attributes such as title, price, rating,
description, category, UPC, availability, and associated URLs. It also includes a
custom validator to ensure that the `price` attribute has at most two decimal places.

Attributes:
    title (str): The title of the book.
    price (Decimal): The non-zero price of the book, maintained with 2-decimal precision.
    rating (int): The rating of the book on a scale of 1 to 5.
    description (str): A detailed description of the book.
    category (str): The category to which the book belongs.
    upc (str): The Universal Product Code of the book.
    num_available_units (int): The number of available units in inventory.
    image_url (HttpUrl): A valid HTTP URL pointing to the book's image.
    book_url (HttpUrl): A valid HTTP URL with more details about the book.

Model Config:
    - json_encoders: Custom JSON encoding for Decimal and HttpUrl types.
    - arbitrary_types_allowed: Enables usage of arbitrary types in the model.
"""

from pydantic import BaseModel, Field, HttpUrl, field_validator
from decimal import Decimal
from typing import List, Optional, Dict, Any


class Book(BaseModel):
    """
    Book model representing a book in the inventory.

    Attributes:
        title (str): The title of the book. Must be at least 1 character long.
        price (Decimal): The price of the book. Must be a positive value and is
            automatically rounded to two decimal places.
        rating (int): The rating of the book, which must be between 1 and 5.
        description (str): A detailed description of the book.
        category (str): The category of the book.
        upc (str): The Universal Product Code of the book.
        num_available_units (int): The number of available units for the book.
        image_url (HttpUrl): A valid URL pointing to the book's image.
        book_url (HttpUrl): A valid URL containing additional book details.
    """
    title: str = Field(..., min_length=1, description="The title of the book")
    price: Decimal = Field(..., gt=0, description="The price of the book")
    rating: int = Field(..., gt=0, le=5, description="The rating of the book (1-5)")
    description: str = Field(..., description="The description of the book")
    category: str = Field(..., description="The category of the book")
    upc: str = Field(..., description="The UPC (Universal Product Code) of the book")
    num_available_units: int = Field(..., description="The number of available units of the book")
    image_url: HttpUrl = Field(..., description="The image URL of the book")
    book_url: HttpUrl = Field(..., description="The URL of the book's detail page")
    
    @field_validator('price')
    def validate_price(cls, v):
        """
        Ensure that the price has a maximum of two decimal places.

        This validator takes the incoming price value and ensures that it is
        quantized to two decimal places, thereby standardizing the price format.

        Args:
            v (Decimal): The price value supplied to the model.

        Returns:
            Decimal: The price value rounded to exactly two decimal places.
        """
        # Convert the value to a Decimal using its string representation
        # to avoid floating point precision issues, then quantize to 0.01
        return Decimal(str(v)).quantize(Decimal('0.01'))
    
    # Custom configuration for the Pydantic model
    model_config = {
        "json_encoders": {
            Decimal: lambda d: str(d),
            HttpUrl: lambda url: str(url),
        },
        "arbitrary_types_allowed": True,
    }
