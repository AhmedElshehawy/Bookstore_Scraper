from pydantic import BaseModel, Field, HttpUrl, field_validator, validator
from decimal import Decimal
from typing import List, Optional, Dict, Any


class Book(BaseModel):
    """
    Book model
    """
    title: str = Field(..., min_length=1, description="The title of the book")
    price: Decimal = Field(..., gt=0, description="The price of the book")
    rating: int = Field(..., gt=0, le=5, description="The rating of the book")
    description: str = Field(..., description="The description of the book")
    category: str = Field(..., description="The category of the book")
    upc: str = Field(..., description="The UPC of the book")
    num_available_units: int = Field(..., description="The number of available units of the book")
    image_url: HttpUrl = Field(..., description="The image URL of the book")
    book_url: HttpUrl = Field(..., description="The URL of the book")
    
    @field_validator('price')
    def validate_price(cls, v):
        """Ensure price has at most 2 decimal places."""
        return Decimal(str(v)).quantize(Decimal('0.01'))
    
    class Config:
        json_encoders = {Decimal: lambda v: str(v)}