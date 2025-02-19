from dataclasses import dataclass
from datetime import datetime
from typing import List


@dataclass
class News:
    """Represents a news headline event.

    This class encapsulates the data for a news
    headline event, including the ID, category,
    the news text, and the timestamp when it was created.
    """

    id: str
    category: str
    text: str


@dataclass
class Review:
    """Represents a product review event.

    This class encapsulates the data for a product
    review event, including the ID, category,
    the review text, and the timestamp when it was created.
    """

    id: str
    category: str
    text: str


@dataclass
class Social:
    """Represents a social media post event.

    This class encapsulates the data for a social
    media post event, including the ID, category,
    the post text, and the timestamp when it was created.
    """

    id: str
    category: str
    text: str
