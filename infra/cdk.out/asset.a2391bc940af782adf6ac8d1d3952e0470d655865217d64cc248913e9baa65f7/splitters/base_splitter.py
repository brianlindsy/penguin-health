"""Base class for org-specific CSV splitters."""

from abc import ABC, abstractmethod
from typing import List, Tuple


class BaseCsvSplitter(ABC):
    """
    Base class for org-specific CSV splitters.

    Each organization may have different CSV formats requiring custom
    parsing logic. Subclasses implement the split() method to handle
    their specific format.
    """

    @property
    @abstractmethod
    def org_id(self) -> str:
        """Return the organization ID this splitter handles."""
        pass

    @abstractmethod
    def split(self, csv_content: str, filename: str) -> List[Tuple[str, str]]:
        """
        Split bulk CSV into individual chart CSVs.

        Args:
            csv_content: Raw CSV file content as string
            filename: Original filename for reference

        Returns:
            List of tuples: (chart_id, csv_content)
            - chart_id: Unique identifier for output filename
            - csv_content: CSV content for this individual chart (headers + data rows)
        """
        pass

    def detect_encoding(self, content: bytes) -> str:
        """
        Detect file encoding from bytes content.

        Tries common encodings in order of likelihood.
        """
        candidates = ['utf-8-sig', 'utf-8', 'cp1252', 'latin-1']
        for enc in candidates:
            try:
                content.decode(enc)
                return enc
            except (UnicodeDecodeError, LookupError):
                continue
        return 'latin-1'
