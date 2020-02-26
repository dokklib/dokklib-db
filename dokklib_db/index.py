# Type checks are enough to test this module.
# pragma: no cover
from abc import ABC, abstractmethod

from typing_extensions import Literal


class GlobalIndex(ABC):
    """Base class for global indices."""

    @property
    @abstractmethod
    def partition_key(self) -> str:
        """Get the name of the partition key."""
        raise NotImplementedError

    @property
    @abstractmethod
    def sort_key(self) -> str:
        """Get the name of the sort key."""
        raise NotImplementedError


class GlobalSecondaryIndex(GlobalIndex):
    """Base class for global secondary indices."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of the secondary index."""
        raise NotImplementedError


class PrimaryGlobalIndex(GlobalIndex):
    """Primary global DynamoDB primary_index."""

    @property
    def partition_key(self) -> Literal['PK']:
        """Get the name of the partition key."""
        return 'PK'

    @property
    def sort_key(self) -> Literal['SK']:
        """Get the name of the sort key."""
        return 'SK'


class InversePrimaryIndex(GlobalSecondaryIndex):
    """Inverted global secondary index."""

    @property
    def name(self) -> Literal['GSI_1']:
        """Get the global secondary index name."""
        return 'GSI_1'

    @property
    def partition_key(self) -> Literal['SK']:
        """Get the name of the partition key."""
        return 'SK'

    @property
    def sort_key(self) -> Literal['PK']:
        """Get the name of the sort key."""
        return 'PK'
