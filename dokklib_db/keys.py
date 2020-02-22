"""Types for entity names and keys in the single table.

Each item in the table is uniquely identified by its primary key.
The primary key is a composite of the partition key and the sort key.
The partition key for an item is the concatenation of its entity type and the
key value: `ENTITY#key-value`.
The sort key equals the partition key if the item does not model a relation and
`OTHER_ENTITY#other_key` if the item models a relation.
Sort keys without values may be used to query relations with a certain
entity type. For example, to query all the subscriptions of a user, one
would use the `USER#foo@example.com` partition key and the `SUBSCRIPTION#`
sort key.

"""
from abc import ABC
from typing import Any, Dict, List, Optional, Type, Union, cast


AnySortKey = Union['SortKey', 'PrefixSortKey']


class EntityName(ABC):
    """Abstract base class of entity names.

    Applications must define their entities by inheriting from this class.
    Eg. in "app/entities.py":

    ```python
    import dokklib_db as db

    class User(db.EntityName):
        pass

    class Product(db.EntityName):
        pass

    ...

    ```

    """

    def __new__(cls) -> 'EntityName':  # pragma: no cover
        """Prevent creating abstract base class."""
        raise TypeError(f'{cls.__name__} can not be instantiated.')

    @classmethod
    def to_prefix(cls) -> str:
        """Convert class name to key prefix.

        Returns:
            The key prefix. Eg. if class name is 'User', then the prefix is
            'USER#'.

        """
        if cls is EntityName:
            raise TypeError(f'Entity names must inherit from {cls.__name__}.')  # pragma: no cover  # noqa 501
        return cls.__name__.upper() + '#'


class EntityKey(ABC):
    """Abstract base class of table keys."""

    def __init__(self, entity_name: Type[EntityName], value: str):
        """Initialize an EntityKey instance.

        Args:
            entity_name: The entity type name.
            value: The key value.

        """
        self._prefix = entity_name.to_prefix()
        self._value = value

    # New must match init + subclasses' init as well.
    def __new__(cls, *args: List[Any], **kwargs: Dict[str, Any]) \
            -> 'EntityKey':
        """Prevent creating abstract base class."""
        if cls is EntityKey:
            raise TypeError(f'{EntityKey.__name__} can not be instantiated.')  # pragma: no cover  # noqa 501
        return cast(EntityKey, object.__new__(cls))

    def __str__(self) -> str:
        """Get the string representation."""
        # Eg. ENTITY#value
        return f'{self._prefix}{self._value}'

    def __eq__(self, other: Any) -> bool:
        """Compare semantic equality."""
        return str(self) == str(other)

    @property
    def prefix(self) -> str:
        """Get the entity prefix of the key."""
        return self._prefix

    @property
    def value(self) -> Optional[str]:
        """Get the value of the key."""
        return self._value


class PartitionKey(EntityKey):
    """Partition key."""


class SortKey(EntityKey):
    """Sort key with a value."""


# Shouldn't inherit from `SortKey` as `PrefixSortKey` shouldn't pass where a
# `SortKey` is required.
class PrefixSortKey(EntityKey):
    """Prefix only sort key to query relations."""

    def __init__(self, entity_name: Type[EntityName], value: str = ''):
        """Initialize a PrefixSortKey instance.

        Args:
            entity_name: The entity type name.
            value: Optional prefix value.

        """
        super().__init__(entity_name, value)
