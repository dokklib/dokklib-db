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
from typing import Any, Dict, List, Optional, Tuple, Type, Union, cast

from dokklib_db.index import GlobalIndex
from dokklib_db.serializer import Serializer

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

    def __hash__(self) -> int:
        """Get the hash value."""
        return hash(str(self))

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


class PrimaryKey:
    """Primary (composite) key of a DynamoDB item."""

    def __init__(self, partition_key: PartitionKey, sort_key: SortKey):
        """Initialize a PrimaryKey instance."""
        super().__init__()

        self._pk = partition_key
        self._sk = sort_key
        self._serializer = Serializer()

    def __hash__(self) -> int:
        return hash(self._tuple)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self._tuple == other._tuple
        else:
            return self._tuple == other

    @property
    def _tuple(self) -> Tuple[str, str]:
        return str(self.partition_key), str(self.sort_key)

    @property
    def partition_key(self) -> PartitionKey:  # pragma: no cover
        """Get the partition key."""
        return self._pk

    @property
    def sort_key(self) -> SortKey:  # pragma: no cover
        """Get the sort key."""
        return self._sk

    def serialize(self, global_index: GlobalIndex) -> Dict[str, Any]:
        """Serialize the primary key to a DynamoDB item.

        Args:
            global_index: The global index where this key will be used.

        Returns:
            The serialized key.

        """
        pk_name = global_index.partition_key
        sk_name = global_index.sort_key
        item = {
            pk_name: str(self.partition_key),
            sk_name: str(self.sort_key)
        }
        return self._serializer.serialize_dict(item)
