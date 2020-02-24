import copy
import re
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional

import boto3
import boto3.dynamodb.conditions as cond
from boto3.dynamodb.types import TypeDeserializer

import botocore.client
from botocore.exceptions import ClientError

from dokklib_db.index import GlobalIndex, GlobalSecondaryIndex, \
    PrimaryGlobalIndex
from dokklib_db.keys import PartitionKey, PrefixSortKey, SortKey
from dokklib_db.op_args import Attributes, DeleteArg, GetArg, InsertArg, \
    OpArg, PutArg, QueryArg, UpdateArg


ItemResult = Mapping[str, Any]


class DatabaseError(Exception):
    """Raised when a DynamoDB error occurred without a more specific reason.

    All other errors raised by `dokklib_db.Table` inherit from this.
    """


class CapacityError(DatabaseError):
    """Raised when a ProvisionedThroughputExceededException is raised."""


class ConditionalCheckFailedError(DatabaseError):
    """Raised when a conditional check failed.

    Eg. when trying to insert an item that already exists.
    """


class TransactionError(DatabaseError):
    """Raised when a transaction failed without a more specific reason."""


class TransactionConflict(TransactionError):
    """The transaction failed due to conflict from an other transaction."""


class Table:
    """DynamoDB table for the single table pattern.

    Table instances are not safe to share across threads.
    """

    @staticmethod
    @contextmanager
    def _dispatch_client_error() -> Iterator[None]:
        """Raise appropriate exception based on ClientError code."""
        try:
            yield None
        except ClientError as e:
            db_error = e.response.get('Error', {})
            code = db_error.get('Code')
            if code == 'ConditionalCheckFailedException':
                raise ConditionalCheckFailedError(e)
            if code == 'ProvisionedThroughputExceededException':
                raise CapacityError(e)
            elif code == 'TransactionCanceledException':
                message = db_error.get('Message', '')
                if 'ConditionalCheckFailed' in message:
                    raise ConditionalCheckFailedError(e)
                elif 'TransactionConflict' in message:
                    raise TransactionConflict(e)
                else:
                    raise TransactionError(e)
            else:
                raise DatabaseError(e)

    @staticmethod
    def _remove_entity_prefix(string: str) -> str:
        # Entity names are upper-cased Python class names.
        pattern = r'^[A-Z0-9_]+#(.+)$'
        match = re.match(pattern, string)
        if match:
            return match.group(1)
        else:
            return string

    @classmethod
    def _strip_prefixes(cls, item: Dict[str, Any]) -> ItemResult:
        """Strip entity prefixes from a DB item."""
        item_copy = copy.deepcopy(item)
        for k, v in item_copy.items():
            if isinstance(v, str):
                item_copy[k] = cls._remove_entity_prefix(v)
        return item_copy

    def __init__(self, table_name: str,
                 primary_index: Optional[GlobalIndex] = None):
        """Initialize a Table instance.

        Args:
            table_name: The DynamoDB table name.
            primary_index: The primary global index of the table.
                Defaults to `db.PrimaryGlobalIndex` that has 'PK' as the
                partition key name and 'SK' as the sort key name.

        """
        self._table_name = table_name
        if primary_index:
            self._primary_index = primary_index
        else:
            self._primary_index = PrimaryGlobalIndex()
        self._deserializer = TypeDeserializer()

        # The boto objects are lazy-initialzied. Connections are not created
        # until the first request.
        self._client_handle = boto3.client('dynamodb')

    @property
    def _client(self) -> 'botocore.client.DynamoDB':
        # Helps mock the client at test time.
        return self._client_handle

    @property
    def primary_index(self) -> GlobalIndex:
        """Get the primary global index of the table."""
        return self._primary_index

    @property
    def table_name(self) -> str:
        """Get the DynamoDB table name."""
        return self._table_name

    def _deserialize_dict(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize a dictionary while preserving its top level keys."""
        return {k: self._deserializer.deserialize(v) for k, v in item.items()}

    def _normalize_item(self, item: Dict[str, Any]) -> ItemResult:
        des_item = self._deserialize_dict(item)
        return self._strip_prefixes(des_item)

    def _put_item(self, put_arg: PutArg) -> None:
        kwargs = put_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_client_error():
            self._client.put_item(**kwargs)

    def _query(self, query_arg: QueryArg) -> List[ItemResult]:
        args = query_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_client_error():
            query_res = self._client.query(**args)
        items = query_res.get('Items', [])
        return [self._normalize_item(item) for item in items]

    def _update_item(self, update_arg: UpdateArg) -> None:
        """Update an item or insert a new item if it doesn't exist.

        Args:
            update_arg: The update item op argument.

        Raises:
            dokklib_db.DatabaseError if there was a problem connecting to
                DynamoDB.

        """
        kwargs = update_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_client_error():
            self._client.update_item(**kwargs)

    def delete(self, pk: PartitionKey, sk: SortKey,
               idempotent: bool = True) -> None:
        """Delete an item from the table.

        Args:
            pk: The primary key.
            sk: The sort key.
            idempotent: Whether the operation is idempotent. Defaults to True.

        """
        delete_arg = DeleteArg(pk, sk, idempotent=idempotent)
        kwargs = delete_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_client_error():
            self._client.delete_item(**kwargs)

    def get(self, pk: PartitionKey, sk: SortKey,
            attributes: Optional[List[str]] = None,
            consistent: bool = False) -> Optional[ItemResult]:
        """Fetch an item by its primary key from the table.

        Args:
            pk: The primary key.
            sk: The sort key.
            attributes: The attributes to get. Returns all attributes if
                omitted.
            consistent: Whether the read is strongly consistent or not.

        Returns:
            The item if it exists.

        """
        get_arg = GetArg(pk, sk, attributes=attributes, consistent=consistent)
        kwargs = get_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_client_error():
            res = self._client.get_item(**kwargs)
        item = res.get('Item')
        if item:
            return self._normalize_item(item)
        else:
            return None

    # Type checks are sufficient to test this function, so it's excluded from
    # unit test coverage.
    def insert(self, pk: PartitionKey, sk: SortKey,
               attributes: Optional[Attributes] = None) -> None:  # pragma: no cover  # noqa 501
        """Insert a new item into the table.

        The UpdateAt attribute of the item is automatically set.
        The insert fails if an item with the same composite key (PK, SK)
        exists.

        Args:
            pk: The partition key.
            sk: The sort key.
            attributes: Dictionary with additional attributes of the item.

        Raises:
            dokklib_db.ItemExistsError if the item with the same composite
                key already exists.
            dokklib_db.DatabaseError if there was a problem connecting to
                DynamoDB.

        """
        put_arg = InsertArg(pk, sk, attributes=attributes)
        self._put_item(put_arg)

    # Type checks are sufficient to test this function, so it's excluded from
    # unit test coverage.
    def put(self, pk: PartitionKey, sk: SortKey,
            attributes: Optional[Attributes] = None,
            allow_overwrite: bool = True) -> None:  # pragma: no cover  # noqa 501
        """Insert a new item or replace an existing item.

        Args:
            pk: The partition key of the item.
            sk: The sort key of the item.
            attributes: Optional additional attributes of the item.
            allow_overwrite: Whether to allow overwriting an existing item.

        Raises:
            dokklib_db.DatabaseError if there was a problem connecting to
                DynamoDB.

        """
        put_arg = PutArg(pk, sk,
                         attributes=attributes,
                         allow_overwrite=allow_overwrite)
        self._put_item(put_arg)

    # Type checks are sufficient to test this function, so it's excluded from
    # unit test coverage.
    def query(self, key_condition: cond.ConditionBase,
              global_index: Optional[GlobalSecondaryIndex] = None,
              attributes: Optional[List[str]] = None,
              consistent: bool = False,
              limit: Optional[int] = None) -> List[ItemResult]:  # pragma: no cover  # noqa 501
        """Fetch items from the table based on a key condition.

        Doesn't support pagination.

        Args:
            key_condition: The key condition. Eg.:
                `Key('PK').eq(str(pk)) & Key('SK').begins_with(str(sk))`
            global_index: The global secondary index to query. Defaults to the
                primary index.
            attributes: The attributes to get. Defaults to `SK`.
            consistent: Whether the read is strongly consistent or not.
            limit: The maximum number of items to fetch. Defaults to 1000.

        Returns:
            The requested items with the entity name prefixes stripped,
            eg. if the value of an attribute is 'USER#foo@example.com',
            only 'foo@example.com' is returned.

        Raises:
            dokklib_db.DatabaseError if there was an error querying the
                table.

        """
        query_arg = QueryArg(key_condition,
                             global_index=global_index,
                             attributes=attributes,
                             consistent=consistent,
                             limit=limit)
        return self._query(query_arg)

    def query_prefix(self, pk: PartitionKey, sk: PrefixSortKey,
                     global_index: Optional[GlobalSecondaryIndex] = None,
                     attributes: Optional[List[str]] = None,
                     consistent: bool = False,
                     limit: Optional[int] = None) -> List[ItemResult]:
        """Fetch a items from the table based on a sort key prefix.

        Doesn't support pagination.

        Args:
            pk: The partition key.
            sk: The sort key prefix.
            global_index: The global secondary index to query. Defaults to the
                primary index.
            attributes: The attributes to get. Defaults to
                `[self.primary_index.sort_key]` if no `global_index` is
                provided and `[global_index.sort_key]` if it is provided.
            consistent: Whether the read is strongly consistent or not.
            limit: The maximum number of items to fetch. Defaults to 1000.

        Returns:
            The requested items with the `PK` and `SK` prefixes stripped.

        Raises:
            dokklib_db.DatabaseError if there was an error querying DynamoDB.

        """
        if global_index:
            pk_name = global_index.partition_key
            sk_name = global_index.sort_key
        else:
            pk_name = self.primary_index.partition_key
            sk_name = self.primary_index.sort_key

        if not attributes:
            attributes = [sk_name]

        key_condition = cond.Key(pk_name).eq(str(pk)) & \
            cond.Key(sk_name).begins_with(str(sk))
        query_arg = QueryArg(key_condition,
                             global_index=global_index,
                             attributes=attributes,
                             consistent=consistent,
                             limit=limit)
        return self._query(query_arg)

    def transact_write_items(self, args: Iterable[OpArg]) -> None:
        """Write multiple items in a transaction.

        Note

        Args:
            args: Write OP args.

        Raises:
            dokklib_db.TransactionError if the transaction fails.
            dokklib_db.DatabaseError if there was a problem connecting
                DynamoDB.

        """
        transact_items = []
        for a in args:
            kwargs = a.get_kwargs(self.table_name, self.primary_index)
            transact_items.append({a.op_name: kwargs})
        with self._dispatch_client_error():
            self._client.transact_write_items(TransactItems=transact_items)

    # Type checks are sufficient to test this function, so it's excluded from
    # unit test coverage.
    def update_attributes(self, pk: PartitionKey, sk: SortKey,
                          attributes: Attributes) -> None:  # pragma: no cover
        """Update an item or insert a new item if it doesn't exist.

        The `UpdatedAt` attribute of the item is automatically set.

        Args:
            pk: The partition key.
            sk: The sort key.
            attributes: Dictionary with attributes to updates. These attributes
                will overwritten if they exist or created if they don't exist.

        Raises:
            dokklib_db.DatabaseError if there was a problem connecting to
                DynamoDB.

        """
        update_arg = UpdateArg(pk, sk, attr_updates=attributes)
        self._update_item(update_arg)
