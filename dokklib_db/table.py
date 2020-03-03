import copy
import re
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Mapping, NamedTuple, \
    Optional, Tuple, Type, Union, cast

import boto3
import boto3.dynamodb.conditions as cond

import botocore.client
import botocore.exceptions as botoex

import dokklib_db.errors as err
from dokklib_db.index import GlobalIndex, GlobalSecondaryIndex, \
    PrimaryGlobalIndex
from dokklib_db.keys import PartitionKey, PrefixSortKey, PrimaryKey, SortKey
from dokklib_db.op_args import Attributes, DeleteArg, GetArg, InsertArg, \
    OpArg, PutArg, QueryArg, UpdateArg
from dokklib_db.serializer import Serializer


ItemResult = Mapping[str, Any]


class BatchGetResult(NamedTuple):
    """Result from a `Table.batch_get` operation."""

    items: List[ItemResult]
    unprocessed_keys: List[PrimaryKey]


class Table:
    """DynamoDB table for the single table pattern.

    Table instances are not safe to share across threads.
    """

    @staticmethod
    def _get_error_code(error: botoex.ClientError) -> str:
        db_error = error.response.get('Error', {})
        return cast(str, db_error.get('Code', 'None'))

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
    @contextmanager
    def _dispatch_transaction_error(cls, op_args: List[OpArg]) \
            -> Iterator[None]:
        """Raise appropriate exception based on ClientError code."""
        try:
            yield None
        except botoex.ClientError as e:
            code = cls._get_error_code(e)
            if code == 'TransactionCanceledException':
                raise err.TransactionCanceledException(op_args,
                                                       str(e),
                                                       e.response,
                                                       e.operation_name)
            else:
                raise cls._get_exception(e)

    @classmethod
    @contextmanager
    def _dispatch_error(cls) -> Iterator[None]:
        """Raise appropriate exception based on ClientError code."""
        try:
            yield None
        except botoex.ClientError as e:
            raise cls._get_exception(e)

    @classmethod
    def _get_exception(cls, error: botoex.ClientError) -> err.ClientError:
        code = cls._get_error_code(error)
        try:
            ex_class = cast(Type[err.ClientError], getattr(err, code))
        except AttributeError:  # pragma: no cover
            # Type checks are enough to test this.
            ex_class = err.ClientError
        return ex_class(str(error), error.response, error.operation_name)

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
        self._serializer = Serializer()

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

    def _normalize_item(self, item: Dict[str, Any]) -> ItemResult:
        des_item = self._serializer.deserialize_dict(item)
        return self._strip_prefixes(des_item)

    def _normalize_items(self, items: List[Dict[str, Any]]) \
            -> List[ItemResult]:
        return [self._normalize_item(item) for item in items]

    def _put_item(self, put_arg: PutArg) -> None:
        kwargs = put_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_error():
            self._client.put_item(**kwargs)

    def _query(self, query_arg: QueryArg) -> List[ItemResult]:
        args = query_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_error():
            query_res = self._client.query(**args)
        items = query_res.get('Items', [])
        return self._normalize_items(items)

    def _update_item(self, update_arg: UpdateArg) -> None:
        """Update an item or insert a new item if it doesn't exist.

        Args:
            update_arg: The update item op argument.

        Raises:
            dokklib_db.DatabaseError if there was a problem connecting to
                DynamoDB.

        """
        kwargs = update_arg.get_kwargs(self.table_name, self.primary_index)
        with self._dispatch_error():
            self._client.update_item(**kwargs)

    def batch_get(self, keys: Iterable[PrimaryKey],
                  attributes: Optional[List[str]] = None,
                  consistent: bool = False) -> BatchGetResult:
        """Fetch multiple items by their primary keys from the table.

        Note that the Dynamodb BatchGetItem API operation doesn't return items
        in order, that's why the primary key (PK and SK) of the item is always
        included in the Table.batch_get results.

        Further, note that while it's possible to make indiviual reads in
        strongly consistent, the returned snapshot has no isolation guarantees.
        If you need a consistent snapshot of multiple items in the database,
        you should use a transaction.

        Doesn't handle `UnprocessedKeys` in response.

        Args:
            keys: The primary keys of the items to get.
            attributes: The attributes to get. Returns all attributes if
                omitted. The partition and sort keys are always included even
                if not specified here.
            consistent: Whether the read is strongly consistent or not.

        Returns:
            The item if it exists.

        """
        attr_s = set(attributes or [])
        attr_s.add(self.primary_index.partition_key)
        attr_s.add(self.primary_index.sort_key)
        # TODO (abiro) convert inputs to expression attribute names
        proj_expr = ','.join(attr_s)

        key_map: Dict[Union[PrimaryKey, Tuple[str, str]], PrimaryKey] = {}
        key_items = []
        for key in keys:
            key_map[key] = key
            ser_key = key.serialize(self.primary_index)
            key_items.append(ser_key)

        request_items = {
            self.table_name: {
                'Keys': key_items,
                'ProjectionExpression': proj_expr,
                'ConsistentRead': consistent
            }
        }
        with self._dispatch_error():
            res = self._client.batch_get_item(RequestItems=request_items)

        responses = res.get('Responses', {})
        items = responses.get(self.table_name, [])
        norm_items = self._normalize_items(items)

        # Map unprocessed keys back to original `PrimaryKey` arguments.
        unproc = res.get('UnprocessedKeys', {})
        unproc_items = unproc.get(self.table_name, {})
        unproc_keys = []
        for item in unproc_items.get('Keys', []):
            pk_dynamo = item[self.primary_index.partition_key]
            sk_dynamo = item[self.primary_index.sort_key]
            pk_val = self._serializer.deserialize_val(pk_dynamo)
            sk_val = self._serializer.deserialize_val(sk_dynamo)
            key_tuple = (cast(str, pk_val), cast(str, sk_val))
            key = key_map[key_tuple]
            unproc_keys.append(key)

        return BatchGetResult(items=norm_items, unprocessed_keys=unproc_keys)

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
        with self._dispatch_error():
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
        with self._dispatch_error():
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

    def transact_write_items(self, op_args: List[OpArg]) -> None:
        """Write multiple items in a transaction.

        Args:
            op_args: Write operation arguments.

        Raises:
            dokklib_db.TransactionError if the transaction fails.
            dokklib_db.DatabaseError if there was a problem connecting
                DynamoDB.

        """
        transact_items = []
        for a in op_args:
            kwargs = a.get_kwargs(self.table_name, self.primary_index)
            transact_items.append({a.op_name: kwargs})
        with self._dispatch_transaction_error(op_args):
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
