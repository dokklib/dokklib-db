"""DynamoDB operation arguments."""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Mapping, Optional, Union

import boto3.dynamodb.conditions as cond

from typing_extensions import Literal

from dokklib_db.index import GlobalIndex, GlobalSecondaryIndex
from dokklib_db.keys import PartitionKey, PrimaryKey, SortKey
from dokklib_db.serializer import Serializer

_DynamoValue = Union[str, bool]

# Can't narrow value types down, because of TypedDict-Mapping
# incompatibiltiy. See https://stackoverflow.com/q/60304154
Attributes = Mapping[str, Any]
Kwargs = Mapping[str, Any]


class OpArg(ABC):
    """DynamoDB operation argument base class."""

    @staticmethod
    def _iso_now() -> str:
        now = datetime.utcnow()
        return now.replace(microsecond=0).isoformat()

    def __init__(self) -> None:
        """Initialize an OpArg instance."""
        self._serializer = Serializer()

    @property
    @abstractmethod
    def op_name(self) -> str:
        """Get the operation name for which this object is an argument.

        Must correspond to TransactWriteItem argument.
        """
        raise NotImplementedError

    @abstractmethod
    def get_kwargs(self, table_name: str, primary_index: GlobalIndex) \
            -> Kwargs:
        """Get key-word arguments that can be passed to the DynamoDB operation.

        Args:
            table_name: The DynamoDB table name for the operation.
            primary_index: The primary global index of the table.

        Returns:
            The key-word arguments.

        """
        raise NotImplementedError

    def _serialize_primary_key(self, primary_index: GlobalIndex,
                               pk: PartitionKey, sk: SortKey) \
            -> Mapping[str, Mapping[str, _DynamoValue]]:
        """Serialize composite key."""
        primary_key = PrimaryKey(pk, sk)
        return primary_key.serialize(primary_index)


class DeleteArg(OpArg):
    """Argument to a DynamoDB DeleteItem operation."""

    def __init__(self, pk: PartitionKey, sk: SortKey,
                 idempotent: bool = True):
        """Initialize a DeleteArg instance.

        Args:
            pk: The partition key of the item.
            sk: The sort key of the item.
            idempotent: If false, the op raises an error if the item to
                delete doesn't exist. Defaults to to true.

        """
        super().__init__()
        self._pk = pk
        self._sk = sk
        self._idempotent = idempotent

    @property
    def op_name(self) -> Literal['Delete']:  # pragma: no cover
        """Get the operation name for which this object is an argument."""
        return 'Delete'

    def get_kwargs(self, table_name: str, primary_index: GlobalIndex) \
            -> Kwargs:
        """Get key-word arguments that can be passed to a DeleteItem operation.

        Args:
            table_name: The DynamoDB table name for the DeleteItem operation.
            primary_index: The primary global index of the table.

        Returns:
            The key-word arguments.

        """
        key = self._serialize_primary_key(primary_index, self._pk, self._sk)
        kwargs = {
            'TableName': table_name,
            'Key': key
        }
        if not self._idempotent:
            # This check is performed after the item is retrieved by the
            # composite key, so no need to specify SK.
            kwargs['ConditionExpression'] = 'attribute_exists(PK)'

        return kwargs


class GetArg(OpArg):
    """Argument to a DynamoDB GetItem operation."""

    def __init__(self, pk: PartitionKey, sk: SortKey,
                 attributes: Optional[List[str]] = None,
                 consistent: bool = False):
        """Initialize a GetArg instance.

        Args:
            pk: The partition key of the item.
            sk: The sort key of the item.
            attributes: The attributes to get. Returns all attributes if
                omitted.
            consistent: Whether the read is strongly consistent or not.

        """
        super().__init__()
        self._pk = pk
        self._sk = sk
        self._attributes = attributes
        self._consistent = consistent

    @property
    def op_name(self) -> Literal['Get']:  # pragma: no cover
        """Get the operation name for which this object is an argument."""
        return 'Get'

    def get_kwargs(self, table_name: str, primary_index: GlobalIndex) \
            -> Kwargs:
        """Get key-word arguments that can be passed to a GetItem operation.

        Args:
            table_name: The DynamoDB table name for the GetItem operation.
            primary_index: The primary global index of the table.

        Returns:
            The key-word arguments.

        """
        key = self._serialize_primary_key(primary_index, self._pk, self._sk)
        kwargs = {
            'TableName': table_name,
            'Key': key,
            'ConsistentRead': self._consistent
        }
        if self._attributes:
            # TODO (abiro) convert inputs to expression attribute names
            kwargs['ProjectionExpression'] = ','.join(self._attributes)

        return kwargs


class PutArg(OpArg):
    """Argument to a DynamoDB PutItem operation.

    This op will replace the entire item. User `UpdateArg` if you just want to
    update a specific attribute.

    The `CreatedAt` attribute of the item is automatically set to the current
    ISO timestamp without microseconds (eg. '2020-02-15T19:09:38').

    """

    def __init__(self, pk: PartitionKey, sk: SortKey,
                 attributes: Optional[Attributes] = None,
                 allow_overwrite: bool = True):
        """Initialize a PutArg instance.

        Args:
            pk: The partition key of the item.
            sk: The sort key of the item.
            attributes: Optional additional attributes of the item.
            allow_overwrite: Whether to allow overwriting an existing item.

        """
        super().__init__()
        self._pk = pk
        self._sk = sk
        self._attributes = attributes
        self._allow_overwrite = allow_overwrite

    @property
    def op_name(self) -> Literal['Put']:  # pragma: no cover
        """Get the operation name for which this object is an argument."""
        return 'Put'

    def _get_dynamo_item(self, primary_index: GlobalIndex) \
            -> Mapping[str, Mapping[str, _DynamoValue]]:
        keys_item = self._serialize_primary_key(primary_index,
                                                self._pk,
                                                self._sk)

        item: Attributes = {
            'CreatedAt': self._iso_now()
        }
        if self._attributes:
            # `item` keys overwrite `_attributes` keys
            item = {**self._attributes, **item}

        dynamo_item = self._serializer.serialize_dict(item)
        return {**dynamo_item, **keys_item}

    def get_kwargs(self, table_name: str, primary_index: GlobalIndex) \
            -> Kwargs:
        """Get key-word arguments that can be passed to a PutItem operation.

        Args:
            table_name: The DynamoDB table name for the PutItem operation.
            primary_index: The primary global index of the table.

        Returns:
            The key-word arguments.

        """
        kwargs = {
            'TableName': table_name,
            'Item': self._get_dynamo_item(primary_index)
        }
        if not self._allow_overwrite:
            # The condition only checks if the item with the same composite key
            # exists. Ie. if there is an item (PK=foo, SK=0) in the table,
            # and we insert a new item (PK=foo, SK=1), the insert will succeed.
            kwargs['ConditionExpression'] = 'attribute_not_exists(PK)'

        return kwargs


class InsertArg(PutArg):
    """DynamoDB PutItem argument that prevents overwriting existing items."""

    def __init__(self, pk: PartitionKey, sk: SortKey,
                 attributes: Optional[Attributes] = None):
        """Initialize an InsertArg instance.

        The `UpdateAt` attribute of the item is automatically set.

        Args:
            pk: The partition key of the item.
            sk: The sort key of the item.
            attributes: Optional additional attributes of the item.

        """
        super().__init__(pk, sk,
                         attributes=attributes,
                         allow_overwrite=False)


class QueryArg(OpArg):
    """DynamoDB query operation argument.

    Note that query can not be used in a transaction, the purpose of this class
    is to provide a simplified interface to the boto3 DynamoDB table class.

    """

    _max_limit = 1000

    @staticmethod
    def _serialize_key_condition(key_cond: cond.ConditionBase) \
            -> cond.BuiltConditionExpression:
        builder = cond.ConditionExpressionBuilder()
        return builder.build_expression(key_cond, is_key_condition=True)

    def __init__(self, key_condition: cond.ConditionBase,
                 global_index: Optional[GlobalSecondaryIndex] = None,
                 attributes: Optional[List[str]] = None,
                 consistent: bool = False,
                 limit: Optional[int] = None):
        """Initialize a QueryArg instance.

        Args:
            key_condition: The key condition. Eg.:
                `Key('PK').eq(str(pk)) & Key('SK').begins_with(str(sk))`
            global_index: The global secondary index to query. Defaults to the
                primary index.
            attributes: The attributes to get. Defaults to `SK`.
            consistent: Whether the read is strongly consistent or not.
            limit: The maximum number of items to fetch. Defaults to 1000 which
                is also the maximum allowed value.

        """
        super().__init__()
        self._key_cond = key_condition
        self._attributes = attributes
        self._consistent = consistent
        self._global_index = global_index

        if limit is not None:
            if limit > self._max_limit:
                raise ValueError(f'Limit {limit} is greater than max '
                                 f'{self._max_limit}')
            self._limit = limit
        else:
            self._limit = self._max_limit

    @property
    def op_name(self) -> Literal['Query']:  # pragma: no cover
        """Get the operation name for which this object is an argument.

        Note that query can not be used in a transaction.

        """
        return 'Query'

    def _serialize_primary_key(self, primary_index: GlobalIndex,
                               pk: PartitionKey,
                               sk: SortKey) \
            -> Mapping[str, Mapping[str, _DynamoValue]]:
        """Serialize composite key."""
        # Using this inherited method in QueryArg would be a mistake, because
        # it wouldn't take into account the global secondary index.
        raise NotImplementedError

    def get_kwargs(self, table_name: str, primary_index: GlobalIndex) \
            -> Kwargs:
        """Get key-word arguments that can be passed to a boto3 DynamoDB table.

        Args:
            table_name: The DynamoDB table name for the operation.
            primary_index: The primary global index of the table.

        Returns:
            The key-word arguments.

        """
        kc = self._serialize_key_condition(self._key_cond)
        kc_value_placeholders = self._serializer.serialize_dict(
            kc.attribute_value_placeholders)
        kwargs = {
            'TableName': table_name,
            'Select': 'SPECIFIC_ATTRIBUTES',
            'KeyConditionExpression': kc.condition_expression,
            'ExpressionAttributeNames': kc.attribute_name_placeholders,
            'ExpressionAttributeValues': kc_value_placeholders,
            'ConsistentRead': self._consistent,
            'Limit': self._limit
        }
        if self._attributes:
            # TODO (abiro) convert inputs to expression attribute names
            kwargs['ProjectionExpression'] = ','.join(self._attributes)
        else:
            kwargs['ProjectionExpression'] = 'SK'
        if self._global_index:
            kwargs['IndexName'] = self._global_index.name
        return kwargs


class UpdateArg(OpArg):
    """Argument to a DynamoDB UpdateItem operation.

    This op updates the specified attributes or creates a new item if it
    doesn't exist yet.

    The `UpdatedAt` attribute of the item is automatically set to the current
    ISO timestamp without microseconds (eg. '2020-02-15T19:09:38').

    """

    def __init__(self, pk: PartitionKey, sk: SortKey,
                 attr_updates: Optional[Attributes] = None):
        """Initialize an UpdateArg instance.

        Args:
            pk: The partition key of the item.
            sk: The sort key of the item.
            attr_updates: Optional attributes to update for the item. These
                attributes will be overwritten if they exist, or created if
                they don't exist.

        """
        super().__init__()
        self._pk = pk
        self._sk = sk
        self._attr_updates = attr_updates

    @property
    def op_name(self) -> Literal['Update']:  # pragma: no cover
        """Get the operation name for which this object is an argument."""
        return 'Update'

    def _get_attr_updates(self) -> Mapping[str, Mapping[str, Any]]:
        item = {
            'UpdatedAt': self._iso_now()
        }
        if self._attr_updates:
            # `item` keys overwrite `_attributes` keys
            item = {**self._attr_updates, **item}
        res = {}
        for k, v in item.items():
            res[k] = {
                'Action': 'PUT',
                'Value': self._serializer.serialize_val(v)
            }
        return res

    def get_kwargs(self, table_name: str, primary_index: GlobalIndex) \
            -> Kwargs:
        """Get key-word arguments that can be passed to a PutItem operation.

        Args:
            table_name: The DynamoDB table name for the PutItem operation.
            primary_index: The primary global index of the table.

        Returns:
            The key-word arguments.

        """
        keys = self._serialize_primary_key(primary_index, self._pk, self._sk)
        attr_updates = self._get_attr_updates()
        kwargs = {
            'TableName': table_name,
            'Key': keys,
            'AttributeUpdates': attr_updates
        }
        return kwargs
