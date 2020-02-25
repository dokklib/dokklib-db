from typing import Any, Dict, Mapping, Optional, cast

from boto3.dynamodb.types import TypeDeserializer, TypeSerializer


_StrKeyDict = Dict[str, Any]


class Serializer:
    """Convert between Python and DynamoDB values."""

    def __init__(self) -> None:
        """Initialize a Serializer instance."""
        # Lazy-initialize serializers
        self._ser_handle: Optional[TypeSerializer] = None
        self._deser_handle: Optional[TypeDeserializer] = None

    @property
    def _deser(self) -> TypeDeserializer:
        if self._deser_handle is None:
            self._deser_handle = TypeDeserializer()
        return self._deser_handle

    @property
    def _ser(self) -> TypeSerializer:
        if self._ser_handle is None:
            self._ser_handle = TypeSerializer()
        return self._ser_handle

    def deserialize_dict(self, dynamo_item: Mapping[str, Any]) -> _StrKeyDict:
        """Deserialize a dictionary while preserving its top level keys.

        Args:
            dynamo_item: The dictionary from DynamoDB to deserialize.

        Returns:
            The deserialized dictionary.

        Raises:
            TypeError: if an unsupported type is encountered.

        """
        return {k: self.deserialize_val(v) for k, v in dynamo_item.items()}

    def deserialize_val(self, dynamo_val: Mapping[str, Any]) -> Any:
        """Convert a DynamoDB value to a Python value.

        Args:
            dynamo_val: The value from DynamoDB to deserialize.

        Returns:
            The deserialized value.

        Raises:
            TypeError: if an unsupported type is encountered.

        """
        return cast(_StrKeyDict, self._deser.deserialize(dynamo_val))

    def serialize_val(self, val: Any) -> _StrKeyDict:
        """Convert a Python value to a DynamoDB value.

        Args:
            val: The value to serialize.

        Returns:
            The serialized DynamoDB value.

        Raises:
            TypeError: if an unsupported type is encountered.

        """
        return cast(_StrKeyDict, self._ser.serialize(val))

    def serialize_dict(self, item: Mapping[str, Any]) -> _StrKeyDict:
        """Serialize a dictionary while preserving its top level keys.

        Args:
            item: The dictionary to serialize.

        Returns:
            The serialized DynamoDB dictionary.

        Raises:
            TypeError: if an unsupported type is encountered.

        """
        return {k: self.serialize_val(v) for k, v in item.items()}
