from typing import Any, Dict


class ClientError(Exception):
    """Base class of Dokklib-DB client errors."""

    def __init__(self,
                 message: str,
                 error_response: Dict[str, Any],
                 operation_name: str):
        """Initialize a ClientError instance.

        Args:
            message: The error message.
            error_response: The error response dict from Boto.
            operation_name: The DynamoDB API operation name.

        """
        super().__init__(message)
        self.response = error_response
        self.operation_name = operation_name
