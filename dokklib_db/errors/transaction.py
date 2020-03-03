import re
from typing import Any, Dict, List, Optional, Type

from dokklib_db.errors import exceptions as ex
from dokklib_db.errors.client import ClientError
from dokklib_db.op_args import OpArg


CancellationReasons = List[Optional[Type[ClientError]]]


class TransactionCanceledException(ClientError):
    """The entire transaction request was canceled.

    Please see DynamoDB docs for details.
    https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html

    """

    # Example match: "reasons [ConditionalCheckFailed, None]"
    _reasons_re = re.compile(r'reasons\W+\[([A-Za-z0-9, ]+)]', re.MULTILINE)

    _codes_to_exceptions: Dict[str, Type[ClientError]] = {
        'ConditionalCheckFailed': ex.ConditionalCheckFailedException,
        'ItemCollectionSizeLimitExceeded': ex.ItemCollectionSizeLimitExceededException,  # noqa 501
        'TransactionConflict': ex.TransactionConflictException,
        'ProvisionedThroughputExceeded': ex.ProvisionedThroughputExceededException,  # noqa 501
        'ThrottlingError': ex.ThrottlingError,
        'ValidationError': ex.ValidationError
    }

    def __init__(self, op_args: List[OpArg], *args: Any, **kwargs: Any):
        """Initialize a TransactionCanceledException instance.

        Args:
            op_args: The list of operations that were the inputs to this
                transaction.

        """
        super().__init__(*args, **kwargs)
        self._op_args = list(op_args)
        self._reasons: Optional[CancellationReasons] = None

    def _extract_reasons(self, message: str) -> List[str]:
        match = re.search(self._reasons_re, message)
        if not match:
            return []
        else:
            reasons = match.group(1)
            split = reasons.split(', ')
            if split[0] == reasons:
                return reasons.split(',')
            else:
                return split

    def _get_reasons(self) -> CancellationReasons:
        db_error = self.response.get('Error', {})
        message = db_error.get('Message', '')
        reasons = self._extract_reasons(message)
        res: CancellationReasons = []
        for r in reasons:
            if r == 'None':
                res.append(None)
            else:
                exception = self._codes_to_exceptions.get(r, ClientError)
                res.append(exception)
        if len(res) != len(self.op_args):
            msg = f'Transaction cancellation reasons don\'t match ' \
                  f'transaction arguments in error:\n{message}'
            raise ValueError(msg)
        return res

    @property
    def op_args(self) -> List[OpArg]:
        """Get the list of inputs to the transaction."""
        return self._op_args

    @property
    def reasons(self) -> CancellationReasons:
        """List of cancellation reasons for each item in the transaction.

        Corresponds to order of `op_args`.

        """
        if self._reasons is None:
            self._reasons = self._get_reasons()
        return self._reasons

    def has_error(self, exception: Type[ClientError]) -> bool:
        """Whether the transaction failed due to a particular exception.

        Args:
            exception: The exception type to check for, eg. `ValidationError`.

        Returns:
            True if any of the failure reasons match the exception type.

        """
        for r in self.reasons:
            if r is exception:
                return True
        else:
            return False
