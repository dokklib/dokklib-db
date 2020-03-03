import dokklib_db as db
from dokklib_db.errors.transaction import TransactionCanceledException

from tests.unit import TestBase


class TestTransactionCanceledException(TestBase):
    _op_name = 'TransactWriteItems'

    def _get_error(self, msg):
        return {
            'Error': {
                'Message': msg,
                'Code': 'TransactionCanceledException'
            }
        }

    def test_empty_message(self):
        error = self._get_error('')
        e = TransactionCanceledException([], '', error, self._op_name)
        self.assertListEqual(e.reasons, [])

    def test_mismatch(self):
        error = self._get_error('')
        e = TransactionCanceledException(['1'], '', error, self._op_name)
        with self.assertRaises(ValueError):
            e.reasons

    def test_one_reason(self):
        msg = 'Transaction cancelled, please refer cancellation reasons for ' \
              'specific reasons [ConditionalCheckFailed]'
        error = self._get_error(msg)
        e = TransactionCanceledException(['1'], '', error, self._op_name)
        exp = [db.errors.ConditionalCheckFailedException]
        self.assertListEqual(e.reasons, exp)

    def test_two_reasons(self):
        msg = 'Transaction cancelled, please refer cancellation reasons for ' \
              'specific reasons [ConditionalCheckFailed, None]'
        error = self._get_error(msg)
        e = TransactionCanceledException(['oparg1', 'oparg2'],
                                         '',
                                         error,
                                         self._op_name)
        exp = [db.errors.ConditionalCheckFailedException, None]
        self.assertListEqual(e.reasons, exp)

    def test_no_space_reasons(self):
        msg = 'Transaction cancelled, please refer cancellation reasons for ' \
              'specific reasons [ConditionalCheckFailed,None]'
        error = self._get_error(msg)
        e = TransactionCanceledException(['oparg1', 'oparg2'],
                                         '',
                                         error,
                                         self._op_name)
        exp = [db.errors.ConditionalCheckFailedException, None]
        self.assertListEqual(e.reasons, exp)

    def test_has_error(self):
        msg = 'Transaction cancelled, please refer cancellation reasons for ' \
              'specific reasons [ConditionalCheckFailed, None]'
        error = self._get_error(msg)
        e = TransactionCanceledException(['oparg1', 'oparg2'],
                                         '',
                                         error,
                                         self._op_name)
        self.assertTrue(e.has_error(db.errors.ConditionalCheckFailedException))

    def test_has_no_error(self):
        msg = 'Transaction cancelled, please refer cancellation reasons for ' \
              'specific reasons [ConditionalCheckFailed, None]'
        error = self._get_error(msg)
        e = TransactionCanceledException(['oparg1', 'oparg2'],
                                         '',
                                         error,
                                         self._op_name)
        self.assertFalse(e.has_error(db.errors.ValidationError))
