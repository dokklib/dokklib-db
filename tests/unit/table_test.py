from abc import ABC, abstractmethod
from unittest.mock import MagicMock

from boto3.dynamodb.conditions import Key

from botocore.exceptions import ClientError

import dokklib_db as db
from dokklib_db.table import Table

from tests.unit import TestBase


class User(db.EntityName):
    pass


class Subscription(db.EntityName):
    pass


class TestRemoveEntityPrefix(TestBase):
    def _test_val(self, prefix, val):
        res = Table._remove_entity_prefix(f'{prefix}{val}')
        self.assertEqual(val, res)

    def test_noop_on_no_match(self):
        val = 'foo'
        res = Table._remove_entity_prefix(val)
        self.assertEqual(val, res)

    def test_removes_class_uppercase(self):
        prefix = 'A1B2_CD3#'
        val = 'foo'
        self._test_val(prefix, val)

    def test_handles_multiple_hashes(self):
        prefix = 'PREFIX#'
        val = '#foo#bar#'
        self._test_val(prefix, val)

    def test_handles_pipe(self):
        prefix = 'PREFIX#'
        val = 'foo|bar'
        self._test_val(prefix, val)


class TestStripPrefixes(TestBase):
    def setUp(self):
        self._pk = db.PartitionKey(User, 'foo@example.com')
        self._sk = db.SortKey(Subscription, 'docs.example.com')

    def test_noop_on_no_prefix(self):
        item = {
            'foo': 'bar'
        }
        res = Table._strip_prefixes(item)
        self.assertDictEqual(item, res)

    def test_strips_prefixes(self):
        item = {
            'PK': str(self._pk),
            'SK': str(self._sk),
            'Foo': str(self._sk)
        }
        res = Table._strip_prefixes(item)
        self.assertEqual(res['PK'], self._pk.value)
        self.assertEqual(res['SK'], self._sk.value)
        self.assertEqual(res['Foo'], self._sk.value)

    def test_makes_copy(self):
        item = {
            'PK': str(self._pk),
            'SK': str(self._sk)
        }
        res = Table._strip_prefixes(item)
        self.assertNotEqual(item['PK'], res['PK'])
        self.assertNotEqual(item['SK'], res['SK'])


class TestInit(TestBase):
    _to_patch = [
        'dokklib_db.table.boto3'
    ]

    def test_client(self):
        boto3 = self._mocks['boto3']

        table = Table('my-table')
        self.assertEqual(table._client, boto3.client.return_value)

    def test_primary_index(self):
        pk_name = 'my-pk-name'
        sk_name = 'my-sk-name'

        class TestIndex(db.GlobalIndex):
            @property
            def partition_key(self) -> str:
                return pk_name

            @property
            def sort_key(self) -> str:
                return sk_name

        table = Table('my-table', TestIndex())
        self.assertEqual(table.primary_index.partition_key, pk_name)
        self.assertEqual(table.primary_index.sort_key, sk_name)


class TableTestCaseMixin(ABC):
    _to_patch = [
        'dokklib_db.table.boto3',
        'dokklib_db.table.Table._client#PROPERTY'
    ]

    @abstractmethod
    def _call_test_fn(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def _dynamo_method(self):
        raise NotImplementedError

    def setUp(self):
        super().setUp()

        self._client = MagicMock()
        self._mocks['_client'].return_value = self._client
        self._pk = db.PartitionKey(User, 'foo@example.com')
        self._sk = db.SortKey(Subscription, 'docs.example.com')
        self._sk_prefix = db.PrefixSortKey(Subscription)

    def test_handlers_throughput_error(self):
        error_response = {
            'Error': {
                'Code': 'ProvisionedThroughputExceededException',
            }
        }
        self._dynamo_method.side_effect = ClientError(error_response,
                                                      'OpName')
        with self.assertRaises(db.errors.ProvisionedThroughputExceededException):  # noqa 501
            self._call_test_fn()


class TestBatchGet(TableTestCaseMixin, TestBase):
    def setUp(self):
        super().setUp()
        self._pk_2 = db.PartitionKey(User, 'bar@example.com')
        self._sk_2 = db.SortKey(Subscription, 'docs.bar.com')
        self._keys = [
            db.PrimaryKey(self._pk, self._sk),
            db.PrimaryKey(self._pk_2, self._sk_2)
        ]
        self._table_name = 'my-table'
        self._table = Table(self._table_name)

    def _call_test_fn(self, attributes=None, consistent=False):
        attributes = attributes or []
        return self._table.batch_get(self._keys,
                                     attributes=attributes,
                                     consistent=consistent)

    def _get_call_arg(self, name, consistent=False, attributes=None):
        self._call_test_fn(attributes=attributes, consistent=consistent)
        _, kwargs = self._dynamo_method.call_args
        return kwargs['RequestItems'][self._table_name][name]

    def _get_attributes_call_arg(self, attributes=None):
        pe = self._get_call_arg('ProjectionExpression', attributes)
        attributes = pe.split(',')
        return attributes

    @property
    def _dynamo_method(self):
        return self._client.batch_get_item

    def test_keys(self):
        keys = self._get_call_arg('Keys', consistent=True)
        self.assertEqual(len(keys), 2)
        for i, key in enumerate(keys):
            exp = self._keys[i].serialize(self._table.primary_index)
            self.assertDictEqual(keys[i], exp)

    def test_consistent(self):
        consistent = self._get_call_arg('ConsistentRead', consistent=True)
        self.assertTrue(consistent)

    def test_retrieves_keys(self):
        attributes = self._get_attributes_call_arg(['foo'])
        self.assertTrue(set(attributes).issuperset({'PK', 'SK'}))

    def test_retrieves_keys_default(self):
        attributes = self._get_attributes_call_arg()
        self.assertSetEqual(set(attributes), {'PK', 'SK'})

    def test_results(self):
        key_1_ser = self._keys[0].serialize(self._table.primary_index)
        key_2_ser = self._keys[1].serialize(self._table.primary_index)
        self._dynamo_method.return_value = {
            'Responses': {
                self._table_name: [
                    key_1_ser
                ]
            },
            'UnprocessedKeys': {
                self._table_name: {
                    'Keys': [
                        key_2_ser
                    ]
                }
            }
        }
        res = self._call_test_fn()
        self.assertEqual(len(res.items), 1)
        self.assertEqual(res.items[0]['PK'], self._pk.value)
        self.assertEqual(res.items[0]['SK'], self._sk.value)

        self.assertEqual(len(res.unprocessed_keys), 1)
        self.assertEqual(res.unprocessed_keys[0], self._keys[1])


class TestDeleteItem(TableTestCaseMixin, TestBase):
    def _call_test_fn(self, table_name='my-table'):
        table = Table(table_name)
        return table.delete(self._pk, self._sk)

    @property
    def _dynamo_method(self):
        return self._client.delete_item


class QueryTestMixin(TableTestCaseMixin):
    def test_handles_no_result(self):
        self._dynamo_method.return_value = {}
        self.assertFalse(self._call_test_fn())

    def test_handles_empty_result(self):
        self._dynamo_method.return_value = {'Items': []}
        self.assertFalse(self._call_test_fn())


class TestQuery(QueryTestMixin, TestBase):
    def _call_test_fn(self, table_name='my-table'):
        table = Table(table_name)
        key_cond = Key('PK').eq(str(self._pk))
        query_arg = db.QueryArg(key_cond)
        return table._query(query_arg)

    @property
    def _dynamo_method(self):
        return self._client.query

    def test_strips_prefixes(self):
        self._dynamo_method.return_value = {
            'Items': [{'PK': {'S': str(self._pk)}}]
        }
        res = self._call_test_fn()
        self.assertEqual(res[0]['PK'], self._pk.value)


class TestGetItem(QueryTestMixin, TestBase):
    def _call_test_fn(self, attributes=None):
        table = Table('my-table')
        return table.get(self._pk, self._sk,
                         attributes=attributes)

    @property
    def _dynamo_method(self):
        return self._client.get_item

    def test_strips_prefixes(self):
        self._dynamo_method.return_value = {
            'Item': {'PK': {'S': str(self._pk)}}
        }
        res = self._call_test_fn()
        self.assertEqual(res['PK'], self._pk.value)


class TestQueryPrefix(QueryTestMixin, TestBase):
    def _call_test_fn(self, global_index=None, attributes=None):
        table = Table('my-table')
        return table.query_prefix(self._pk, self._sk_prefix,
                                  global_index=global_index,
                                  attributes=attributes)

    @property
    def _dynamo_method(self):
        return self._client.query

    def test_correct_key(self):
        self._call_test_fn()
        _, kwargs = self._dynamo_method.call_args
        kc = kwargs['KeyConditionExpression']
        self.assertEqual('(#n0 = :v0 AND begins_with(#n1, :v1))', kc)

    def test_global_index(self):
        index = db.InversePrimaryIndex()
        self._call_test_fn(global_index=index)
        _, kwargs = self._dynamo_method.call_args
        attr_names = kwargs['ExpressionAttributeNames']
        self.assertEqual(attr_names['#n0'], index.partition_key)
        self.assertEqual(attr_names['#n1'], index.sort_key)

    def test_defaults_to_global_index_sk_if_provided(self):
        index = db.InversePrimaryIndex()
        self._call_test_fn(global_index=index)
        _, kwargs = self._dynamo_method.call_args
        self.assertEqual(kwargs['ProjectionExpression'], index.sort_key)


class PutItemTestMixin(TableTestCaseMixin):
    def test_handles_conditional_check_failed(self):
        error_response = {'Error': {'Code': 'ConditionalCheckFailedException'}}
        self._dynamo_method.side_effect = ClientError(error_response,
                                                      'PutItem')
        with self.assertRaises(db.errors.ConditionalCheckFailedException):
            self._call_test_fn()


class TestPutItem(PutItemTestMixin, TestBase):
    def _call_test_fn(self, table_name='my-table'):
        table = Table(table_name)
        put_arg = db.PutArg(self._pk, self._sk)
        return table._put_item(put_arg)

    @property
    def _dynamo_method(self):
        return self._client.put_item


class TestTransactWriteItems(PutItemTestMixin, TestBase):

    def _call_test_fn(self, items=None, table_name='my-table'):
        table = Table(table_name)
        if not items:
            items = []
        return table.transact_write_items(items)

    @property
    def _dynamo_method(self):
        return self._client.transact_write_items

    def _setup_error(self, message=''):
        error_response = {
            'Error': {
                'Code': 'TransactionCanceledException',
                'Message': message
            }
        }
        self._dynamo_method.side_effect = ClientError(error_response,
                                                      'TransactWriteItems')

    def test_converts_to_op_name_dicts(self):
        op_name = 'my-op-name'
        table_name = 'foo-table-name'

        arg_mock = MagicMock(spec=db.PutArg)
        arg_mock.get_kwargs.return_value = 1
        arg_mock.op_name = op_name
        expected_item = {op_name: 1}

        self._call_test_fn(items=[arg_mock], table_name=table_name)
        arg_mock.get_kwargs.assert_called_once()
        args, _ = arg_mock.get_kwargs.call_args
        self.assertEqual(args[0], table_name)
        _, kwargs = self._dynamo_method.call_args
        self.assertDictEqual(kwargs, {'TransactItems': [expected_item]})

    def test_handles_transaction_failed(self):
        self._setup_error()
        with self.assertRaises(db.errors.TransactionCanceledException):
            self._call_test_fn()


class TestUpdateItem(TableTestCaseMixin, TestBase):
    def _call_test_fn(self, table_name='my-table'):
        table = Table(table_name)
        put_attributes = {
            'foo': 'bar'
        }
        update_arg = db.UpdateArg(self._pk, self._sk,
                                  attr_updates=put_attributes)
        return table._update_item(update_arg)

    @property
    def _dynamo_method(self):
        return self._client.update_item
