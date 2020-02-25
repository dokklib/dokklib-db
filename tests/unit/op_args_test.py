import re
from unittest.mock import patch

import boto3.dynamodb.conditions as cond

import dokklib_db as db
import dokklib_db.op_args as m

from tests.unit import TestBase


class User(db.EntityName):
    pass


class Subscription(db.EntityName):
    pass


class TestOpArg(TestBase):
    def test_iso_now(self):
        res = m.OpArg._iso_now()
        iso_format = r'\d{4}-\d{2}-\d{2}T\d{2}\:\d{2}\:\d{2}'
        self.assertTrue(re.match(iso_format, res))


class OpTestMixin:
    def _get_kwargs(self):
        return self._op_arg.get_kwargs(self._table_name, self._primary_index)

    def setUp(self):
        self._pk = db.PartitionKey(User, 'eva.lu-ator@example.com')
        self._sk = db.SortKey(Subscription, 'mitpress.mit.edu')
        self._table_name = 'my-table'
        self._primary_index = db.PrimaryGlobalIndex()

    def test_table_name(self):
        kwargs = self._get_kwargs()
        self.assertEqual(kwargs['TableName'], self._table_name)


class ConsistencyTestMixin:
    def test_key(self):
        kwargs = self._get_kwargs()
        self.assertFalse(kwargs['ConsistentRead'])


class KeyTestMixin:
    def test_key(self):
        kwargs = self._get_kwargs()
        key = kwargs['Key']
        self.assertEqual(key['PK']['S'], str(self._pk))
        self.assertEqual(key['SK']['S'], str(self._sk))


class TestDeleteArg(KeyTestMixin, OpTestMixin, TestBase):
    def setUp(self):
        super().setUp()
        self._op_arg = m.DeleteArg(self._pk, self._sk)

    def test_not_idempotent(self):
        op_arg = m.DeleteArg(self._pk, self._sk, idempotent=False)
        kwargs = op_arg.get_kwargs(self._table_name, self._primary_index)
        self.assertEqual(kwargs['ConditionExpression'],
                         'attribute_exists(PK)')

    def test_idempotent(self):
        kwargs = self._get_kwargs()
        self.assertNotIn('ConditionExpression', kwargs)


class TestGetArg(ConsistencyTestMixin, KeyTestMixin, OpTestMixin, TestBase):
    def setUp(self):
        super().setUp()
        self._op_arg = m.GetArg(self._pk, self._sk)

    def test_projection(self):
        op_arg = m.GetArg(self._pk, self._sk, attributes=['PK', 'SK', 'foo'])
        kwargs = op_arg.get_kwargs(self._table_name, self._primary_index)
        proj = kwargs['ProjectionExpression']
        self.assertLessEqual(proj, 'PK,SK,foo')


class TestPutArg(OpTestMixin, TestBase):
    def setUp(self):
        super().setUp()
        self._op_arg = m.PutArg(self._pk, self._sk)

    @patch('dokklib_db.op_args.PutArg._iso_now')
    def test_adds_created_at(self, iso_now):
        exp_created_at = 'test-time-stamp'
        iso_now.return_value = exp_created_at
        item = self._op_arg._get_dynamo_item(self._primary_index)
        self.assertEqual(item['CreatedAt']['S'], exp_created_at)

    def test_keys_added(self):
        item = self._op_arg._get_dynamo_item(self._primary_index)
        self.assertEqual(item['PK']['S'], self._pk)
        self.assertEqual(item['SK']['S'], self._sk)

    def test_adds_attributes(self):
        put_arg = m.PutArg(self._pk, self._sk,
                           attributes={'foo': '1', 'bar': 2})
        item = put_arg._get_dynamo_item(self._primary_index)
        self.assertEqual(item['foo']['S'], '1')
        self.assertEqual(item['bar']['N'], '2')

    def test_attributes_dont_overwrite_keys(self):
        attributes = {
            'foo': '1',
            'bar': 2,
            'PK': 'my-pk',
            'SK': 'my-sk'
        }
        put_arg = m.PutArg(self._pk, self._sk, attributes=attributes)
        item = put_arg._get_dynamo_item(self._primary_index)
        self.assertEqual(item['PK']['S'], self._pk)
        self.assertEqual(item['SK']['S'], self._sk)

    def test_disallow_overwrite(self):
        put_arg = m.PutArg(self._pk, self._sk, allow_overwrite=False)
        kwargs = put_arg.get_kwargs(self._table_name, self._primary_index)
        cond_expression = 'attribute_not_exists(PK)'
        self.assertEqual(kwargs['ConditionExpression'], cond_expression)


class TestInsertArg(OpTestMixin, TestBase):
    def setUp(self):
        super().setUp()
        self._op_arg = m.InsertArg(self._pk, self._sk)

    def test_no_overwrite(self):
        kwargs = self._get_kwargs()
        cond_expression = 'attribute_not_exists(PK)'
        self.assertEqual(kwargs['ConditionExpression'], cond_expression)


class TestQueryArg(ConsistencyTestMixin,
                   OpTestMixin,
                   TestBase):
    def setUp(self):
        super().setUp()
        self._cond = cond.Key('PK').eq(str(self._pk))
        self._op_arg = m.QueryArg(self._cond)

    def test_key_cond(self):
        kwargs = self._get_kwargs()
        key_cond = kwargs['KeyConditionExpression']
        tokens = key_cond.split(' ')
        self.assertEqual(tokens[1], '=')

    def test_expr_attribute_names(self):
        kwargs = self._get_kwargs()
        key_cond = kwargs['KeyConditionExpression']
        tokens = key_cond.split(' ')
        attr_names = kwargs['ExpressionAttributeNames']
        key, val = list(attr_names.items())[0]
        self.assertEqual(tokens[0], key)
        self.assertEqual(val, 'PK')

    def test_expr_attribute_values(self):
        kwargs = self._get_kwargs()
        key_cond = kwargs['KeyConditionExpression']
        tokens = key_cond.split(' ')
        attr_vals = kwargs['ExpressionAttributeValues']
        key, val = list(attr_vals.items())[0]
        self.assertEqual(tokens[2], key)
        self.assertEqual(val['S'], str(self._pk))

    def test_limit(self):
        limit = 10
        op_arg = m.QueryArg(self._cond, limit=limit)
        kwargs = op_arg.get_kwargs(self._table_name, self._primary_index)
        self.assertLessEqual(kwargs['Limit'], limit)

    def test_default_limit(self):
        kwargs = self._get_kwargs()
        limit = kwargs['Limit']
        self.assertLessEqual(limit, 1000)

    def test_over_limit(self):
        with self.assertRaises(ValueError):
            m.QueryArg(self._cond, limit=10000)

    def test_default_projection(self):
        kwargs = self._get_kwargs()
        proj = kwargs['ProjectionExpression']
        self.assertLessEqual(proj, 'SK')

    def test_projection(self):
        op_arg = m.QueryArg(self._cond, attributes=['PK', 'SK', 'foo'])
        kwargs = op_arg.get_kwargs(self._table_name, self._primary_index)
        proj = kwargs['ProjectionExpression']
        self.assertLessEqual(proj, 'PK,SK,foo')


class TestUpdateArg(OpTestMixin, TestBase):
    def setUp(self):
        super().setUp()
        self._op_arg = m.UpdateArg(self._pk, self._sk)

    def test_key(self):
        kwargs = self._get_kwargs()
        key = kwargs['Key']
        self.assertEqual(key['PK']['S'], str(self._pk))
        self.assertEqual(key['SK']['S'], str(self._sk))

    def test_put_args(self):
        put_attrs = {'foo': 1}
        op_arg = m.UpdateArg(self._pk, self._sk, attr_updates=put_attrs)
        kwargs = op_arg.get_kwargs(self._table_name, self._primary_index)
        foo_update = kwargs['AttributeUpdates']['foo']
        self.assertEqual(foo_update['Action'], 'PUT')
        self.assertEqual(foo_update['Value']['N'], str(put_attrs['foo']))
