import dokklib_db.serializer as m

from tests.unit import TestBase


class TestSerialize(TestBase):
    def setUp(self):
        self._ser = m.Serializer()

    def test_deserialize_dict(self):
        d = {
            'foo': {'S': 'bar'},
            'baz': {'N': '1'}
        }
        exp = {
            'foo': 'bar',
            'baz': 1
        }
        res = self._ser.deserialize_dict(d)
        self.assertDictEqual(exp, res)

    def test_deserialize_val(self):
        val = {'L': [{'N': '1'}, {'N': '2'}]}
        res = self._ser.deserialize_val(val)
        self.assertListEqual(res, [1, 2])

    def test_serialize_dict(self):
        d = {
            'foo': False,
            'bar': 1
        }
        res = self._ser.serialize_dict(d)
        exp = {
            'foo': {'BOOL': False},
            'bar': {'N': '1'}
        }
        self.assertDictEqual(res, exp)

    def test_serialize_val(self):
        s = {'1', '2'}
        res = self._ser.serialize_val(s)
        self.assertSetEqual(set(res['SS']), s)
