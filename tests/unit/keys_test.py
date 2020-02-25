import dokklib_db as db
import dokklib_db.keys as m

from tests.unit import TestBase


class Project(db.EntityName):
    pass


class Session(db.EntityName):
    pass


class Subscription(db.EntityName):
    pass


class User(db.EntityName):
    pass


class TestToPrefix(TestBase):
    def test_correct_prefix(self):
        self.assertEqual(User.to_prefix(), 'USER#')

    def test_pk_prefix(self):
        pk = m.PartitionKey(User, 'val')
        self.assertEqual(User.to_prefix(), pk.prefix)


class TestEntityKeyEq(TestBase):
    def test_self(self):
        pk = m.PartitionKey(User, 'value')
        self.assertEqual(pk, pk)

    def test_eq(self):
        pk_1 = m.PartitionKey(User, 'value')
        pk_2 = m.PartitionKey(User, 'value')
        self.assertEqual(pk_1, pk_2)

    def test_pk_eq_sq(self):
        pk = m.PartitionKey(User, 'value')
        sk = m.SortKey(User, 'value')
        self.assertEqual(pk, sk)


class TestEntityKeyHash(TestBase):
    def test_hash_eq(self):
        pk_1 = m.PartitionKey(User, 'value')
        pk_2 = m.PartitionKey(User, 'value')
        self.assertEqual(hash(pk_1), hash(pk_2))


class TestStrValueMixin:
    _constructor = None

    def test_value(self):
        value = 'value'
        pk = self._constructor(Project, value)
        self.assertEqual(pk, f'PROJECT#{value}')

    def test_different_types(self):
        value = 'value'
        pk_domain = self._constructor(Project, value)
        pk_user = self._constructor(User, value)
        self.assertNotEqual(pk_domain, pk_user)


class TestPartitionKeyStr(TestBase, TestStrValueMixin):
    _constructor = m.PartitionKey


class TestSortKeyStr(TestBase, TestStrValueMixin):
    _constructor = m.SortKey


class TestPrefixSortKeyStr(TestBase):
    def test_no_value(self):
        sk_domain = m.PrefixSortKey(Subscription)
        self.assertEqual(sk_domain, 'SUBSCRIPTION#')


class TestEntityKeyRepr(TestBase):
    def test_pk_repr_no_leak(self):
        """Representation of PK shouldn't leak DB data."""
        value = 'pk-value-1234'
        pk = m.PartitionKey(Session, value)
        self.assertNotIn(value, repr(pk))

    def test_sk_repr_no_leak(self):
        """Representation of SK shouldn't leak DB data."""
        value = 'sk-value-5678'
        sk = m.SortKey(Session, value)
        self.assertNotIn(value, repr(sk))


class TestPrimaryKey(TestBase):
    def setUp(self):
        self._pk = m.PartitionKey(User, 'alice')
        self._sk = m.SortKey(Project, 'foo')
        self._primary = m.PrimaryKey(self._pk, self._sk)

    def test_repr_no_leak(self):
        """Representation of primary key shouldn't leak DB data."""
        self.assertNotIn(self._pk.value, repr(self._primary))
        self.assertNotIn(self._sk.value, repr(self._primary))

    def test_self_eq(self):
        p = m.PrimaryKey(self._pk, self._sk)
        return self.assertEqual(self._primary, p)

    def test_str_eq(self):
        t = (str(self._pk), str(self._sk))
        return self.assertEqual(self._primary, t)

    def test_self_hash(self):
        p = m.PrimaryKey(self._pk, self._sk)
        return self.assertEqual(hash(self._primary), hash(p))

    def test_str_hash(self):
        t = (str(self._pk), str(self._sk))
        return self.assertEqual(hash(self._primary), hash(t))

    def test_serialize(self):
        index = db.PrimaryGlobalIndex()
        res = self._primary.serialize(index)
        des_res = self._primary._serializer.deserialize_dict(res)
        exp = {
            index.partition_key: str(self._pk),
            index.sort_key: str(self._sk)
        }
        self.assertDictEqual(des_res, exp)
