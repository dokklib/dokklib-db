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


class TestEq(TestBase):
    def test_self(self):
        pk = m.PartitionKey(User, 'value')
        self.assertEqual(pk, pk)

    def test_neq(self):
        pk_1 = m.PartitionKey(User, 'value')
        pk_2 = m.PartitionKey(User, 'value')
        self.assertEqual(pk_1, pk_2)

    def test_pk_eq_sq(self):
        pk = m.PartitionKey(User, 'value')
        sk = m.SortKey(User, 'value')
        self.assertEqual(pk, sk)


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


class TestRepr(TestBase):
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
