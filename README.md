# Dokklib-DB

Dokklib-DB is a library for the DynamoDB single table pattern.

## Features

- Simple, Pythonic query interface on top of Boto3 (no more nested dict literals!).
- Type safety for primary keys and indices (for documentation and data integrity).
- Full unit test coverage + integration testing.

## Documentation

TODO (abiro) add link to docs when ready

## Install

Install with:

`pip install dokklib-db`

## Example usage

```python
import dokklib_db as db


class User(db.EntityName):
    """User entity name.

    Key value: unique user name, eg. 'alice'.
    Example key: 'USER#alice'.

    """


class Group(db.EntityName):
    """Group entity name.

    Key value: unique group name, eg. 'my-group'.
    Example key: 'GROUP#my-group'.

    """


table = db.Table('SingleTable')

# Construct entity keys.
pk_alice = db.PartitionKey(User, 'alice')
pk_bob = db.PartitionKey(User, 'bob')
sk_group1 = db.SortKey(Group, 'group1')

# Add users to group one.
# Insert is a `PutItem` operation that fails if the item already exists.
table.insert(pk_alice, sk_group1)
table.insert(pk_bob, sk_group1)

# Get all users in group one.
pk_group = db.PartitionKey(Group, 'group1')
user_prefix = db.PrefixSortKey(User)
group_members = table.query_prefix(pk_group, user_prefix, 
    global_index=db.InverseGlobalIndex())

print(group_members)
# [{'PK': 'alice'}, {'PK': 'bob'}]

# Move users from group one to group two atomically.
sk_group2 = db.SortKey(Group, 'group2')
table.transact_write_items([
    db.DeleteArg(pk_alice, sk_group1),
    db.DeleteArg(pk_bob, sk_group1),
    db.InsertArg(pk_alice, sk_group2),
    db.InsertArg(pk_bob, sk_group2)
])
```

## Status

The library is in beta and under heavy development as I'm working on it while building a [serverless project](https://github.com/dokknet/dokknet-api) that relies on it.
I have only implemented parts of the DynamoDB API that I needed so far, but I'm planning on achieving full coverage.
Feature and pull requests are welcome. (Please open an issue, before starting work on a pull request to avoid wasted effort.)
