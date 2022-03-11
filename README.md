# Dokklib-DB (UNMAINTAINED)

![CI](https://github.com/dokklib/dokklib-db/workflows/CI/badge.svg) [![Join the chat at https://gitter.im/dokklib/dokklib-db](https://badges.gitter.im/dokklib/dokklib-db.svg)](https://gitter.im/dokklib/dokklib-db?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Dokklib-DB is a Python library for the DynamoDB single table pattern.

## Features

- Simple, Pythonic query interface on top of Boto3. No more nested dict literals!
- Type safety for primary keys and indices (for documentation and data integrity).
- Easy error handling.
- Full type hint & unit test coverage + integration testing.

## [Documentation](https://github.com/dokklib/dokklib/tree/master/docs)

## Install

Install with:

`pip install "boto3>=1.10.34,<2" dokklib-db`

Requires Python 3.6 or later.

Note that Boto3 is not specified as an installation requirement for Dokklib-DB, so you have to install it separately (like in the example command above).
The reason for this is to make Dokklib-DB easier to use in AWS Lambda where Boto3 is part of the default environment.
The earliest supported Boto3 version is `1.10.34` which is the same version as the Boto3 package in the Python 3 AWS Lambda environments.

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
    global_index=db.InversePrimaryIndex())

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

The project is no longer maintained.

~The library is in beta and under heavy development as I'm working on it while building a [serverless project](https://github.com/dokknet/dokknet-api) that relies on it.
I have only implemented parts of the DynamoDB API that I needed so far, but I'm planning on achieving full coverage.
Feature and pull requests are welcome. (Please open an issue, before starting work on a pull request to avoid wasted effort.)~
