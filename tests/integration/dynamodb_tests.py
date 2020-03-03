#!/usr/bin/env python3
"""DynamoDB integration tests.

!!! This script will clear the table first, so make sure to only use it on a
testing table. !!!
"""
import logging

import boto3

import dokklib_db as db


TABLE_NAME = 'DokklibDB-IntegrationTest-SingleTable'

logging.basicConfig(level=logging.INFO)


class Order(db.EntityName):
    """Order entity name.

    Key value: unique order id with date prefixed, eg: '2020-02-21|order-id'
    Example key: 'ORDER#2020-02-21|order-id'.

    """


class User(db.EntityName):
    """User entity name.

    Key value: user email, eg: 'alice@example.com'.
    Example key: 'USER#alice@example.com'.

    """


class Product(db.EntityName):
    """Order entity name.

    Key v
    alue: unique product name, eg: 'my-book'.
    Example key: 'PRODUCT#my-book'.

    """


# From: https://stackoverflow.com/a/56616499
def _clear_db(table_name: str):
    logging.info('Clearing table')
    table = boto3.resource('dynamodb').Table(table_name)
    scan = None

    with table.batch_writer() as batch:
        while scan is None or 'LastEvaluatedKey' in scan:
            if scan is not None and 'LastEvaluatedKey' in scan:
                scan = table.scan(
                    ProjectionExpression='PK,SK',
                    ExclusiveStartKey=scan['LastEvaluatedKey'],
                )
            else:
                scan = table.scan(ProjectionExpression='PK,SK')

            for item in scan['Items']:
                batch.delete_item(Key={'PK': item['PK'], 'SK': item['SK']})


logging.info('Starting integration tests')

# We clear the DB instead of recreating it to save time.
_clear_db(TABLE_NAME)
table = db.Table(TABLE_NAME)

# Users
pk_alice = db.PartitionKey(User, 'alice@example.com')
sk_alice = db.SortKey(User, 'alice@example.com')

# Products
pk_book = db.PartitionKey(Product, 'book')

# Orders
pk_order1 = db.PartitionKey(Order, '2020-02-21|order-1')
sk_order1 = db.SortKey(Order, '2020-02-21|order-1')
sk_order2 = db.SortKey(Order, '2020-02-21|order-2')

logging.info('Testing insert')
table.insert(pk_alice, sk_alice)

logging.info('Testing update_attributes')
table.update_attributes(pk_alice, sk_alice, {'MyJson': {'A': 1}})

logging.info('Testing get_item')
res = table.get(pk_alice, sk_alice,
                attributes=['MyJson'],
                consistent=True)
assert res['MyJson']['A'] == 1, res

logging.info('Testing transact_write_items')
table.transact_write_items([
    db.InsertArg(pk_alice, sk_order1),
    db.InsertArg(pk_book, sk_order1),
    db.InsertArg(pk_alice, sk_order2)
])

logging.info('Testing transact_write_items error handling')
try:
    table.transact_write_items([
        db.InsertArg(pk_alice, sk_order1),
        db.InsertArg(pk_book, sk_order2)
    ])
except db.errors.TransactionCanceledException as e:
    assert e.reasons[0] is db.errors.ConditionalCheckFailedException, e.reasons
    assert e.reasons[1] is None, e.reasons

logging.info('Testing batch_get')
res = table.batch_get([
    db.PrimaryKey(pk_alice, sk_order1),
    db.PrimaryKey(pk_book, sk_order1),
    db.PrimaryKey(pk_alice, sk_order2),
], consistent=True)
assert len(res.items) == 3, res

logging.info('Testing query_prefix')
res = table.query_prefix(pk_alice, db.PrefixSortKey(Order))
assert len(res) == 2, res

logging.info('Testing query_prefix on inverse index')
res = table.query_prefix(pk_order1, db.PrefixSortKey(User),
                         global_index=db.InversePrimaryIndex())
assert len(res) == 1, res

logging.info('Testing delete')
table.delete(pk_alice, sk_alice, idempotent=False)

logging.info('+++ Success +++')
