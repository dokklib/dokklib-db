#!/usr/bin/env python3
"""Create DynamoDB table for integration tests.

Run from repo root.
"""
import argparse
import logging
from pathlib import Path

import boto3


def create_stack(stack_name: str, template_path: Path):
    logging.info(f'Using template {template_path}')
    with open(template_path) as f:
        template_body = f.read()
    logging.info(f'Creating stack "{stack_name}"...')
    client = boto3.client('cloudformation')
    client.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Capabilities=['CAPABILITY_IAM']
    )
    waiter = client.get_waiter('stack_create_complete')
    waiter.wait(
        StackName=stack_name,
        WaiterConfig={
            'Delay': 10  # seconds
        }
    )
    logging.info(f'Successfully created stack {stack_name}')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-n', '--stack_name', type=str,
                        default='DokklibDB-IntegrationTest',
                        help='Stack name')
    parser.add_argument('-p', '--template_path', type=Path,
                        default='./tests/integration/cloudformation.yml',
                        help='Path to the template file name')
    args = parser.parse_args()

    create_stack(args.stack_name, args.template_path)
