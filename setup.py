import setuptools


with open('README.md') as f:
    long_description = f.read()

setuptools.setup(
    name='dokklib_db',
    # TODO (abiro) get version from git tags
    version='0.0.7',
    author='Agost Biro',
    author_email='agost+dokklib_db@dokknet.com',
    description='DynamoDB Single Table Library',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://dokklib.com/libs/db/',
    packages=['dokklib_db'],
    install_requires=[
        'boto3>=1.10.34,<2',
        'botocore>=1.13.34,<2'
    ],
    data_files=[('', ['LICENSE', 'NOTICE'])],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3.8',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Database'
    ]
)
