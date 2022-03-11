from setuptools import find_packages, setup


with open('README.md') as f:
    long_description = f.read()

print(find_packages(exclude=['tests*']))

setup(
    name='dokklib_db',
    author='Agost Biro',
    author_email='agost+dokklib_db@dokknet.com',
    description='DynamoDB Single Table Library',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://dokklib.com/libs/db/',
    packages=find_packages(exclude=['tests*']),
    use_scm_version=True,
    # Needed to let mypy use package for type hints
    zip_safe=False,
    package_data={"dokklib_db": ["py.typed"]},
    setup_requires=['setuptools_scm'],
    python_requires='>=3.6',
    install_requires=[
        'typing-extensions>=3.7.2,<4'
    ],
    classifiers=[
        'Development Status :: 7 - Inactive',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Database'
    ]
)
