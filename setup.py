from setuptools import setup, find_packages

setup(
    name='couchbase-sqlalchemy',
    version='0.1.5',
    author='Ayush Tripathi',
    author_email='ayush.tripathi@couchbase.com',
    description='A SQLAlchemy dialect for Couchbase Columnar.',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy>=1.3.0,<2.0',
        'couchbase>=4.0.0',
        'urllib3>=1.25.0',
        'requests>=2.20.0',
        'six>=1.11.0',
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'couchbasedb = src.dialect.couchbase_dialect:CouchbaseDialect',
        ],
    },
)
