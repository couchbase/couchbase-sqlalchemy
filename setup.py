from setuptools import setup, find_packages

setup(
    name='couchbase-sqlalchemy',
    version='0.1.9',
    author='Ayush Tripathi',
    author_email='ayush.tripathi@couchbase.com',
    description='A SQLAlchemy dialect for Couchbase Analytics/Columnar.',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy>=1.3.0,<2.0',
        'couchbase>=4.0.0',
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'couchbase = couchbase_sqlalchemy.dialect.couchbase_dialect:CouchbaseDialect',
        ],
    },
)
