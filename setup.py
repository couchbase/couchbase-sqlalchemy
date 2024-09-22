from setuptools import setup, find_packages
import os
from codecs import open

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

github_url = 'https://github.com/couchbase/couchbase-sqlalchemy'
with open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='couchbase-sqlalchemy',
    version='0.1.13',
    author='Ayush Tripathi',
    author_email='ayush.tripathi@couchbase.com',
    license = "Apache-2.0",
    long_description=long_description,
    description='A SQLAlchemy dialect for Couchbase Analytics/Columnar.',
    keywords='Couchbase db database cloud analytics Columnar',
    packages=find_packages(),
    install_requires=[
        'sqlalchemy>=1.3.0,<2.0',
        'couchbase>=4.0.0'
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'couchbase = couchbase_sqlalchemy.dialect.couchbase_dialect:CouchbaseDialect',
        ],
    },
)
