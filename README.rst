Couchbase SQLAlchemy
=====================

Couchbase dialect for SQLAlchemy to `Couchbase Analytics/Columnar <https://docs.couchbase.com/server/current/learn/services-and-indexes/services/analytics-service.html/>`_.
A Python client library for Couchbase Analytics, `couchbase-sqlalchemy` provides a comprehensive DB-API and SQLAlchemy dialect implementation for Couchbase.

.. image:: https://img.shields.io/pypi/v/couchbase-sqlalchemy.svg
    :target: https://pypi.python.org/pypi/couchbase-sqlalchemy/

.. image:: https://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt


Installing Couchbase SQLAlchemy
===============================

The Couchbase SQLAlchemy package can be installed from the public PyPI repository using pip:

    .. code-block:: shell

        pip install --upgrade couchbase-sqlalchemy

Run a Simple query
==================

1.Creatge a file (e.g. test.py) that contains the following Python sample code, which connects to Couchbase Analytics and displays a simple locate function query:

    .. code-block:: python

        import urllib.parse
        from sqlalchemy import create_engine
        # Define the Couchbase connection string (replace with your details)
        URL = 'couchbase://<username>:<password>@<host>:<port>?ssl=false'
        # Create the engine for Couchbase using the Couchbase SQLAlchemy dialect
        engine = create_engine(COUCHBASE_URL)
        query_result = engine.execute("select locate('something','there is something good');")
        result = query_result.fetchall()
        print(result)

2. Replace <your_user_login_name>, <your_password>, and <host> and <port> with the appropriate values for your Couchbase account and user.

3. Execute the sample code. For example, if you created a file named test.py:

    .. code-block:: shell

        python test.py


License
=======

Couchbase SQLAlchemy is distributed under the `Apache 2.0
<https://www.apache.org/licenses/LICENSE-2.0>`_.