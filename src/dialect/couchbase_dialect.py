from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.types import (
    BIGINT,
    BOOLEAN,
    DATE,
    DATETIME,
    FLOAT,
    TIME,
    VARCHAR,
)
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import make_url
import re
from urllib.parse import urlparse, parse_qs, urlencode
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy import select
from urllib.parse import unquote

class CouchbaseIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect, **kw):
        quote = '`'
        super().__init__(dialect, initial_quote=quote, escape_quote=quote)

class CouchbaseDialect(DefaultDialect):
    name = "couchbasedb"
    driver = "couchbasedb"
    preparer = CouchbaseIdentifierPreparer
    supports_alter = False
    max_identifier_length = 255
    default_paramstyle = 'pyformat'
    paramstyle = 'pyformat'
    supports_native_enum = False
    supports_native_boolean = True
    supports_statement_cache = True

    @classmethod
    def dbapi(cls):
        from src.dbapi import couchbase_dbapi
        return couchbase_dbapi

    def create_connect_args(self, url):
        # Parse the URL
        parsed_url = urlparse(str(url))
        username = unquote(parsed_url.username)
        password = unquote(parsed_url.password)
        host = unquote(parsed_url.hostname)
        port = parsed_url.port  # Default port if not specified
        # Handle the query parameters
        query_params = parse_qs(parsed_url.query, keep_blank_values=True)
        # Determine the protocol based on the SSL parameter
        ssl_enabled = query_params.get('ssl', ['true'])[0].lower()
        protocol = 'couchbases' if ssl_enabled=='true' else 'couchbase'
        # Remove the SSL parameter from the query parameters 
        query_params.pop('ssl', None)
        # Construct the query string without the SSL parameter
        query_string = urlencode(query_params, doseq=True)
        # Construct the new connection string
        connection_string = f"{protocol}://{host}"
        if port:
            connection_string += f":{port}"
        if query_string:
            connection_string += f"?{query_string}"

        return ([connection_string, username, password], {})

    

    def has_table(self, connection, table_name, schema=None):
        """
        Checks if the table exists
        """
        return self._has_object(connection,"TABLE",table_name,schema)
    
    def _get_table_columns(self, connection, table_name, schema=None):
        return self.get_columns(self, connection, table_name, schema)
        
    def _has_object(self, connection, object_type, object_name, schema=None):
        try:
            results = connection.execute(
                """ 
                SELECT d.DatasetName
                FROM Metadata.`Dataset` d
                Where d.DatasetName= "{object_name}}" 
                """
            )
            row = results.fetchone()
            have = row is not None
            return have
        except Exception as e:
            raise

    def get_schema_names(self, connection, **kw):
        # Hardcoding "Analytics View" as the schema name for demonstration
        query = """
                SELECT d.DataverseName 
                FROM Metadata.`Dataverse` d
                """
        result = connection.execute(query)
        schema = []
        for r in result:
            for p in r:
                schema.append(p[1:])
        return schema

    def get_table_names(self, connection, schema=None, **kw):
        # we define views inplace of tables.
        return []
        
    def get_view_names(self, connection, schema=None, **kw):
        query = """
                SELECT d.DatasetName 
                FROM Metadata.`Dataset` d 
                WHERE d.DatasetType = 'VIEW' 
                AND 
                d.DataverseName = "{schema}";
                """.format(schema=schema)
        result = connection.execute(query)
        view = []
        for r in result:
            for p in r:
                view.append(p[1:])
        return view
    
    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        query = f"""
                SELECT RAW d.ViewDetails.PrimaryKey
                FROM Metadata.`Dataset` d 
                WHERE d.DatasetType = 'VIEW'
                """
        result = connection.execute(query)
        keys = result.fetchall()
        Primary_Keys= [item for sublist in keys for inner_list in sublist for item in inner_list]
        pk_info = {
            'constrained_columns': [],
            'name': None
        }
        pk_info['constrained_columns'] = Primary_Keys
        return pk_info
    
    def get_columns(self, connection, table_name, schema=None, **kw):
        query = f"""
                SELECT d.Derived.Record.Fields 
                FROM Metadata.`Datatype` d 
                WHERE d.DatatypeName = '$d$t$i${table_name}';
                """
        
        result = connection.execute(query)
        columns_data = result.fetchall()
    # Assuming 'columns_data' returns a list of dictionaries under the key 'Fields'
        fields = columns_data[0]['Fields']
    
    # Map to translate JSON types to SQLAlchemy types
        type_map = {
        "string": VARCHAR,
        "int64": BIGINT,
        "datetime": DATETIME,
        "boolean": BOOLEAN,
        "double": FLOAT,
        "date":DATE,
        "time":TIME
        }

    # Constructing the list of dictionaries as per SQLAlchemy requirements
        columns = []
        for field in fields:
            column_info = {
            'name': f"{field['FieldName'][1:]}",
            'type': type_map[field['FieldType'][1:]](),
            'nullable': field['IsNullable'],
            'default': None,
            'autoincrement': field.get('IsAutoincrement', False)
            }
            columns.append(column_info)
        
        return columns


    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []