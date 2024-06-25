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

class CouchbaseSQLCompiler(SQLCompiler):
    def visit_column(self, column, add_to_result_map=None, include_table=True, **kwargs):
        if include_table and column.table is not None:
            return f"{column.table.name}.{column.name}"
        else:
            return f"{column.name}"
    def visit_table(self, table, asfrom=False, alias=None, **kwargs):
        # Customize the output of table names
        # `asfrom` indicates whether the table is being compiled in a FROM clause
        # `alias` is used if the table has an alias in the query
        print("table : ",table)
        if alias is not None:
            return f"{table.name} AS {alias}"
        else:
            return f"{table.name}"

class CouchbaseIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect, **kw):
        quote = '`'
        super().__init__(dialect, initial_quote=quote, escape_quote=quote)
    
class CouchbaseDialect(DefaultDialect):
    name = "columnar"
    driver = "couchbase"
    preparer = CouchbaseIdentifierPreparer
    statement_compiler = CouchbaseSQLCompiler
    supports_alter = False
    max_identifier_length = 255
    default_paramstyle = 'pyformat'
    paramstyle = 'pyformat'
    supports_native_enum = False
    supports_native_boolean = True
    supports_statement_cache = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def dbapi(cls):
        from columnar.dbapi import couchbase_dbapi
        return couchbase_dbapi
    def create_connect_args(self, url):
        print("reaching here create_connect_args")
        opts = url.translate_connect_args()
        opts.update(url.query)
        username = opts.get('username', '')
        password = opts.get('password','')
        server = opts.get('host','127.0.0.1')
        ssl_cert_path = opts.get('ssl','')
        connection_string = ""
        print("this is",ssl_cert_path,"empty")
        if ssl_cert_path != "": 
            port = opts.get('port',11998)
            connection_string = f"couchbases://{server}:{port}?truststorepath={ssl_cert_path}"
        else :
            port = opts.get('port',12000)
            connection_string = f"couchbase://{server}:{port}"
        
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
        print("schema is : ",schema)
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
        print("this is table name  : ",table_name)
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