from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.engine.default import DefaultDialect
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
from urllib.parse import urlparse, parse_qs, urlencode
from urllib.parse import unquote

class CouchbaseIdentifierPreparer(IdentifierPreparer):
    def __init__(self, dialect, **kw):
        quote = '`'
        super().__init__(dialect, initial_quote=quote, escape_quote=quote)

    def _quote_free_identifiers(self, *ids):
        """
        Unilaterally identifier-quote any number of strings.
        """
        return tuple(self.quote(i) for i in ids if i is not None)

    def quote_schema(self, schema, force=None):
        """
        Split schema by a dot and merge with required quotes
        """
        idents = self._split_schema_by_dot(schema)
        return ".".join(self._quote_free_identifiers(*idents))

    def _split_schema_by_dot(self, schema):
        ret = []
        idx = 0
        pre_idx = 0
        in_quote = False
        while idx < len(schema):
            if not in_quote:
                if schema[idx] == "." and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    pre_idx = idx + 1
                elif schema[idx] == '`':
                    in_quote = True
                    pre_idx = idx + 1
            else:
                if schema[idx] == '`' and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    in_quote = False
                    pre_idx = idx + 1
            idx += 1
            if pre_idx < len(schema) and schema[pre_idx] == ".":
                pre_idx += 1
        if pre_idx < idx:
            ret.append(schema[pre_idx:idx])
        return ret

class CouchbaseDialect(DefaultDialect):
    name = "couchbase"
    driver = "couchbase"
    preparer = CouchbaseIdentifierPreparer
    supports_alter = False
    max_identifier_length = 255
    default_paramstyle = 'pyformat'
    paramstyle = 'pyformat'
    supports_native_enum = False
    supports_native_boolean = True
    supports_statement_cache = True
    schema_flag_map = {}
    @classmethod
    def dbapi(cls):
        from couchbase_sqlalchemy.dbapi import couchbase_dbapi
        return couchbase_dbapi

    def create_connect_args(self, url):
        parsed_url = urlparse(str(url))
        username = unquote(parsed_url.username)
        password = unquote(parsed_url.password)
        host = unquote(parsed_url.hostname)
        port = parsed_url.port
        query_params = parse_qs(parsed_url.query, keep_blank_values=True)
        ssl_enabled = query_params.get('ssl', ['true'])[0].lower()
        protocol = 'couchbases' if ssl_enabled=='true' else 'couchbase'
        query_params.pop('ssl', None)
        query_string = urlencode(query_params, doseq=True)
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

    def check_two_part_name(self, schema):
        parts = schema.split('/')
        if len(parts) == 2:
            database_name, scope_name = parts
            return database_name + '.' + scope_name
        else:
            return schema

    def get_schema_names(self, connection, **kw):
        query = """
            SELECT d.DatabaseName, d.DataverseName
            FROM Metadata.`Dataverse` d
        """
        result = connection.execute(query)
        schema_names = result.fetchall()
        schemas = []
        for r in schema_names:
            schema_key = r['DataverseName']
            if r['DatabaseName'] is None:
                schema_name = self.check_two_part_name(r['DataverseName'])
                schemas.append(schema_name)
                self.schema_flag_map[schema_name] = False
            else:
                schema_name = r['DatabaseName'] + '.' + r['DataverseName']
                schemas.append(schema_name)
                self.schema_flag_map[schema_name] = True
        return schemas

    def get_table_names(self, connection, schema=None, **kw):
        return []

    def get_view_names(self, connection, schema=None, **kw):
        if schema not in self.schema_flag_map:
            print(f"Schema '{schema}' not found in schema map.")
            return []
        is_full_schema = self.schema_flag_map[schema]
        query_base = """
        SELECT ds.DatasetName
        FROM Metadata.`Dataset` ds
        JOIN Metadata.`Datatype` dt ON ds.DatatypeDataverseName = dt.DataverseName
        AND ds.DatasetType = 'VIEW' AND ds.DatatypeName = dt.DatatypeName
        AND array_length(dt.Derived.Record.Fields) > 0
        """
        parts = schema.split('.')
        if is_full_schema:
            database_name, dataverse_name = parts
            condition = f"AND ds.DataverseName = '{dataverse_name}' AND ds.DatabaseName = '{database_name}'"
        elif len(parts) == 2:
            database_name, scope_name = parts
            dataverse_name = database_name + '/' + scope_name
            condition = f"AND ds.DataverseName = '{dataverse_name}'"
        else:
            condition = f"AND ds.DataverseName = '{schema}'"
        query = query_base + condition + ";"
        view_names = connection.execute(query)
        result = view_names.fetchall()
        views = [row['DatasetName'] for row in result]
        return views

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
        fields = columns_data[0]['Fields']
        type_map = {
        "string": VARCHAR,
        "int64": BIGINT,
        "datetime": DATETIME,
        "boolean": BOOLEAN,
        "double": FLOAT,
        "date":DATE,
        "time":TIME
        }
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