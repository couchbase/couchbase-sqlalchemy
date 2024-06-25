from sqlalchemy.dialects import registry

registry.register(
    "columnar",
    "columnar.dialect.couchbase_dialect",
    "CouchbaseDialect"
)
