import logging
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from datetime import timedelta
from sqlalchemy import Float, String, Boolean, DateTime
from sqlalchemy import BigInteger
from couchbase.options import AnalyticsOptions
from sqlalchemy import BigInteger, String, Float, Boolean, DateTime, Date, Time
from datetime import datetime, timedelta
import struct

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

paramstyle = 'pyformat'


# Define the DB-API Exceptions
class Error(Exception):
    pass

class DatabaseError(Error):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class CouchbaseConnection:
    def __init__(self, connection_string, username, password):
        self.cluster = None
        self.open_buckets = {}
        try:
            self.cluster = Cluster(connection_string, ClusterOptions(PasswordAuthenticator(username, password)))
        except Exception as e:
            logger.error(f"Failed to connect to columnar cluster: {e}")
            raise

    def cursor(self):
        return CouchbaseCursor(self.cluster)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.open_buckets.clear()
        logger.info("Connection closed and buckets cleared.")

class CouchbaseCursor:
    def __init__(self, cluster):
        self.cluster = cluster
        self._rows = []
        self._description = []
        self.rowcount = -1

    def exe(self,query):
        return self.cluster.analytics_query(query)
    
    def execute(self, query, l = True ,params=None):
        self._rows = []
        qresult = self.cluster.analytics_query(query, AnalyticsOptions(raw={"format": "lossless-adm","signature": True, "client-type": "jdbc", "sql-compat": True}))
        
        self._rows = [row for row in qresult]
        # Assuming you've already created type_map as shown previously
        metadata = qresult.metadata().signature()
        self._description.clear()
        print("rows ",self._rows," rows"," ",metadata)
        if metadata.get('*') == '*':
            return
        try:
            for i in range(len(metadata['name'])):
                col_name = metadata['name'][i]
                col_type = metadata['type'][i]
                self._description.append((col_name,self.type_mapping(col_type), None, None, None, None, None))
        except Exception as e:
            logger.error(f"Failed to connect to columnar cluster: {e}")
            raise


    @property
    def description(self):
        return self._description

    def fetchone(self):
        if self._rows:
            row = self._rows.pop(0)
            return tuple(row.values())
        return None
    
    def type_mapping(self,col_type):
        type_mappings = {
        "int64": BigInteger,
        "int64?": BigInteger,
        "double": Float,
        "double?": Float,
        "string": String,
        "string?": String,
        "boolean": Boolean,
        "boolean?": Boolean,
        "datetime": DateTime,
        "datetime?": DateTime,
        "time": Time,
        "time?": Time,
        "date": Date,
        "date?": Date,
        "array?": "array"
        }
        return type_mappings.get(col_type)
    
    def convert_date(self,days):
        epoch = datetime(1970, 1, 1)
        target_date = epoch + timedelta(days=days)
        return target_date.strftime('%Y-%m-%d')

    def convert_time(self,milliseconds):
        seconds, milliseconds = divmod(milliseconds, 1000)
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{milliseconds:03d}"

    def convert_datetime(self,milliseconds):
        epoch = datetime(1970, 1, 1)
        target_datetime = epoch + timedelta(milliseconds=milliseconds)
        return target_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    
    import struct

    def convert_encoded_float(self,encoded_str):
        parts = encoded_str.split(':')
        if len(parts) != 2:
            raise ValueError("Invalid input format. Expected format 'HEX:NUMBER'.")
    
        hex_prefix, number_str = parts
        try:
        # Parse the hexadecimal part as an unsigned long long (64-bit integer)
            int_value = int(number_str)
        except ValueError:
            raise ValueError("The numeric part of the input isn't a valid integer.")
    
    # Check if the number is negative
        if int_value & (1 << 63):
        # If the number is negative, we need to compute the two's complement
            int_value = -(~int_value + 1 & (2**64 - 1))
    
        try:
        # Convert the 64-bit integer to a double precision float
            float_value = struct.unpack('>d', struct.pack('>q', int_value))[0]
        except struct.error:
            raise ValueError("Failed to convert integer to float.")
        return float_value

    def type_conversion(self,value,col_type):
        value_type = self.type_mapping(col_type)
        
        if value_type == Date and value.startswith("10:"):
            days_string = value[len("10:"):]
            days = int(days_string)
            return self.convert_date(days)
    
        elif value_type == Time and value.startswith("10:"):
            time_string = value[len("10:"):]
            time_milliseconds = int(time_string)
            return self.convert_time(time_milliseconds)

        elif value_type == DateTime and value.startswith("10:"):
            datetime_string = value[len("10:"):]
            milliseconds = int(datetime_string)
            return self.convert_datetime(milliseconds)
        
        elif value_type == Float:
            return self.convert_encoded_float(value)
        
        elif value_type == String:
            return value[1:]
        
        return value

    def fetchall(self):
        all_row_values = []
        column_info = [(desc[0], desc[1]) for desc in self._description]
        for row in self._rows:
            row_ordered_values = []
            for col_name, col_type in column_info:
                value = row.get(col_name)
                if value != None:
                    row_ordered_values.append(self.type_conversion(value,col_type))
                else :
                    row_ordered_values.append(None)
               
            all_row_values.append(row_ordered_values)  
        self._rows = [] 
        return all_row_values


    def close(self):
        self._rows = []
        logger.info("Cursor closed.")

def connect(connection_string, username, password):
    return CouchbaseConnection(connection_string, username, password,)
