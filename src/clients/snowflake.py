import snowflake.connector

class Snowflake:
    def __init__(self, **kwargs) -> None:
        self.account = kwargs.get("account", None)
        self.user = kwargs.get("user", None)
        self.password = kwargs.get("password", None)
        self.role = kwargs.get("role", None)
        self.warehouse = kwargs.get("warehouse", None)
        self.conn = self._get_conn()
        
    def _get_conn(self):
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            role=self.role,
            warehouse=self.warehouse
        )
    
    def __enter__(self):
        return self
    
    def __exit__(self):
        self.conn.close()
                
    def query(self, query):
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            print(f"Executed query: {query}")
            print(
                f"Number of rows returned: {cursor.rowcount}"
                if cursor.rowcount > 0 else "No rows returned"
            )
            
            results = cursor.fetchall()
        return results
    
    def copy(self):
        query = """
            COPY INTO @%s
            FROM @%s
            FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',')
        """
        
    def assert_rows(self, table):
        query = f"""
            SELECT COUNT(*)
            FROM {table}
        """
        return self.query(query)