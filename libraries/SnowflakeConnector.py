import snowflake.connector
from typing import Optional, List, Dict
from robot.api import logger

class SnowflakeConnector:
    """
    A class to handle Snowflake database connections and query execution with testing capabilities
    """
    
    def __init__(self, account: str, user: str, password: str, 
                 warehouse: str, database: str, schema: str):
        """
        Initialize Snowflake connection parameters
        
        Args:
            account: Snowflake account identifier
            user: Username for authentication
            password: Password for authentication
            warehouse: Snowflake warehouse name
            database: Database name
            schema: Schema name
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """
        Establish connection to Snowflake
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            self.cursor = self.connection.cursor()
            logger.console("Successfully connected to Snowflake")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {str(e)}")
            return False

    def execute_query(self, query: str) -> Optional[List[Dict]]:
        """
        Execute a SQL query and return results
        
        Args:
            query: SQL query to execute
            
        Returns:
            Optional[List[Dict]]: Query results as list of dictionaries, None if error
        """
        try:
            if not self.connection or not self.cursor:
                if not self.connect():
                    return None
                    
            self.cursor.execute(query)
            # Get column names
            columns = [col[0] for col in self.cursor.description]
            # Fetch results and convert to list of dictionaries
            results = [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
            logger.console(f"Successfully executed query: {query[:100]}...")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return None

    def close(self):
        """Close Snowflake connection and cursor"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            logger.console("Successfully closed Snowflake connection")
            
        except Exception as e:
            logger.error(f"Error closing connection: {str(e)}")

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
