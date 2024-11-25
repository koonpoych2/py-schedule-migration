import schedule
import time
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import logging
from tqdm import tqdm
from datetime import datetime
import os
import csv
import yaml
import urllib.parse

class CollisionLogger:
    def __init__(self):
        """Initialize collision logger with timestamp-based filename"""
        self.timestamp = datetime.now().strftime('%Y-%m-%d')
        self.log_file = f'collision_log_{self.timestamp}.csv'
        self._create_log_file()
        
    def _create_log_file(self):
        """Create collision log file with headers"""
        headers = ['timestamp', 'source_db', 'table_name', 'primary_key', 'collision_type']
        # Check if the file exists and is not empty
        if not os.path.exists(self.log_file) or os.path.getsize(self.log_file) == 0:
            with open(self.log_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
            
    def log_collision(self, source_db, table_name, primary_key, collision_type):
        """Log a collision event"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([timestamp, source_db, table_name, primary_key, collision_type])

class DatabaseMigrator:
    def __init__(self, source_config, target_config):
        """Initialize database connections"""
        try :
            self.source_config = source_config
            self.target_config = target_config
            self.collision_logger = CollisionLogger()
            self.setup_logging()
        except Exception as e :
            self.logger.error(f"Initialize failed: {str(e)}")
            raise
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('migration.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging
        
    def connect_db(self, config):
        """Create database connection"""
        try:
            connection = mysql.connector.connect(
                host=config['host'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                port=config['port']
            )
            return connection
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            raise
            
    def get_tables(self, connection):
        """Get list of tables from database"""
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = [table[0] for table in cursor.fetchall()]
        cursor.close()
        return tables
    
    def get_primary_key_column(self, connection, table_name):
        try:
            """Assume the primary key is always 'id'"""
            pk_column = 'id'  # Default primary key column name
            cursor = connection.cursor()
            cursor.execute(f"DESCRIBE {table_name}")
            table_columns = {column[0] for column in cursor.fetchall()}
            cursor.close()
            
            if pk_column in table_columns :
                return pk_column
            else:
                self.logger.warning(f"No primary key found for table {table_name}. Proceeding without collision detection.")
                return None
        except Exception as e:
            self.logger.error(f"Error getting primary key for table {table_name}: {str(e)}")
            return None
    
    def get_primary_keys(self, connection, table_name):
        """Get all primary keys from a table"""
        pk_column = 'id'  # Primary key is fixed
        cursor = connection.cursor()
        try:
            cursor.execute(f"SELECT {pk_column} FROM {table_name}")
            keys = {row[0] for row in cursor.fetchall()}
        except Exception as e:
            self.logger.error(f"Error fetching primary keys for table {table_name}: {str(e)}")
            keys = set()
        finally:
            cursor.close()
        return keys
    
    def check_table_exists(self, connection, table_name):
        """Check if a table exists in the target database"""
        cursor = connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    
    def create_connection_string(self, config):
        """
        Create a safe MySQL connection string
        Properly handle special characters in password
        """
        # URL encode the password to handle special characters
        encoded_password = urllib.parse.quote_plus(config['password'])
        
        connection_string = (
            f"mysql+mysqlconnector://{config['user']}:{encoded_password}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
        return connection_string

    
    def migrate_table(self, table_name, collisions, source_name, batch_size=1000):
        """Migrate single table data using INSERT IGNORE for duplicates"""
        try:
            source_conn = self.connect_db(self.source_config)
            target_conn = self.connect_db(self.target_config)

            # Check if table exists in the target database
            if not self.check_table_exists(target_conn, table_name):
                self.logger.warning(f"Table {table_name} not found in target. Skipping migration.")
                return  # Skip the migration for this table

            # Get primary key column name
            pk_column = self.get_primary_key_column(source_conn, table_name)

            # Get total rows for progress bar
            cursor = source_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_rows = cursor.fetchone()[0]

            # Create SQLAlchemy engines for pandas using the safe connection string method
            source_engine = create_engine(
                self.create_connection_string(self.source_config),
                pool_pre_ping=True,
                pool_recycle=3600
            )

            # Get target table columns and their properties
            target_cursor = target_conn.cursor(dictionary=True)
            target_cursor.execute(f"DESCRIBE {table_name}")

            # Process the fetched data into two dictionaries
            target_column_info = {}
            column_defaults = {}

            for col in target_cursor.fetchall():
                target_column_info[col['Field']] = col['Type']  # Column type mapping
                column_defaults[col['Field']] = col['Default']  # Column default value mapping

            target_columns = set(target_column_info.keys())  # Set of target column names
            target_cursor.close()

            # Migrate in batches
            for offset in tqdm(range(0, total_rows, batch_size), desc=f"Migrating {table_name}"):
                query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                df = pd.read_sql(query, source_engine)
         
                if not df.empty:

                    # Ensure DataFrame columns match target table columns
                    missing_cols = target_columns - set(df.columns)
                    extra_cols = set(df.columns) - target_columns

                    if missing_cols:
                        self.logger.warning(f"Missing columns in DataFrame for table {table_name}: {missing_cols}")
     

                    if extra_cols:
                        self.logger.warning(f"Extra columns in DataFrame for table {table_name}: {extra_cols}")
                        df = df.drop(columns=extra_cols)
         
                    

                    # Define a robust NaN handling function
                    def convert_value(value, column_name):
                        if pd.isna(value) or (isinstance(value, float) and np.isnan(value)):
                            # Check column type and replace NaN with appropriate value
                            col_type = target_column_info.get(column_name, '').lower()
                            default_value = column_defaults.get(column_name)

                            if 'decimal' in col_type or 'float' in col_type:
                                # Replace NaN with the default value, or None if NULL is allowed
                                return default_value if default_value is not None else 0
                            elif 'int' in col_type or 'tinyint' in col_type:
                                # Replace NaN with 0 or default value
                                return default_value if default_value is not None else 0
                            elif 'char' in col_type or 'text' in col_type:
                                # Replace NaN with an empty string or default value
                                return default_value if default_value is not None else ''
                            elif 'date' in col_type or 'time' in col_type:
                                # Replace NaN with default date/time value or None
                                return default_value if default_value is not None else None

                            return None  # Fallback for other types

                        return value

                    # Apply NaN conversion to each column
                    for col in df.columns:
                        df[col] = df[col].apply(lambda x: convert_value(x, col))

                    
                    # Prepare INSERT IGNORE query with escaped column names
                    insert_query = f"INSERT IGNORE INTO {table_name} ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"


                    with target_conn.cursor() as target_cursor:
                        for _, row in df.iterrows():
                            # Check for collisions if primary key exists
                            # Print row to check if None is passed as NULL
                            if pk_column and row[pk_column] in collisions:
                                self.collision_logger.log_collision(
                                    source_name,
                                    table_name,
                                    row[pk_column],
                                    'cross_database_collision'
                                )
                                continue

                            # Execute INSERT IGNORE for each row
                            try:
                                target_cursor.execute(insert_query, tuple(row))
                            except Exception as e:
                                self.logger.error(f"Failed to insert row {row.to_dict()} into {table_name}: {str(e)}")
                                continue

                        target_conn.commit()

            self.logger.info(f"Successfully migrated table: {table_name}")

        except Exception as e:
            self.logger.error(f"Error migrating table {table_name}: {str(e)}")
            raise
        finally:
            source_conn.close()
            target_conn.close()
    


def get_collision_primary_keys(config):
    collision = {}
    all_pk = {} 
    for source_name, source_config in config['sources'].items():
        tables = source_config['tables']
        migrator = DatabaseMigrator(source_config, config['target'])
        available_tables = migrator.get_tables(migrator.connect_db(source_config))

        if source_name not in all_pk:
            all_pk[source_name] = {}  
        
        for table in tables:
            if table in available_tables:
                connection = migrator.connect_db(source_config)
                
                # Check if table has primary key
                pk_column = 'id'
                if not pk_column:
                    continue  # Skip collision detection for tables without primary key
                    
                primary_keys = migrator.get_primary_keys(connection, table)
                
                for other_source in all_pk:
                    if other_source != source_name:
                        collision_pk = primary_keys.intersection(all_pk[other_source].get(table, set()))
                        if collision_pk:
                            if table not in collision:
                                collision[table] = set()  
                            collision[table] |= collision_pk


                all_pk[source_name][table] = primary_keys
                connection.close()
    return collision
def run_migration():
    """Function to run the migration process"""
    logging.info(f"Starting scheduled migration at {datetime.now()}")
    try:
        # Load configuration
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        collisions = get_collision_primary_keys(config)
        
        for source_name, source_config in config['sources'].items():
            logging.info(f"Processing {source_name}")
            try:
                tables = source_config.pop('tables')
                migrator = DatabaseMigrator(source_config, config['target'])
                available_tables = migrator.get_tables(migrator.connect_db(source_config))
                
                for table in tables:
                    if table in available_tables:
                        if table in collisions and collisions[table]:
                            migrator.migrate_table(table, collisions[table], source_name)
                        else:
                            migrator.migrate_table(table, set(), source_name)
                    else:
                        logging.warning(f"Table '{table}' not found in {source_name}")
            except Exception as e:
                logging.error(f"Failed to process {source_name}: {str(e)}")
                continue
                
        logging.info(f"Scheduled migration completed at {datetime.now()}")
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('migration_scheduler.log'),
            logging.StreamHandler()
        ]
    )
    
    # Schedule the migration to run at midnight (00:00)
    schedule.every().day.at("00:00").do(run_migration)
    
    logging.info("Migration scheduler started. Waiting for scheduled time...")
    
    # Run the scheduler
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logging.info("Scheduler stopped by user")
            break
        except Exception as e:
            logging.error(f"Scheduler error: {str(e)}")
            # Wait for 5 minutes before retrying in case of error
            time.sleep(300)