from typing import List, Optional, Dict, Any, Tuple, Union
import csv
import pandas as pd
import re
from faker import Faker
import psycopg2
import os
from datetime import datetime
from psycopg2.extensions import connection, cursor

class UserDataETL:
    """
    A class to perform Extract, Transform, Load (ETL) operations for user data.

    This class generates fake user data, transforms it, and loads it into a PostgreSQL database.
    It provides methods for each stage of the ETL process with type-safe operations.

    Attributes:
        num_records (int): Number of fake user records to generate
        fake (Faker): Faker instance for generating synthetic data
        csv_filename (str): Filename for the raw CSV data
        transformed_csv_filename (str): Filename for the transformed CSV data
    """

    def __init__(self, num_records: int = 1000) -> None:
        """
        Initialize the UserDataETL instance.

        Args:
            num_records (int, optional): Number of fake user records to generate. Defaults to 1000.
        """
        self.num_records: int = num_records
        self.fake: Faker = Faker()
        self.csv_filename: str = "fake_data.csv"
        self.transformed_csv_filename: str = "transformed_users_data.csv"

    def generate_csv(self) -> None:
        """
        Generate a CSV file with fake user data.

        Creates a CSV file with columns: user_id, name, email, signup_date
        Uses Faker to generate synthetic, realistic user information.
        """
        with open(self.csv_filename, mode='w', newline='', encoding='utf-8') as file:
            writer: csv.writer = csv.writer(file)
            writer.writerow(['user_id', 'name', 'email', 'signup_date'])
            
            for i in range(1, self.num_records + 1):
                user_id: int = i
                name: str = self.fake.name()
                email: str = self.fake.email()
                signup_date: str = self.fake.date_time_between(
                    start_date='-5y', 
                    end_date='now'
                ).strftime('%Y-%m-%d %H:%M:%S')
                
                writer.writerow([user_id, name, email, signup_date])
        
        print(f"Generated {self.num_records} records in {self.csv_filename}")

    def is_valid_email(self, email: str) -> bool:
        """
        Validate email address using a regular expression.

        Args:
            email (str): Email address to validate

        Returns:
            bool: True if email is valid, False otherwise
        """
        email_regex: str = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        return re.match(email_regex, email) is not None

    def extract_domain(self, email: str) -> Optional[str]:
        """
        Extract domain from an email address.

        Args:
            email (str): Email address to extract domain from

        Returns:
            Optional[str]: Domain part of the email, or None if no domain found
        """
        return email.split('@')[-1] if '@' in email else None

    def transform_data(self) -> pd.DataFrame:
        """
        Transform the generated CSV data.

        Performs the following transformations:
        - Converts signup_date to datetime
        - Filters out invalid emails
        - Extracts email domains

        Returns:
            pd.DataFrame: Transformed user data
        """
        data: pd.DataFrame = pd.read_csv(self.csv_filename)
        data['signup_date'] = pd.to_datetime(data['signup_date']).dt.date
        
        # Filter valid emails
        data = data[data['email'].apply(self.is_valid_email)]
        
        # Extract domain
        data['domain'] = data['email'].apply(self.extract_domain)
        
        # Save transformed data
        data.to_csv(self.transformed_csv_filename, index=False)
        print(f"Transformed data saved to {self.transformed_csv_filename}")
        
        return data

    def load_to_postgresql(
        self, 
        dbname: str, 
        user: str, 
        password: str, 
        host: str, 
        port: int
    ) -> None:
        """
        Load transformed data into PostgreSQL database.

        Args:
            dbname (str): Name of the database
            user (str): Database username
            password (str): Database password
            host (str): Database host
            port (int): Database port
        """
        try:
            # Establish database connection
            conn: connection = psycopg2.connect(
                dbname=dbname, 
                user=user, 
                password=password, 
                host=host, 
                port=port
            )
            cur: cursor = conn.cursor()

            # Create users table if not exists
            create_table_query: str = """
            CREATE TABLE IF NOT EXISTS users (
                user_id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                signup_date TIMESTAMP
            );
            """
            cur.execute(create_table_query)
            conn.commit()

            # Truncate table to avoid duplicate key errors
            cur.execute("TRUNCATE TABLE users RESTART IDENTITY;")
            conn.commit()

            # Read transformed data
            data: pd.DataFrame = pd.read_csv(self.transformed_csv_filename)

            # Prepare and execute batch insert
            insert_query: str = """
            INSERT INTO users (name, email, signup_date) 
            VALUES (%s, %s, %s)
            """
            
            # Convert DataFrame to list of tuples for batch insert
            records: List[Tuple[str, str, datetime]] = data[
                ['name', 'email', 'signup_date']
            ].values.tolist()

            cur.executemany(insert_query, records)
            conn.commit()

            print("Data inserted successfully into PostgreSQL.")

            # Close database connection
            cur.close()
            conn.close()

        except psycopg2.Error as db_error:
            print(f"Database error: {db_error}")
        except Exception as e:
            print(f"Unexpected error during data loading: {e}")

    def run_etl(
        self, 
        dbname: str, 
        user: str, 
        password: str, 
        host: str, 
        port: int
    ) -> None:
        """
        Execute the complete ETL process.

        Args:
            dbname (str): Name of the database
            user (str): Database username
            password (str): Database password
            host (str): Database host
            port (int): Database port
        """
        try:
            self.generate_csv()
            self.transform_data()
            self.load_to_postgresql(dbname, user, password, host, port)
        except Exception as e:
            print(f"ETL process failed: {e}")

def main() -> None:
    """
    Main entry point for the ETL script.

    Reads database configuration from environment variables.
    Runs the ETL process with the configured parameters.
    """
    # Read database configuration from environment variables
    db_config: Dict[str, Union[str, int]] = {
        'dbname': os.getenv('POSTGRES_DB', 'transformed_data_db'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
        'host': os.getenv('POSTGRES_HOST', 'postgres'),
        'port': int(os.getenv('POSTGRES_PORT', 5432))
    }

    # Initialize and run ETL
    etl: UserDataETL = UserDataETL()
    etl.run_etl(**db_config)

if __name__ == "__main__":
    main()