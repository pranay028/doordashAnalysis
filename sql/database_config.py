from sqlalchemy import create_engine, text
import pandas as pd
import urllib.parse

# Format: mysql+mysqlconnector://[user]:[password]@[host]:[port]/[database]

USER = "root" 
PASSWORD = "Root@1234"
ENCODED_PASSWORD = urllib.parse.quote_plus(PASSWORD)
HOST = "localhost" 
PORT = "3306"
DATABASE = "doordash" 

connection_string = f"mysql+mysqlconnector://{USER}:{ENCODED_PASSWORD}@{HOST}:{PORT}/{DATABASE}"
engine = create_engine(connection_string)



def create_and_inject(file_path):
    try:
        # 2. Create the Table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS deliveries (
            id INT AUTO_INCREMENT PRIMARY KEY,
            order_created_time DATETIME,
            store_name VARCHAR(255),
            total_item_count INT,
            subtotal_in_cents BIGINT,
            order_type VARCHAR(100)
        );
        """
        with engine.connect() as conn:
            conn.execute(text(create_table_query))
            print("Table verified/created successfully.")

        # 3. Read and Clean CSV
        df = pd.read_csv(file_path)
        
        # Convert timestamp strings to actual datetime objects
        df['ORDER_CREATED_TIME'] = pd.to_datetime(df['ORDER_CREATED_TIME'])
        
        # Match CSV headers to SQL column names (lowercase)
        df.columns = [col.lower() for col in df.columns]

        # 4. Inject Data
        df.to_sql('deliveries', con=engine, if_exists='append', index=False)
        print(f"Successfully injected {len(df)} rows into the database!")

    except Exception as e:
        print(f"Error occurred: {e}")


create_and_inject('./data/silver/dasher_delivery_information.csv')