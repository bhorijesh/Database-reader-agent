import os
from dotenv import load_dotenv
from openai import OpenAI
from sqlalchemy.sql import text  
from connect_db import DbConnection

# Load environment variables from .env file
load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Establish database connection
conn = DbConnection().get_server_connection()
engine = conn.engine

def get_database_schema(engine):
    """Extract the schema of the database using SQLAlchemy reflection."""
    from sqlalchemy import MetaData

    metadata = MetaData()
    metadata.reflect(bind=engine)

    schema = {}
    for table_name, table in metadata.tables.items():
        schema[table_name] = {
            "columns": [column.name for column in table.columns],
            "primary_key": [pk.name for pk in table.primary_key],
            "foreign_keys": [
                {
                    "column": fk.parent.name,
                    "references": fk.target_fullname.split(".")[0],
                    "referenced_column": fk.target_fullname.split(".")[1],
                }
                for fk in table.foreign_keys
            ],
        }
    return schema

def generate_sql_query(user_input, schema):
    """Generate SQL query based on user input."""
    prompt = f"""
    You are a Database Expert. Given the following database schema:
    {schema}
    
    And the user question: "{user_input}"
    
    Generate ONLY the SQL query to answer the question. Do not include any explanations or additional text or code block markers like '```sql'.
    Only the SQL query should be returned. 
    If the question cannot be answered, return "Can't Answer".
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a database expert."},
            {"role": "user", "content": prompt},
        ],
        max_tokens=200,
        temperature=0.1,
    )
    # Clean the response to remove code block markers
    return response.choices[0].message.content.strip()

def execute_query(engine, query):
    """Execute the SQL query and return the results."""
    try:
        with engine.connect() as connection:
            # Use text() to safely execute raw SQL queries
            result = connection.execute(text(query))
            rows = result.fetchall()
            return rows
    except Exception as e:
        return f"Error executing query: {str(e)}"

def main():
    print("Welcome to the Database Reader Agent!")
    schema = get_database_schema(engine)

    while True:
        user_input = input("\nAsk me something about the database (or type 'exit' to quit): ")
        if user_input.lower() == "exit":
            break

        # Generate SQL query
        sql_query = generate_sql_query(user_input, schema)
        print(f"\nGenerated SQL Query: {sql_query}")

        # Execute the query and display results
        if sql_query != "Can't Answer":
            result = execute_query(engine, sql_query)
            print("\nQuery Result:")
            if isinstance(result, list):  
                for row in result:
                    print(row)
            else:
                print(result) 
        else:
            print("\nThe question cannot be answered.")

if __name__ == "__main__":
    main()