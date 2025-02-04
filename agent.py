import os
from dotenv import load_dotenv
from openai import OpenAI
from connect_db import DbConnection

load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
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
    """Generate SQL query based on user input"""
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = f"""
    You are a Database Expert. Given the following database schema:
    {schema}
    and the user question: {user_input}
    generate the SQL query to answer the question. If the question cannot be answered, return "Can't Answer".
    """
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": "you are a database expert"},
            {"role": "user", "content": prompt}],
        max_tokens=200,
        temperature=0.1
    )
    return response.choices[0].message.content.strip()

def execute_query(engine, query):
    """Execute the SQL query and return the results."""
    try:
        with engine.connect() as connection:
            result = connection.execute(query)
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
            for row in result:
                print(row)
        else:
            print("\nThe question cannot be answered.")

if __name__ == "__main__":
    main()