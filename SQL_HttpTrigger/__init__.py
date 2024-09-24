import logging
import os
import pymssql
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get SQL connection parameters from environment variables
    server = os.environ.get('SQLSERVER')
    database = os.environ.get('SQLDATABASE')
    username = os.environ.get('SQLUSERNAME')
    password = os.environ.get('SQLPASSWORD')

    if not all([server, database, username, password]):
        return func.HttpResponse(
            "Missing one or more environment variables (SQLSERVER, SQLDATABASE, SQLUSERNAME, SQLPASSWORD).",
            status_code=500
        )

    # Extract the SQL query from the request body or query string
    try:
        req_body = req.get_json()
        query = req_body.get('query')
    except ValueError:
        query = req.params.get('query')

    if not query:
        return func.HttpResponse(
            "Please provide a SQL query in the request body or query string.",
            status_code=400
        )

    try:
        # Connect to the Azure SQL Database
        conn = pymssql.connect(server=server, user=username,
                               password=password, database=database)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch all rows from the executed query
        rows = cursor.fetchall()

        # Create a DataFrame from the query results
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])

        # Check if the DataFrame is empty
        if df.empty:
            return func.HttpResponse("No data found", status_code=200)

        # Convert the DataFrame to a string for returning in the HTTP response
        result_str = df.to_string(index=False)

        return func.HttpResponse(f"Query result: \n\n{result_str}", status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return func.HttpResponse(f"Error connecting to database: {str(e)}", status_code=500)

    finally:
        # Ensure the connection is closed
        if 'conn' in locals():
            conn.close()
