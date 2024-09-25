import logging
import os
import pymssql
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get the 'sql' parameter from the route
    # query = req.route_params.get('sqlquery')

    # Extract the SQL query from the request body or query string
    try:
        req_body = req.get_json()
        query = req_body.get('query')
    except ValueError:
        query = req.params.get('query')

    # Get SQL connection parameters from the route
    server = f"{req.route_params.get('server')}.database.windows.net"
    database = req.route_params.get('database')
    username = req.route_params.get('username')
    password = req.route_params.get('password')

    # return func.HttpResponse(f"{server, database, username, password, query}", status_code=200)

    if not all([server, database, username, password, query]):
        return func.HttpResponse(
            "Missing one or more environment variables (SERVER, DATABASE, USERNAME, PASSWORD, SQLQUERY).",
            status_code=500
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
        # return func.HttpResponse(f"Query result: {rows}", status_code=200)

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
