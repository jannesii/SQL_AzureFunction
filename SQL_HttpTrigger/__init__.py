import logging
import os
import pymssql
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    # os.environ['SQLCONNSTR']
    server = os.environ['SQLSERVER']
    database = os.environ['SQLDATABASE']
    username = os.environ['SQLUSERNAME']
    password = os.environ['SQLPASSWORD']

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
        print(conn)
        # Create a cursor object
        cursor = conn.cursor()

        # Execute a query
        cursor.execute(query)

        # Fetch and print the result
        rows = cursor.fetchall()

        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])

        # Prepare the result for response
        result = df  # [list(row) for row in rows]

        if df.empty:
            return func.HttpResponse("No data found", status_code=200)
        else:
            return func.HttpResponse(f"Query result: \n\n{result}", status_code=200)
        
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error connecting to database: {e}", status_code=500)
    finally:
        # Close the connection
        conn.close()
