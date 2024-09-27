import logging
import pymssql
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Extract the SQL query and server parameters from the request body
    try:
        req_body = req.get_json()
        query = req_body.get('query')
        server = req_body.get('server')
        database = req_body.get('database')
        username = req_body.get('username')
        password = req_body.get('password')
    except ValueError:
        query = req.params.get('query')

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

        # Check if the query is an INSERT, UPDATE, or DELETE
        if query.strip().lower().startswith(('insert', 'update', 'delete')):
            # Commit the changes for data-modifying queries
            conn.commit()
            return func.HttpResponse(f"Query executed successfully: {query}", status_code=200)

        # If it's a SELECT query, fetch the result set
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
        return func.HttpResponse(f"Error connecting to database: {str(e)}\nQuery: {query}", status_code=500)

    finally:
        # Ensure the connection is closed
        if 'conn' in locals():
            conn.close()
