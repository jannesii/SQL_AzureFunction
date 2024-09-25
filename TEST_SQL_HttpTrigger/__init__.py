import logging
import pymssql
import pandas as pd
import azure.functions as func
import traceback

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Extract the SQL query from the request body or query string
        try:
            req_body = req.get_json()
            query = req_body.get('query')
            logging.info(f"Received query: {query}")
        except ValueError:
            query = req.params.get('query')

        # Get SQL connection parameters from the route
        server = f"{req.route_params.get('server')}.database.windows.net"
        database = req.route_params.get('database')
        username = req.route_params.get('username')
        password = req.route_params.get('password')

        logging.info(f"Connection details - Server: {server}, Database: {database}, User: {username}")

        # Ensure that all parameters are present
        if not all([server, database, username, password, query]):
            logging.error("Missing one or more required parameters.")
            return func.HttpResponse(
                "Missing one or more environment variables (SERVER, DATABASE, USERNAME, PASSWORD, SQLQUERY).",
                status_code=500
            )

        try:
            # Connect to the Azure SQL Database
            logging.info("Connecting to database...")
            conn = pymssql.connect(server=server, user=username,
                                   password=password, database=database)
            cursor = conn.cursor()

            # Execute the query
            logging.info(f"Executing query: {query}")
            cursor.execute(query)

            # Check if the query is an INSERT, UPDATE, or DELETE
            if query.strip().lower().startswith(('insert', 'update', 'delete')):
                # Commit the changes for data-modifying queries
                logging.info("Committing changes...")
                conn.commit()
                return func.HttpResponse(f"Query executed successfully: {query}", status_code=200)

            # If it's a SELECT query, fetch the result set
            rows = cursor.fetchall()
            logging.info(f"Query returned {len(rows)} rows.")

            # Create a DataFrame from the query results
            df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])

            # Check if the DataFrame is empty
            if df.empty:
                logging.info("No data found.")
                return func.HttpResponse("No data found", status_code=200)

            # Convert the DataFrame to a string for returning in the HTTP response
            result_str = df.to_string(index=False)

            return func.HttpResponse(f"Query result: \n\n{result_str}", status_code=200)

        except Exception as e:
            logging.error(f"Error executing query: {traceback.format_exc()}")
            return func.HttpResponse(f"Error executing query: {str(e)}\nQuery: {query}", status_code=500)

        finally:
            # Ensure the connection is closed
            if 'conn' in locals():
                logging.info("Closing connection.")
                conn.close()

    except Exception as e:
        logging.error(f"General error occurred: {traceback.format_exc()}")
        return func.HttpResponse(f"Error occurred: {str(e)}", status_code=500)
