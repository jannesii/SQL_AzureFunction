import logging

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get the 'sql' parameter from the route
    sql = req.route_params.get('sql')

    if sql:
        # Process the SQL value or send a response
        return func.HttpResponse(f"SQL Parameter: {sql}")
    else:
        return func.HttpResponse(
             "No SQL parameter was provided in the route.",
             status_code=400
        )