import requests
import json
import psycopg2
from azure.storage.blob import BlobServiceClient
import logging
import azure.functions as func
from datetime import datetime

# Access the environment variables
table_id = ''
api_key = ''
base_id = ''
contract_start_date_field = ''
logging_country_field = ''
product_gifted_field = ''
connection_string = ''
container_name = ''
blob_name = ''
webhook_id = ''
host = ""
database = ""
user = ""
password = ""
#Read cursor number for current query 
def read_cursor_number():
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)
    # Get a reference to the blob
    blob_client = container_client.get_blob_client(blob_name)
    # Download the blob contents
    blob_contents = blob_client.download_blob().readall()
    # Convert the blob contents to a string or parse as JSON
    value = blob_contents.decode()  # Assuming the blob contains a string
    return value
    
#Upate cursor number for next query 
def update_cursor_number(cursor):
    # Store cursor in a blob
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(str(cursor), overwrite=True)


def get_latest_payloads():
    # Get accurate cursor number
    cursor = read_cursor_number()
    logging.info(cursor)
    # API endpoint URL
    url = f'https://api.airtable.com/v0/bases/{base_id}/webhooks/{webhook_id}/payloads'

    # Set headers with authentication token
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    # We need to store cursor somewhere
    params = {
        "cursor": cursor
    }

    # Send GET request to retrieve payload data
    response = requests.get(url, headers=headers, params=params)
    response_data = response.json()
    payloads = response_data.get("payloads")
    if not payloads:
        logging.info('No Data to Process')
        return [], "", "", ""

    products_gifted = []
    logging_country = ""
    contract_start_date = ""
    record_id = ""

    for payload in payloads:
        changed_tables = payload.get("changedTablesById")
        if changed_tables and table_id in changed_tables:
            changed_records = changed_tables[table_id].get("changedRecordsById")
            if changed_records:
                for record_id, record_data in changed_records.items():
                    current_values = record_data.get("current", {}).get("cellValuesByFieldId", {}).get(product_gifted_field)
                    if current_values:
                        for value in current_values:
                            product_name = value.get("name")
                            if product_name:
                                products_gifted.append(product_name)

                        logging_country_value = record_data.get("unchanged", {}).get("cellValuesByFieldId", {}).get(logging_country_field)
                        if logging_country_value:
                            for value in logging_country_value:
                                logging_country = value.get("name")

                        contract_start_date = record_data.get("unchanged", {}).get("cellValuesByFieldId", {}).get(contract_start_date_field)

    # Create a DataFrame from the extracted values
    logging.info(products_gifted)
    logging.info(logging_country)
    logging.info(contract_start_date)
    next_cursor = response_data.get('cursor')
    update_cursor_number(next_cursor)
    return products_gifted, logging_country, contract_start_date, record_id



def get_redshift_value(products_gifted, logging_country, contract_start_date):
    product_dict = {}
    total_sum = 0
    #I want to iterate over each product now
    for product in products_gifted:
        parts = product.split('--')
        name = parts[0].strip()
        quantity = float(parts[1].strip().split(' ')[-1][1:])
        # Establish a connection to the database
        conn = psycopg2.connect(
            host=host,
            port=5439,
            database=database,
            user=user,
            password=password
        )
        # Create a cursor object
        cur = conn.cursor()

        # SQL query
        query = f"""
WITH flps_fixed AS (
        SELECT
            country_code,
            CASE 
                WHEN detailed_category = 'Beds' THEN 
                    CASE 
                        WHEN CHARINDEX('-', product_name) > 0 THEN SUBSTRING(product_name, 1, CHARINDEX('-', product_name) - 1)
                        ELSE product_name
                    END
                ELSE product_name
            END AS product_name_fixed,
            order_id,
            SUM(net_revenue) AS net_revenue,
            SUM(purchase_cost) AS purchase_cost,
            SUM(logistic_cost) AS logistic_cost,
            SUM(psp_costs) AS psp_costs,
            SUM(
                CASE
                    WHEN detailed_category = 'Beds' AND product_group_l4 != 'Lead SKU' THEN 0
                    ELSE qty_ordered
                END
            ) AS qty_ordered
        FROM redshift_table_name
        WHERE
            lower(product_name_fixed) = lower('{name}')
            AND country_code = '{logging_country}'
            AND order_date <= '{contract_start_date}'
        GROUP BY 1, 2, 3
        ORDER BY 1 DESC
    ), ranked AS (
        SELECT
            product_name_fixed,
            NVL((NVL(net_revenue, 0) + NVL(purchase_cost, 0) + NVL(logistic_cost, 0) + NVL(psp_costs, 0)) / NULLIF(qty_ordered, 0), 0) AS retail_price,
            RANK() OVER (PARTITION BY product_name_fixed ORDER BY retail_price DESC) AS rank
        FROM flps_fixed
    )

    SELECT 
        nvl(sum(retail_price),0) as retail_price
    FROM ranked
    WHERE rank = 1
    
        
        """
        # Execute the query
        cur.execute(query)

        # Fetch all the rows
        rows = cur.fetchall()
        logging.info(repr(rows))
        if rows == []:
            value = 0
        else:
            value = rows[0][0]
        product_dict[name] = value
        total_sum = total_sum + (value * quantity)
    total_sum = repr(total_sum)
    product_dict = repr(product_dict)
    logging.info(repr(total_sum))
    logging.info(repr(product_dict))    
    return total_sum, product_dict


def push_data_to_airtable(record_id, product_value,product_dict):
    
   # Create the data payload
    data = {
        "records": [
            {
                "id": record_id,
                "fields": {
                    "Products Gifted Value": product_value,
                    "Products Gifted Price Breakdown": product_dict
                }
            }
        ]
    }

    payload = json.dumps(data)
    url = 'https://api.airtable.com/v0/{base_id}/Table_name'

    # Set the authentication token
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }


    # Send the PATCH request
    response = requests.patch(url, headers=headers, data=payload)

    # Check the response status
    if response.status_code == 200:
        logging.info('Data pushed successfully.')
    else:
        logging.info (str(response.status_code))
        logging.info(str(response.reason))
        logging.info(str(response.text))
        logging.info('Data push failed.')

#Steps set up HTTP trigger
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.info('Get data from webhook payload')
    products_gifted, logging_country, contract_start_date, record_id = get_latest_payloads()
    #Push Calculating to airtable
    logging.info("Telling people we are calculating data")
    push_data_to_airtable(record_id, "Calculating","Compiling")
    # Check if products_gifted is empty
    if not products_gifted:
        product_value = '0'
        product_dict = 'No Data Available for this Product'
        logging.info("Pushing Value of Product Gifted")
        push_data_to_airtable(record_id, product_value,product_dict)
        logging.info("Push Complete") 
        return func.HttpResponse(f"This HTTP triggered function executed successfully.")
    #Check if logging_country is empty
    if not logging_country:
        logging.info('Logging country is empty. Skipping further processing.')
        return func.HttpResponse(f"Logging country is empty. Skipping further processing.")
    #Assigning contract start date to todays date if left blanck
    if not contract_start_date:
        today = datetime.now().date()
        contract_start_date = today.strftime('%Y-%m-%d')
    logging.info("Received Data")
    logging.info("Computing Product Gifted Value")
    product_value,product_dict = get_redshift_value(products_gifted, logging_country, contract_start_date)
    logging.info("Pushing Value of Product Gifted")
    push_data_to_airtable(record_id, product_value,product_dict)
    logging.info("Push Complete") 
    return func.HttpResponse(f"This HTTP triggered function executed successfully.")