import requests
import logging
import azure.functions as func
import json

# Access the environment variables
table_id = ''
api_key = ''
base_key = ''
contract_start_date = ''
logging_country = ''
product_gifted = ''



def create_webhook():
    data = {
        "notificationUrl": 'https://im-products-gifted-automation-prod.azurewebsites.net/api/webhook',
        "specification": {
            "options": {
                "filters": {
                    "dataTypes": ["tableData"],
                    "recordChangeScope": table_id,
                    "watchDataInFieldIds": [
                        product_gifted
                    ],
                },
                "includes": {
                    "includeCellValuesInFieldIds": [
                        product_gifted,
                        logging_country,
                        contract_start_date
                    ]
                }
            }
        }
    }

    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    create_webhook_url = f'https://api.airtable.com/v0/bases/{base_key}/webhooks'
    response = requests.post(create_webhook_url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        logging.warning('Webhook created successfully')
        logging.info(response.text)
        return response.text, 200
    else:
        logging.warning('Error creating webhook: %s', response.text)
        return response.text, 500


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    create_webhook()
    
    return func.HttpResponse(f"This HTTP triggered function executed successfully.")