# Microsoft Azure Webhooks

The idea was to create a way where the an update in a cell in Airtable will cause an update in another cell of the same row with data from Redshift. In order to make these updates live, we decided to create webhooks with Microsoft Azure Functions App. There are two steps basically. 

## Step 1:Creating the Webhook 
Here we create the webhook, mention the notifications url, specify which cells we need to watch (so which changes do we want to notice in order update the other cell in Airtable. 

## Step 2: Processing the data from Airtable
Here we you the Airtable API and get data from the specific row of the airtable. The cell with the changes has will be sent used as a filter in the redshift table to compute a final value that i will upload to Airtable
