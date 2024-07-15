## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

What things you need to install the software and how to install them:

- Python 3.x
- Elasticsearch
- Requests library

You can install the necessary Python libraries using pip:

```bash
pip install elasticsearch requests
```

### Setting Up

1. **Elasticsearch Server**: Ensure you have Elasticsearch running. You can either install Elasticsearch locally or use a cloud-based service. Set the `es_server` environment variable to your Elasticsearch server URL.

2. **API Key**: If your Elasticsearch server requires an API key, set the `es_api` environment variable with your API key.

3. **Environment Variables**: Set up the necessary environment variables. You can do this by running:

```bash
export es_server='your_elasticsearch_server_url'
export es_api='your_elasticsearch_api_key'
```

### Running the Code

To run the code, simply execute the `main.py` script:

```bash
python main.py
```

## Code Overview

### `main.py`
This is the main script of the project. It includes the necessary imports, setup for logging, and the definition of the `get_sales_from_bc` function.

#### Imports

- `requests`: Used to make HTTP requests to the Bandcamp API.
- `os`: Used to access environment variables.
- `logging`: Used for logging information and errors.
- `elasticsearch`, `helpers`: Used to interact with Elasticsearch.
- `datetime`: Used for handling dates and times.

#### Logging Setup

Configures logging to write to `app.log`with a specific format.

#### Elasticsearch Instance

Creates an instance of Elasticsearch configured with the server URL and API key from environment variables.

#### `full_item_type` Dictionary
Maps short codes to item types (e.g., "t" to "track").

#### `bulk_purchases` List

A list to hold bulk purchase data before indexing in Elasticsearch.

### Function: `get_sales_from_bc`

**Purpose**: Fetches sales data from the Bandcamp API and returns it as a list of events. If an error occurs during the fetch process, it logs the error and returns an empty list.

**Parameters**: None

**Returns**: A list of sales events fetched from the Bandcamp API. Returns an empty list if there's an error.

---

### Function: `add_purchases_to_bulk`

**Purpose**: Iterates through a list of purchases, cleans each purchase using the `clean_purchase` function, and adds it to the global `bulk_purchases` list for bulk indexing.

**Parameters**:
- `purchases`: A list of purchase events to be cleaned and added to the bulk list.

**Returns**: None

---

### Function: `clean_purchase`

**Purpose**: Cleans and transforms a single purchase event's data for indexing in Elasticsearch. This includes converting timestamps, rounding amounts, and mapping item types.

**Parameters**:
- `purchase`: A dictionary representing a single purchase event.

**Returns**: A cleaned dictionary of the purchase event, ready for indexing.

---

### Function: `bulk_add_purchases_to_elastic`

**Purpose**: Takes a list of cleaned purchase events and bulk indexes them into Elasticsearch. Logs the outcome of the bulk indexing operation.

**Parameters**:
- `purchases`: A list of cleaned purchase events to be indexed.

**Returns**: None. Logs errors or success messages to the application log.

---

### Execution Flow

1. **Fetch Sales Data**: Calls `get_sales_from_bc` to fetch sales data from the Bandcamp API.
2. **Prepare for Bulk Indexing**: Uses `add_purchases_to_bulk` to prepare the fetched sales data for bulk indexing.
3. **Bulk Indexing**: Calls `bulk_add_purchases_to_elastic` to index the prepared sales data into Elasticsearch.
4. **Completion Log**: Logs a "Job's Done!" message indicating the end of the script's execution.

## Customization

You can customize the script by modifying the `purchase_index` variable to use a different Elasticsearch index.
