import requests
import os
import logging
from elasticsearch import Elasticsearch, helpers
import datetime

logging.basicConfig(filename='app.log', level="INFO", format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

purchase_index = "purchases-test"

# Create an instance of Elasticsearch
es = Elasticsearch(os.environ.get("es_server"), api_key=os.environ.get("es_api"))

full_item_type = {
    "t": "track",
    "a": "album",
    "p": "merch",
    "b": "bundle"
}

bulk_purchases = []

# fuction name: get_sales_from_bc purpose: get sales from api  and iterate through the purchases to index purchases in elastic search
def get_sales_from_bc():
    try:
        # get the sales from the bandcamp API
        response = requests.get("https://bandcamp.com/api/salesfeed/1/get")
        response = response.json()
        response = response["events"]
    
        logging.info("Sales fetched from Bandcamp API")
    
        return response
    
    except Exception as e:
        logging.error(f"Error fetching sales from Bandcamp API: {e}")
        return []


def add_purchases_to_bulk(purchases):
    for per_event_purchase in purchases:
        for individual_purchase in per_event_purchase['items']:
            ready_purchase = clean_purchase(individual_purchase)
            bulk_purchases.append(ready_purchase)

def clean_purchase(purchase):

    utc_datetime = datetime.datetime.fromtimestamp(purchase["utc_date"])
    cleaned_purchase = {
        "utc_date": purchase["utc_date"],
        "artist_name": purchase["artist_name"],
        "item_description": purchase["item_description"],
        "album_title": purchase["album_title"],
        "currency": purchase["currency"],
        "amount_paid": purchase["amount_paid"],
        "item_price": purchase["item_price"],
        "amount_paid_usd": purchase["amount_paid_usd"],
        "country": purchase["country"],
        "timestamp":  utc_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "country_code":  None or purchase["country_code"].upper(),
        "account_username":  purchase["url"].split("//")[1].split(".bandcamp.com")[0],
        "item_price":  round(purchase["item_price"], 2),
        "amount_paid":  round(purchase["amount_paid"], 2),
        "amount_paid_usd":  round(purchase["amount_paid_usd"], 2),
        "item_type":  full_item_type[purchase["item_type"]],
    }

    return cleaned_purchase


def bulk_add_purchases_to_elastic(purchases):
    try:
        actions = []
        for purchase in purchases:
            action = {
                "_index": purchase_index,
                "_id": purchase["utc_date"],
                "_source": purchase
            }
            actions.append(action)
        
        response = helpers.bulk(es, actions)
        if response[1]:
            logger.error("Error adding purchases to Elasticsearch")
            logger.error(response[1])
        else:
            print(f"Purchases added to Elasticsearch: {response[0]}")
            logger.info("Purchases added to Elasticsearch")
    
    except Exception as e:
        logger.error(f"Error adding purchases to Elasticsearch: {e}")


def remove_art_url_from_purchases():
    try:
        # Retrieve all documents from the purchases index
        query = {
            "query": {
                "match_all": {}
            },
            "size": 10000  # Increase the size to retrieve more documents at once
        }
        response = es.search(index=purchase_index, body=query, scroll="5m")
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]

        # Prepare bulk update requests
        bulk_requests = []

        # Remove the art_url field from each document
        for hit in hits:
            doc_id = hit["_id"]
            doc = hit["_source"]
            if "item_type" in doc:
                del doc["item_type"]
                # Add update request to bulk requests
                bulk_requests.append({
                    "_op_type": "update",
                    "_index": purchase_index,
                    "_id": doc_id,
                    "doc": doc
                })

        # Execute bulk update requests
        response = helpers.bulk(es, bulk_requests)

        if response[1]:
            logger.error("Error removing art_url field from purchases")
            logger.error(response[1])
        else:
            print(f"Art_url field removed from purchases: {response[0]}")
            logger.info("Art_url field removed from purchases")

        # Scroll through the remaining documents
        while hits:
            response = es.scroll(scroll_id=scroll_id, scroll="5m")
            scroll_id = response["_scroll_id"]
            hits = response["hits"]["hits"]

            # Prepare bulk update requests
            bulk_requests = []

            # Remove the art_url field from each document
            for hit in hits:
                doc_id = hit["_id"]
                doc = hit["_source"]
                if "item_type" in doc:
                    del doc["item_type"]
                    # Add update request to bulk requests
                    bulk_requests.append({
                        "_op_type": "update",
                        "_index": purchase_index,
                        "_id": doc_id,
                        "doc": doc
                    })

            # Execute bulk update requests
            response = helpers.bulk(es, bulk_requests)
            print(response)
            if response[1]:
                logger.error("Error removing art_url field from purchases")
                logger.error(response[1])
            else:
                print(f"Art_url field removed from purchases: {response[0]}")
                logger.info("Art_url field removed from purchases")

    except Exception as e:
        logger.error(f"Error removing art_url field from purchases: {e}")


purchases = get_sales_from_bc()
add_purchases_to_bulk(purchases)
bulk_add_purchases_to_elastic(bulk_purchases)

logger.info("Job's Done!")
