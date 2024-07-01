import requests
import os

import logging
import elasticsearch
import datetime

logging.basicConfig(filename='app.log', level="INFO", format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

# Create an instance of Elasticsearch
es = elasticsearch.Elasticsearch(os.environ.get("es_server"), api_key=os.environ.get("es_api"))

# fuction name: get_sales_from_bc purpose: get sales from api every 2 minutes and iterate through the purchases and index them in elastic search
def get_sales_from_bc():
    try:
        # get the sales from the bandcamp API
        response = requests.get("https://bandcamp.com/api/salesfeed/1/get?start=0&count=50")
        response = response.json()
        response = response["events"]
    
        logging.info("Sales fetched from Bandcamp API")
    
        return response
    
    except Exception as e:
        logging.error(f"Error fetching sales from Bandcamp API: {e}")
        return []

def add_purchases(purchases):
    for purchase in purchases:
        # handle case of multiple items per purchase
        if type(purchase) == list:
            for item in purchase:
                add_purchase(item)
        add_purchase(purchase)

    print(f"Purchases added: {len(purchases)}") 

def clean_purchase(purchase):
    purchase = purchase["items"][0]
    purchase_utc = purchase["utc_date"]
    utc_datetime = datetime.datetime.fromtimestamp(purchase_utc)
    timestamp = utc_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    stripped_purchase = {
        "utc_date": purchase["utc_date"],
        "timestamp": timestamp,
        "artist_name": purchase["artist_name"],
        "album_title": purchase["album_title"],
        "item_type": purchase["item_type"],
        "item_description": purchase["item_description"],
        "currency": purchase["currency"],
        "item_price": purchase["item_price"],
        "amount_paid": purchase["amount_paid"],
        "amount_paid_usd": round(purchase["amount_paid_usd"], 2),
        "country": purchase["country"] or None,
        "country_code": purchase["country_code"].upper(),
        "url": "https:" + purchase["url"],
        "art_url": purchase["art_url"],
        "account_username": purchase["url"].split("//")[1].split(".bandcamp.com")[0],
    }

    return stripped_purchase


def add_purchase(purchase):
    """
    Endpoint to add a purchase.

    Args:
        purchase (Purchase): The purchase object.

    Returns:
        dict: A dictionary with a success message and the payload of the purchase.
    """
    
    # prep purchase for adding to es index 
    purchase = clean_purchase(purchase)

    # Add the purchase to the purchases index
    new_purchase = es.index(index="purchases", body=purchase)
    logger.info(f"Purchase added: {purchase['album_title']}")
    
    # Return a success message
    return {
        "message": "Purchase added successfully",
        "artist_update": bool(new_purchase["_shards"]["successful"]),
    }

purchases = get_sales_from_bc()
add_purchases(purchases)
logger.info("Job's Done!")
