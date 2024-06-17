import requests
import os
import time
import logging
from elasticsearch import Elasticsearch

logging.basicConfig(filename='app.log', level="INFO", format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

# Create an instance of Elasticsearch
es = Elasticsearch(os.environ.get("es_server"), api_key=os.environ.get("es_api"))

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

    stripped_purchase = {
        "utc_date": purchase["utc_date"],
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

    purchase = clean_purchase(purchase)
    
    # Prepare the update script
    body = {
        "script": {
            "source": """
                ctx._source.purchase_count += params.purchase_count;
                ctx._source.total_revenue += params.total_revenue;
                if (!ctx._source.countries.contains(params.country)) {
                    ctx._source.countries.add(params.country);
                }
            """,
            "lang": "painless",
            "params": {
                "account_name": purchase["account_username"],
                "country": purchase["country"],
                "purchase_count": 1,
                "total_revenue": purchase["amount_paid_usd"],
            },
        }
    }

    # Update the artist object in Elasticsearch
    artist_update = es.update(
        index = "artists",
        id = purchase["account_username"],
        body = body,
        upsert = {
            "account_name": purchase["account_username"],
            "url": purchase["url"],
            "purchase_count": 0,
            "total_revenue": purchase["amount_paid_usd"],
            "countries": [purchase["country"]],
        },
    )

    country_script = {
        "script": {
            "source": """
                ctx._source.revenue = (ctx._source.revenue == null) ? params.revenue : (ctx._source.revenue + params.revenue);
                ctx._source.count = (ctx._source.count == null) ? params.count : (ctx._source.count + params.count);
            """,
            "lang": "painless",
            "params": {
                "revenue": purchase["amount_paid_usd"],
                "count": 1
            },
        }
    }

    es.update(
        index = "countries",
        id = purchase["country_code"],
        body = country_script,
        upsert = {
            "country_code": purchase["country_code"],
            "country_name": purchase["country"],
        })
    
    # Add the purchase to the purchases index
    new_purchase = es.index(index="purchases", body=purchase)
    logger.info(f"Purchase added: {purchase['album_title']}")
    # Return a success message
    return {
        "message": "Purchase added successfully",
        "artist_update": bool(artist_update["_shards"]["successful"]),
    }

while True:
    purchases = get_sales_from_bc()
    add_purchases(purchases)
    logger.info("Sleeping for 2 minutes...")
    time.sleep(120)