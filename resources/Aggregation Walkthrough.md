# Insight Goals
Let's go through each goal and examine how we would approach reaching our intended data through Elasticsearch.


## Find top 5 selling accounts within a given time span
To find the top 5 `account_name` values grouped by the sum of `amount_paid_usd` over a given start and stop time period in Elastic, you would use a combination of a date range filter and an aggregation query. Below is an example of how you can achieve this with an Elasticsearch query:

```json
GET purchases/_search
{
  "size": 0,
  "query": {
    "range": {
      "timestamp": {
        "gte": "2024-01-01T00:00:00Z",  // Replace with your start time
        "lte": "2024-12-31T23:59:59Z"   // Replace with your stop time
      }
    }
  },
  "aggs": {
    "top_artists": {
      "terms": {
        "field": "account_name.keyword",
        "size": 5,
        "order": {
          "total_amount_paid_usd": "desc"
        }
      },
      "aggs": {
        "total_amount_paid_usd": {
          "sum": {
            "field": "amount_paid_usd"
          }
        }
      }
    }
  }
}
```

#### Explanation:
- **`query`**: The `range` query filters the documents based on the `timestamp` field to be within the specified start and stop time period.
- **`aggs`**: 
   - **`top_artists`**: This is a terms aggregation on the `artist_name.keyword` field to group by artist names.
   - **`size: 5`**: Limits the aggregation to the top 5 artists.
   - **`order`**: Orders the results by the `total_amount_paid_usd` in descending order.
   - **`total_amount_paid_usd`**: This is a nested aggregation that sums up the `amount_paid_usd` for each artist.

This will return the top 5 artists by the sum of `amount_paid_usd` within the specified time range, allowing you to identify the highest-earning artists over that period.



## Find top accounts with the largest global distribution
Combining two aggregations (top selling account and ) into a single query is challenging because Elasticsearch does not support directly filtering the results of one aggregation to be used in another aggregation within the same query. However, you can use a `terms` aggregation followed by a `top_hits` aggregation to first get the top 100 selling artists and then sort these based on the unique country count within the same query.

Here’s how you can structure this:

### Query Example

```json
GET purchases/_search
{
  "size": 0,
  "aggs": {
    "top_selling_artists": {
      "terms": {
        "field": "account_username.keyword",
        "size": 100,
        "order": {
          "total_sales": "desc"
        }
      },
      "aggs": {
        "total_sales": {
          "sum": {
            "field": "amount_paid_usd"
          }
        },
        "unique_country_count": {
          "cardinality": {
            "field": "country.keyword"
          }
        },
        "top_artists_by_country_count": {
          "bucket_sort": {
            "sort": [
              {
                "unique_country_count": {
                  "order": "desc"
                }
              }
            ],
            "size": 5
          }
        }
      }
    }
  }
}
```

### Explanation:
- **`aggs`**:
   - **`top_selling_artists`**:
     - **`terms`**: Groups the documents by `account_username.keyword`.
     - **`size`**: Retrieves the top 100 artists.
     - **`order`**: Orders the results by the `total_sales` in descending order.
   - **`total_sales`**: A nested aggregation that calculates the total sales for each `account_username`.
   - **`unique_country_count`**: A nested aggregation that calculates the number of unique `country` values for each `account_username`.
   - **`top_artists_by_country_count`**:
     - **`bucket_sort`**: A bucket sort aggregation to sort the buckets by `unique_country_count` in descending order and limit the output to the top 5.

This combined query first aggregates the top 100 selling artists based on total sales and then sorts these top 100 by the number of unique countries, returning the top 5 artists with the most unique countries.


## Find top countries that bought the most items within a time span
This query is straightfoward. To find the top 5 countries that bought the most items within a specific time span, you can use a combination of a `range` query to filter the documents by the specified time range and a `terms` aggregation to group the results by the `country` field and sort them by the total number of items purchased.

### Query Example

```json
GET purchases/_search
{
  "size": 0,
  "query": {
    "range": {
      "timestamp": {
        "gte": "2024-01-01T00:00:00Z",  // Start time
        "lte": "2024-12-31T23:59:59Z"   // End time
      }
    }
  },
  "aggs": {
    "top_countries": {
      "terms": {
        "field": "country.keyword",
        "size": 5,
        "order": {
          "_count": "desc"
        }
      }
    }
  }
}
```

### Explanation:
**`query`**:
 - **`range`**: Filters the documents based on the `timestamp` field to be within the specified start and stop time period.
   - **`gte`**: Greater than or equal to the start time.
   - **`lte`**: Less than or equal to the end time.
**`aggs`**:
 - **`top_countries`**:
   - **`terms`**: Groups the documents by the `country.keyword` field.
   - **`size`**: Limits the aggregation to the top 5 countries.
   - **`order`**: Orders the results by the document count (`_count`) in descending order.


## Look up the top selling items of a specific country
To find the top 5 selling items in a specific country, you can use a combination of a term query to filter the documents by the specified `country_code` and a terms aggregation to group the results by the `item_description` field. This will allow you to determine which items are the best-selling in that country.

To include the `artist_name` and `album_title` in the aggregation results for more specific results, you can use a `terms` aggregation for `item_description` and add `top_hits` sub-aggregations to retrieve the top `artist_name` and `album_title` for each item. Here's the query to achieve this:

### Query Example

```json
GET purchases/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "country_code.keyword": "US"  // Replace with the desired country code
          }
        }
      ]
    }
  },
  "aggs": {
    "top_selling_items": {
      "terms": {
        "field": "item_description.keyword",
        "size": 5,
        "order": {
          "total_sales": "desc"
        }
      },
      "aggs": {
        "total_sales": {
          "sum": {
            "field": "amount_paid_usd"
          }
        },
        "top_hit_artist_album": {
          "top_hits": {
            "size": 1,
            "_source": {
              "includes": ["artist_name", "album_title"]
            }
          }
        }
      }
    }
  }
}
```

### Explanation:

**`query`**:
- **`bool`**: Combines multiple query conditions.
    - **`must`**: Both conditions must be satisfied.
    - **`term`**: Filters the documents to include only those from the specified country (`country.keyword`).
    - **`range`**: Filters the documents based on the `timestamp` field to be within the specified start and stop time period.
        - **`gte`**: Greater than or equal to the start time.
        - **`lte`**: Less than or equal to the end time.
**`aggs`**:
- **`top_selling_items`**:
    - **`terms`**: Groups the documents by the `item_description.keyword` field.
    - **`size`**: Limits the aggregation to the top 5 items.
    - **`order`**: Orders the results by the total sales amount in descending order.
- **`total_sales`**: A sub-aggregation that calculates the total sales amount (`amount_paid_usd`) for each `item_description`.
- **`top_hit_artist_album`**:
    - **`top_hits`**: A sub-aggregation that retrieves the top document for each bucket. We include the `artist_name` and `album_title` fields in the source.



## Look up geographic  distribution of purchases for a specific artist
To return all the different countries for the purchases of a specific artist, ordered by the number of purchases (popularity) in each country, you can use a `terms` aggregation with a `term` query to filter by the specific artist and sort the countries by the document count.

### Query Example

```json
GET purchases/_search
{
  "size": 0,
  "query": {
    "term": {
      "artist_name.keyword": "specific_artist_name"  // Replace with the desired artist name
    }
  },
  "aggs": {
    "countries": {
      "terms": {
        "field": "country.keyword",
        "size": 1000,  // Adjust the size if you expect more than 1000 unique countries
        "order": {
          "_count": "desc"  // Orders by the number of purchases in descending order
        }
      }
    }
  }
}
```

### Explanation:

2. **`query`**:
   - **`term`**: Filters the documents to include only those with the specified `artist_name.keyword`.
3. **`aggs`**:
   - **`countries`**:
     - **`terms`**: Groups the documents by the `country.keyword` field.
     - **`size`**: Limits the aggregation to the top N countries (adjust the size based on the expected number of unique countries).
     - **`order`**: Orders the results by the document count (`_count`) in descending order.

## Find average purchase price to item ratio per country (offered cost vs. item value)

