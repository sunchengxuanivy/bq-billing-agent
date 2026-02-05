import logging
import os
from google.cloud import bigquery
import datetime

def get_price_list() -> dict:
    """
    This function is to request the latest COMPUTE ENGINE pricing information from Bigquery pricing table.
    It will retrieve all relevant pricing details for each SKU.

    Please export Json key of the account having the right permission,
    and set GOOGLE_APPLICATION_CREDENTIALS as the path of the Json file.
    :return:
    A dictionary where the key is SKU_ID, and value is a dictionary
    containing list_prices, final_prices,
    description, and pricing_unit.
    """
    table_name = os.environ.get('BIGQUERY_PRICING_TABLE')
    if not table_name:
        logging.warning("BIGQUERY_PRICING_TABLE environment variable not set. Using empty price list.")
        return {}

    pricing_query = f'''
        SELECT
            sku.id as sku_id,
            sku.description as sku_description,
            list_price,
            billing_account_price,
            date(export_time) as pricing_date
        FROM `{table_name}`
        WHERE
        DATE(_PARTITIONTIME) = (select max(date(_PARTITIONTIME)) from `{table_name}`)
        AND service.id in ('6F81-5844-456A','65DF-8A98-0834')
    '''

    client = bigquery.Client()
    query_job = client.query(pricing_query)
    results = query_job.result()

    all_prices = {}
    for row in results:
        list_price_details = dict(row.list_price) if row.list_price else {}
        if 'tiered_rates' in list_price_details and list_price_details['tiered_rates']:
            list_price_details['tiered_rates'] = [dict(rate) for rate in list_price_details['tiered_rates']]

        final_price_details = dict(row.billing_account_price) if row.billing_account_price else {}
        if 'tiered_rates' in final_price_details and final_price_details['tiered_rates']:
            final_price_details['tiered_rates'] = [dict(rate) for rate in final_price_details['tiered_rates']]

        all_prices[row.sku_id] = {
            "description": row.sku_description,
            "list_price_details": list_price_details,
            "final_price_details": final_price_details,
            "pricing_date": str(row.pricing_date),
        }
    logging.info(f"Pricing of {len(all_prices)} SKUs loaded.")
    return all_prices


def get_pricing_for_sku_from_bq(sku_id: str) -> dict:
    """
    This function is to request the latest COMPUTE ENGINE pricing information for a specific SKU from Bigquery pricing table.
    It will retrieve all relevant pricing details for the SKU.

    Please export Json key of the account having the right permission,
    and set GOOGLE_APPLICATION_CREDENTIALS as the path of the Json file.
    :return:
    A dictionary containing all pricing details for the SKU.
    """
    table_name = os.environ.get('BIGQUERY_PRICING_TABLE')
    if not table_name:
        logging.warning("BIGQUERY_PRICING_TABLE environment variable not set. Cannot get pricing for SKU.")
        return {}

    pricing_query = f'''
        SELECT
            sku.id as sku_id,
            sku.description as sku_description,
            list_price,
            billing_account_price,
            date(export_time) as pricing_date
        FROM `{table_name}`
        WHERE
        DATE(_PARTITIONTIME) = (select max(date(_PARTITIONTIME)) from `{table_name}`)
        AND sku.id = "{sku_id}"
    '''

    client = bigquery.Client()
    query_job = client.query(pricing_query)
    results = query_job.result()

    if results.total_rows == 0:
        return {"error": "SKU not found"}

    return dict(list(results)[0])
