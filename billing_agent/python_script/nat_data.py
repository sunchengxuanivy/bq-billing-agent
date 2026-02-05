"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""
import json

import yaml
from google.cloud import monitoring_v3, storage, bigquery

import datetime
from pytz import timezone

from lib.price import get_price_list
from lib.metrics.monitor import nat_request

if __name__ == '__main__':
    date_str = '2022-01-10'
    bucket_name = 'sunivy-hkjc-daml'
    nat_data_dict = nat_request(date_str, ['bytedance-cloud-216403', 'bytedance-yawn-default'])

    list_price_dict, final_price_dict = get_price_list()
    with open('conf/sku_nat.yaml', 'r') as sku_yaml:
        sku_nat_dict = yaml.safe_load(sku_yaml)

    for item in nat_data_dict.values():
        # print(type(item))
        in_and_out = item['received_bytes_count'] + item['sent_bytes_count']
        region = item['region']
        sku_id = sku_nat_dict['nat-data'][region]
        price = final_price_dict[sku_id]
        item['nat_fee_usd'] = price * in_and_out

    tmp_file_name = f'{date_str}.json'
    data_file = open(tmp_file_name, "w")
    data_file.writelines([f'{json.dumps(item)}\n' for item in nat_data_dict.values()])
    data_file.close()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = f'nat/{tmp_file_name}'
    blob = bucket.blob(blob_name)
    blob._chunk_size = 1024 * 1024 * 2
    blob.upload_from_filename(tmp_file_name)

    bigquery_client = bigquery.Client()
    query = f"""
    delete from `sunivy-hkjc-poc-public.report.nat_details_v3` where date(usage_date) = '{date_str}'
    """
    job = bigquery_client.query(query)
    result = job.result()
    table_id = "sunivy-hkjc-poc-public.report.nat_details_v3"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = [
        f'gs://{bucket_name}/{blob_name}'
    ]

    load_job = bigquery_client.load_table_from_uri(
        uri,
        table_id,
        # location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
