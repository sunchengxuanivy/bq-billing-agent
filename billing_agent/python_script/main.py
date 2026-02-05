import json
from datetime import datetime, timedelta

import yaml
from google.cloud import bigquery, storage

from lib.metrics.monitor import nat_request
from lib.price import get_price_list


# cd python-script && zip -r nat.zip *
def gce_instance_billing(request):
    client = bigquery.Client()
    target_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    query = f"""
declare target_date Date;
declare service_id STRING;
SET target_date = '{target_date}';
SET service_id ='6F81-5844-456A';

delete from sunivy-hkjc-poc-public.report.compute_engine_details_v3 where usage_date=target_date ;

insert into sunivy-hkjc-poc-public.report.compute_engine_details_v3
with billing_with_category as
(
SELECT
  billing_account_id as billing_name,
  invoice.month as invoice_month,
  DATE(usage_start_time, "America/Los_Angeles") as usage_date,
  project.id as project_id,
  cost + COALESCE((SELECT SUM(amount) FROM UNNEST(credits)), 0)  as total_cost,
(select value from unnest(labels) where key='instance_name') as instance_name,
(select value from unnest(labels) where key='psm') as psm,
(select value from unnest(labels) where key='inner_ip') as inner_ip,
(case
 when upper(sku.description) like '%CPU%' or upper(sku.description) like '%CORE%' then 'CPU'
 when upper(sku.description) like '%RAM%' then 'RAM'
 when upper(sku.description) like '%GPU%' then 'GPU'
 when (upper(sku.description) like '%SSD%' and upper(sku.description) like '%PD%') then 'PD_SSD'
 when (upper(sku.description) like '%SSD%' and upper(sku.description) like '%LOCAL%') then 'LOCAL_SSD'
 when (upper(sku.description) like '%PD%') then 'PD_HDD'
 when upper(sku.description) like '%NETWORK%' then 'NETWORK'
ELSE 'OTHERS'
END) as sku_category,
 FROM `bytedance-cloud-216403.billing.gcp_billing_export_v1_01A25B_AA5CE5_4FBF3D`
where --invoice.month in ('202109','202110', '202111','202112')
 DATE(usage_start_time, "America/Los_Angeles")=target_date
  and service.id=service_id
  and ('forwarding-rule-name' not in (select key from unnest(labels)))
)
select
 billing_name, invoice_month, usage_date, project_id,
 ifnull(instance_name,'--') as instance_name,
 ifnull(psm,'--') as psm, inner_ip,
(ifnull(CPU,0) +
ifnull(RAM,0) +
ifnull(NETWORK,0) +
ifnull(PD_SSD,0) +
ifnull(LOCAL_SSD,0) +
ifnull(PD_HDD,0) +
ifnull(GPU,0) +
ifnull(OTHERS,0)) AS SUBTOTAL,
ifnull(CPU,0) AS CPU,
ifnull(RAM,0) AS RAM,
ifnull(NETWORK,0) AS NETWORK,
ifnull(PD_SSD,0) AS PD_SSD,
ifnull(LOCAL_SSD,0) AS LOCAL_SSD,
ifnull(PD_HDD,0) AS PD_HDD,
ifnull(GPU,0) AS GPU,
ifnull(OTHERS,0) AS OTHERS,
from billing_with_category
pivot( sum(total_cost)
for sku_category in ('CPU','RAM','NETWORK','PD_SSD','LOCAL_SSD','PD_HDD','GPU','OTHERS'))
-- where instance_name='web1617710019-compass546826-2'
-- insert into sunivy-hkjc-poc-public.report.compute_engine_details_v3 from other billing accounts
    
    """
    query_job = client.query(query)
    results = query_job.result()
    return 'OK'


def gce_network_billing(request):
    client = bigquery.Client()
    target_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    query = f"""
declare target_date Date;
declare service_id STRING;
SET target_date = '{target_date}';
SET service_id ='6F81-5844-456A';

delete from sunivy-hkjc-poc-public.report.compute_load_balancer_details where usage_date=target_date ;

insert into sunivy-hkjc-poc-public.report.compute_load_balancer_details
with billing_with_category as
(
SELECT
  billing_account_id as billing_name,
  invoice.month as invoice_month,
  DATE(usage_start_time, "America/Los_Angeles") as usage_date,
  project.id as project_id,
  cost + COALESCE((SELECT SUM(amount) FROM UNNEST(credits)), 0)  as total_cost,
(select value from unnest(labels) where key='forwarding-rule-name') as forwarding_rule_name,
 FROM `bytedance-cloud-216403.billing.gcp_billing_export_v1_01A25B_AA5CE5_4FBF3D`
where --invoice.month in ('202109','202110', '202111','202112')
 DATE(usage_start_time, "America/Los_Angeles")=target_date
  and service.id=service_id
  and ('forwarding-rule-name' in (select key from unnest(labels)))
)
select
 billing_name, invoice_month, usage_date, project_id,
 ifnull(forwarding_rule_name,'--') as forwarding_rule_name,
 sum(total_cost) as SUBTOTAL_USD
 from billing_with_category
 group by billing_name, invoice_month, usage_date, project_id, forwarding_rule_name;
    """
    query_job = client.query(query)
    results = query_job.result()
    return 'OK'


def gce_others_billing(request):
    client = bigquery.Client()
    target_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    query = f"""
declare target_date Date;
declare service_id STRING;
SET target_date = '{target_date}';
SET service_id ='6F81-5844-456A';

delete from `sunivy-hkjc-poc-public.report.compute_others_details_v3` where usage_date=target_date ;

insert into `sunivy-hkjc-poc-public.report.compute_others_details_v3`
with no_label as (
SELECT
  billing_account_id as billing_name,
  invoice.month as invoice_month,
  DATE(usage_start_time, "America/Los_Angeles") as usage_date,
  project.name as project_name,
  cost + COALESCE((SELECT SUM(amount) FROM UNNEST(credits)), 0)  as total_cost,
  sku.description as description
 FROM `bytedance-cloud-216403.billing.gcp_billing_export_v1_01A25B_AA5CE5_4FBF3D`
 where --invoice.month in ('202110','202111','202112') and
 date(usage_start_time, "America/Los_Angeles") = target_date and
 service.id='6F81-5844-456A' and
 upper(sku.description) not like '%CPU%'
 and  upper(sku.description) not like '%CORE%'
 and upper(sku.description) not like '%RAM%'
 and  upper(sku.description) not like '%GPU%'
 and  (upper(sku.description) not like '%PD%')
 and  upper(sku.description) not like '%NETWORK%'
 and not (upper(sku.description) like '%SSD%' and upper(sku.description) like '%LOCAL%')
 and array_length(labels) = 0
)
select description, billing_name , project_name , invoice_month , usage_date , sum(total_cost ) as SUBTOTAL from no_label
group by description, billing_name , project_name, invoice_month , usage_date;

    """
    query_job = client.query(query)
    results = query_job.result()
    return 'OK'


def storage_billing(request):
    client = bigquery.Client()
    target_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    query = f"""
declare target_date Date;
declare service_id STRING;
SET target_date = '{target_date}';
SET service_id ='95FF-2EF5-5EA1';

delete from sunivy-hkjc-poc-public.report.storage_details_v3 where usage_date=target_date ;

insert into `sunivy-hkjc-poc-public.report.storage_details_v3`
with billing_with_category as
(
SELECT
  billing_account_id as billing_name,
  invoice.month as invoice_month,
  DATE(usage_start_time, "America/Los_Angeles") as usage_date,
  project.name as project_name,
  location.location as region,
  cost + COALESCE((SELECT SUM(amount) FROM UNNEST(credits)), 0)  as total_cost,
  usage.amount_in_pricing_units * cast(EXTRACT(DAY FROM LAST_DAY(DATE(usage_start_time, "America/Los_Angeles"))) as int) as total_usage,
ifnull((select value from unnest(labels) where key='bucket_name') ,'--') as bucket_name,
trim(REGEXP_REPLACE(sku.description, r'\W+', '_'),'_') AS SKU_DESCRIPTION,
(case
 when upper(sku.description) like 'STANDARD STORAGE%' then 'STORAGE'
 when upper(sku.description) like 'NEARLINE STORAGE%' then 'STORAGE'
 when upper(sku.description) like 'DOWNLOAD%' then trim(REGEXP_REPLACE(sku.description, r'\W+', '_'),'_')
 when upper(sku.description) like '%STANDARD CLASS A OPERATIONS' then 'STANDARD_CLASS_A_OPERATIONS'
 when upper(sku.description) like '%STANDARD CLASS B OPERATIONS' then 'STANDARD_CLASS_B_OPERATIONS'
 when upper(sku.description) like '%EGRESS%' then trim(REGEXP_REPLACE(sku.description, r'\W+', '_'),'_')
ELSE 'OTHERS'
END) as sku_category,
 FROM `bytedance-cloud-216403.billing.gcp_billing_export_v1_01A25B_AA5CE5_4FBF3D`
 WHERE --invoice.month in ('202112', '202111', '202110')
  DATE(usage_start_time, "America/Los_Angeles")=target_date
  and service.id=service_id

)
select BUCKET_INFO.*,
(
    ifnull(STORAGE_COST.STORAGE,0) +
    ifnull(CLASS_A_OPERATIONS_COST.STANDARD_CLASS_A_OPERATIONS,0) +
    ifnull(CLASS_B_OPERATIONS_COST.STANDARD_CLASS_B_OPERATIONS,0) +
    ifnull(NETWORK_AND_OTHERS.NETWORK_PRICE,0 ) +
    ifnull(NETWORK_AND_OTHERS.OTHERS,0 )
    ) AS SUBTOTAL,
ifnull(STORAGE_COST.STORAGE_TYPE, '--') as STORAGE_TYPE,
ifnull(STORAGE_COST.STORAGE, 0) as STORAGE,
ifnull(STORAGE_COST.storage_usage, 0 ) as USAGE_GB,
ifnull(CLASS_A_OPERATIONS_COST.STANDARD_CLASS_A_OPERATIONS_TYPE, '--') as STANDARD_CLASS_A_OPERATIONS_TYPE,
ifnull(CLASS_A_OPERATIONS_COST.STANDARD_CLASS_A_OPERATIONS, 0) as STANDARD_CLASS_A_OPERATIONS,
ifnull(CLASS_B_OPERATIONS_COST.STANDARD_CLASS_B_OPERATIONS_TYPE ,'--') as STANDARD_CLASS_B_OPERATIONS_TYPE,
ifnull(CLASS_B_OPERATIONS_COST.STANDARD_CLASS_B_OPERATIONS,0) as STANDARD_CLASS_B_OPERATIONS,
ifnull(NETWORK_AND_OTHERS.Download_APAC,0) AS Download_APAC,
ifnull(NETWORK_AND_OTHERS.Download_Australia,0) AS Download_Australia,
ifnull(NETWORK_AND_OTHERS.Download_China,0) AS Download_China,
ifnull(NETWORK_AND_OTHERS.Download_Worldwide_Destinations_excluding_Asia_Australia,0) AS Download_Worldwide_Destinations_excluding_Asia_Australia,
ifnull(NETWORK_AND_OTHERS.APAC_based_Storage_egress_via_peered_interconnect_network,0) AS APAC_based_Storage_egress_via_peered_interconnect_network,
ifnull(NETWORK_AND_OTHERS.EU_based_Storage_egress_via_peered_interconnect_network,0) AS EU_based_Storage_egress_via_peered_interconnect_network,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_APAC_and_SA,0) AS GCP_Storage_egress_between_APAC_and_SA,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_AU_and_APAC,0) AS GCP_Storage_egress_between_AU_and_APAC,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_AU_and_EU,0) AS GCP_Storage_egress_between_AU_and_EU,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_AU_and_NA,0) AS GCP_Storage_egress_between_AU_and_NA,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_AU_and_SA,0) AS GCP_Storage_egress_between_AU_and_SA,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_EU_and_APAC,0) AS GCP_Storage_egress_between_EU_and_APAC,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_EU_and_SA,0) AS GCP_Storage_egress_between_EU_and_SA,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_NA_and_APAC,0) AS GCP_Storage_egress_between_NA_and_APAC,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_NA_and_EU,0) AS GCP_Storage_egress_between_NA_and_EU,
ifnull(NETWORK_AND_OTHERS.GCP_Storage_egress_between_NA_and_SA,0) AS GCP_Storage_egress_between_NA_and_SA,
ifnull(NETWORK_AND_OTHERS.Inter_region_GCP_Storage_egress_within_APAC,0) AS Inter_region_GCP_Storage_egress_within_APAC,
ifnull(NETWORK_AND_OTHERS.Inter_region_GCP_Storage_egress_within_AU,0) AS Inter_region_GCP_Storage_egress_within_AU,
ifnull(NETWORK_AND_OTHERS.Inter_region_GCP_Storage_egress_within_EU,0) AS Inter_region_GCP_Storage_egress_within_EU,
ifnull(NETWORK_AND_OTHERS.Inter_region_GCP_Storage_egress_within_NA,0) AS Inter_region_GCP_Storage_egress_within_NA,
ifnull(NETWORK_AND_OTHERS.Inter_region_GCP_Storage_egress_within_SA,0) AS Inter_region_GCP_Storage_egress_within_SA,
ifnull(NETWORK_AND_OTHERS.NA_based_Storage_egress_via_peered_interconnect_network,0) AS NA_based_Storage_egress_via_peered_interconnect_network,
ifnull(NETWORK_AND_OTHERS.Networking_Traffic_Egress_GAE_Firebase_Storage,0) AS Networking_Traffic_Egress_GAE_Firebase_Storage,
ifnull(NETWORK_AND_OTHERS.NETWORK_PRICE,0) as NETWORK_PRICE,
ifnull(NETWORK_AND_OTHERS.OTHERS ,0) as OTHERS

from
(SELECT
    billing_name ,
    invoice_month ,
    usage_date,
    project_name ,
    region ,
    bucket_name
from billing_with_category
group by billing_name , invoice_month, usage_date, project_name , region , bucket_name
order by project_name
) BUCKET_INFO
LEFT JOIN
 (
select billing_name , invoice_month , project_name , region,  bucket_name, usage_date,
sku_description AS STORAGE_TYPE, sum(total_cost) AS STORAGE, sum(total_usage) as STORAGE_USAGE
from billing_with_category where sku_category ='STORAGE'
group by billing_name , invoice_month , project_name , region, sku_description, bucket_name, usage_date
) STORAGE_COST
ON BUCKET_INFO.bucket_name = STORAGE_COST.bucket_name and BUCKET_INFO.region = STORAGE_COST.region and BUCKET_INFO.project_name = STORAGE_COST.project_name and BUCKET_INFO.usage_date = STORAGE_COST.usage_date and BUCKET_INFO.invoice_month = STORAGE_COST.invoice_month
left join
(
select billing_name , invoice_month , project_name , region,  bucket_name, usage_date, sku_description AS STANDARD_CLASS_A_OPERATIONS_TYPE, sum(total_cost) AS STANDARD_CLASS_A_OPERATIONS
from billing_with_category where sku_category ='STANDARD_CLASS_A_OPERATIONS'
group by billing_name , invoice_month , project_name , region, sku_description, bucket_name, usage_date
) CLASS_A_OPERATIONS_COST
ON BUCKET_INFO.bucket_name = CLASS_A_OPERATIONS_COST.bucket_name and BUCKET_INFO.region = CLASS_A_OPERATIONS_COST.region and BUCKET_INFO.project_name = CLASS_A_OPERATIONS_COST.project_name and BUCKET_INFO.usage_date = CLASS_A_OPERATIONS_COST.usage_date and BUCKET_INFO.invoice_month = CLASS_A_OPERATIONS_COST.invoice_month
left join
(
select billing_name , invoice_month , project_name , region,  bucket_name, usage_date, sku_description AS STANDARD_CLASS_B_OPERATIONS_TYPE, sum(total_cost) AS STANDARD_CLASS_B_OPERATIONS
from billing_with_category where sku_category ='STANDARD_CLASS_B_OPERATIONS'
group by billing_name , invoice_month , project_name , region, sku_description, bucket_name, usage_date
) CLASS_B_OPERATIONS_COST
on BUCKET_INFO.bucket_name = CLASS_B_OPERATIONS_COST.bucket_name  and BUCKET_INFO.region = CLASS_B_OPERATIONS_COST.region and BUCKET_INFO.project_name = CLASS_B_OPERATIONS_COST.project_name and BUCKET_INFO.usage_date =CLASS_B_OPERATIONS_COST.usage_date and BUCKET_INFO.invoice_month = CLASS_B_OPERATIONS_COST.invoice_month
left join
(
select
project_name, region, bucket_name, usage_date, invoice_month ,
ifnull(Download_APAC,0) AS Download_APAC,
ifnull(Download_Australia,0) AS Download_Australia,
ifnull(Download_China,0) AS Download_China,
ifnull(Download_Worldwide_Destinations_excluding_Asia_Australia,0) AS Download_Worldwide_Destinations_excluding_Asia_Australia,
ifnull(APAC_based_Storage_egress_via_peered_interconnect_network,0) AS APAC_based_Storage_egress_via_peered_interconnect_network,
ifnull(EU_based_Storage_egress_via_peered_interconnect_network,0) AS EU_based_Storage_egress_via_peered_interconnect_network,
ifnull(GCP_Storage_egress_between_APAC_and_SA,0) AS GCP_Storage_egress_between_APAC_and_SA,
ifnull(GCP_Storage_egress_between_AU_and_APAC,0) AS GCP_Storage_egress_between_AU_and_APAC,
ifnull(GCP_Storage_egress_between_AU_and_EU,0) AS GCP_Storage_egress_between_AU_and_EU,
ifnull(GCP_Storage_egress_between_AU_and_NA,0) AS GCP_Storage_egress_between_AU_and_NA,
ifnull(GCP_Storage_egress_between_AU_and_SA,0) AS GCP_Storage_egress_between_AU_and_SA,
ifnull(GCP_Storage_egress_between_EU_and_APAC,0) AS GCP_Storage_egress_between_EU_and_APAC,
ifnull(GCP_Storage_egress_between_EU_and_SA,0) AS GCP_Storage_egress_between_EU_and_SA,
ifnull(GCP_Storage_egress_between_NA_and_APAC,0) AS GCP_Storage_egress_between_NA_and_APAC,
ifnull(GCP_Storage_egress_between_NA_and_EU,0) AS GCP_Storage_egress_between_NA_and_EU,
ifnull(GCP_Storage_egress_between_NA_and_SA,0) AS GCP_Storage_egress_between_NA_and_SA,
ifnull(Inter_region_GCP_Storage_egress_within_APAC,0) AS Inter_region_GCP_Storage_egress_within_APAC,
ifnull(Inter_region_GCP_Storage_egress_within_AU,0) AS Inter_region_GCP_Storage_egress_within_AU,
ifnull(Inter_region_GCP_Storage_egress_within_EU,0) AS Inter_region_GCP_Storage_egress_within_EU,
ifnull(Inter_region_GCP_Storage_egress_within_NA,0) AS Inter_region_GCP_Storage_egress_within_NA,
ifnull(Inter_region_GCP_Storage_egress_within_SA,0) AS Inter_region_GCP_Storage_egress_within_SA,
ifnull(NA_based_Storage_egress_via_peered_interconnect_network,0) AS NA_based_Storage_egress_via_peered_interconnect_network,
ifnull(Networking_Traffic_Egress_GAE_Firebase_Storage,0) AS Networking_Traffic_Egress_GAE_Firebase_Storage,

(ifnull(Download_APAC,0) +
ifnull(Download_Australia,0) +
ifnull(Download_China,0) +
ifnull(Download_Worldwide_Destinations_excluding_Asia_Australia,0) +
ifnull(APAC_based_Storage_egress_via_peered_interconnect_network,0) +
ifnull(EU_based_Storage_egress_via_peered_interconnect_network,0) +
ifnull(GCP_Storage_egress_between_APAC_and_SA,0) +
ifnull(GCP_Storage_egress_between_AU_and_APAC,0) +
ifnull(GCP_Storage_egress_between_AU_and_EU,0) +
ifnull(GCP_Storage_egress_between_AU_and_NA,0) +
ifnull(GCP_Storage_egress_between_AU_and_SA,0) +
ifnull(GCP_Storage_egress_between_EU_and_APAC,0) +
ifnull(GCP_Storage_egress_between_EU_and_SA,0) +
ifnull(GCP_Storage_egress_between_NA_and_APAC,0) +
ifnull(GCP_Storage_egress_between_NA_and_EU,0) +
ifnull(GCP_Storage_egress_between_NA_and_SA,0) +
ifnull(Inter_region_GCP_Storage_egress_within_APAC,0) +
ifnull(Inter_region_GCP_Storage_egress_within_AU,0) +
ifnull(Inter_region_GCP_Storage_egress_within_EU,0) +
ifnull(Inter_region_GCP_Storage_egress_within_NA,0) +
ifnull(Inter_region_GCP_Storage_egress_within_SA,0) +
ifnull(NA_based_Storage_egress_via_peered_interconnect_network,0) +
ifnull(Networking_Traffic_Egress_GAE_Firebase_Storage,0)
) as NETWORK_PRICE,

ifnull(OTHERS,0) AS OTHERS,

from (
    select  project_name, bucket_name, region, usage_date, sku_category, total_cost, invoice_month from
billing_with_category where sku_category not in ('STORAGE','STANDARD_CLASS_A_OPERATIONS','STANDARD_CLASS_B_OPERATIONS')
)
pivot( sum(total_cost)
for sku_category in (
    'Download_APAC',
	'Download_Australia',
	'Download_China',
	'Download_Worldwide_Destinations_excluding_Asia_Australia',
	'APAC_based_Storage_egress_via_peered_interconnect_network',
	'EU_based_Storage_egress_via_peered_interconnect_network',
	'GCP_Storage_egress_between_APAC_and_SA',
	'GCP_Storage_egress_between_AU_and_APAC',
	'GCP_Storage_egress_between_AU_and_EU',
	'GCP_Storage_egress_between_AU_and_NA',
	'GCP_Storage_egress_between_AU_and_SA',
	'GCP_Storage_egress_between_EU_and_APAC',
	'GCP_Storage_egress_between_EU_and_SA',
	'GCP_Storage_egress_between_NA_and_APAC',
	'GCP_Storage_egress_between_NA_and_EU',
	'GCP_Storage_egress_between_NA_and_SA',
	'Inter_region_GCP_Storage_egress_within_APAC',
	'Inter_region_GCP_Storage_egress_within_AU',
	'Inter_region_GCP_Storage_egress_within_EU',
	'Inter_region_GCP_Storage_egress_within_NA',
	'Inter_region_GCP_Storage_egress_within_SA',
	'NA_based_Storage_egress_via_peered_interconnect_network',
	'Networking_Traffic_Egress_GAE_Firebase_Storage',
	'OTHERS'
)) order by bucket_name desc
) AS NETWORK_AND_OTHERS
ON BUCKET_INFO.bucket_name = NETWORK_AND_OTHERS.bucket_name and BUCKET_INFO.project_name = NETWORK_AND_OTHERS.project_name  and BUCKET_INFO.region = NETWORK_AND_OTHERS.region and BUCKET_INFO.usage_date = NETWORK_AND_OTHERS.usage_date and BUCKET_INFO.invoice_month = NETWORK_AND_OTHERS.invoice_month
order by storage desc;
    """
    query_job = client.query(query)
    results = query_job.result()
    return 'OK'


def gce_nat_billing(request):
    bucket_name = 'sunivy-hkjc-daml'

    date_str = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")

    nat_data_dict = nat_request(date_str, ['bytedance-yawn-default'])

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

    tmp_file_name = f'/tmp/{date_str}.json'
    data_file = open(tmp_file_name, "w")
    data_file.writelines([f'{json.dumps(item)}\n' for item in nat_data_dict.values()])
    data_file.close()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = f'nat/{date_str}.json'
    blob = bucket.blob(blob_name)
    # blob._chunk_size = 1024 * 1024 * 2
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
    return 'OK'
