"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""
import io
import json

import yaml
from google.cloud import bigquery

filtering_cpu_dict = {
    'e2-predefined': ['E2 Instance Core running in%'],
    'e2-custom': ['E2 Instance Core running in%'],
    'e2-cud-1y': ['Commitment v1: E2 Cpu in%', '%for 1 Year'],
    'e2-cud-3y': ['Commitment v1: E2 Cpu in%', '%for 3 Year'],
    'a2-predefined': ['A2 Instance Core running in%'],
    'a2-cud-1y': ['Commitment v1: A2 Cpu in%', '%for 1 Year'],
    'a2-cud-3y': ['Commitment v1: A2 Cpu in%', '%for 3 Year'],
    'a3-predefined': ['A3 Instance Core running in%'],
    'a3-cud-1y': ['Commitment v1: A3 Cpu in%', '%for 1 Year'],
    'a3-cud-3y': ['Commitment v1: A3 Cpu in%', '%for 3 Years'],
    'g2-predefined': ['G2 Instance Core running in%'],
    'g2-custom': ['G2 Custom Instance Core running in%'],
    'g2-cud-1y': ['Commitment v1: G2 Cpu in%', '%for 1 Year'],
    'g2-cud-3y': ['Commitment v1: G2 Cpu in%', '%for 3 Years'],
    'n1-predefined': ['N1 Predefined Instance Core running in%'],
    'n1-custom': ['Custom Instance Core running in%'],
    'n1-cud-1y': ['Commitment v1: Cpu in%', '%for 1 Year'],
    'n1-cud-3y': ['Commitment v1: Cpu in%', '%for 3 Year'],
    'n2-predefined': ['N2 Instance Core running in%'],
    'n2-custom': ['N2 Custom Instance Core running in%'],
    'n2-cud-1y': ['Commitment v1: N2 Cpu in%', '%for 1 Year'],
    'n2-cud-3y': ['Commitment v1: N2 Cpu in%', '%for 3 Year'],
    'n2d-predefined': ['N2D AMD Instance Core running in%'],
    'n2d-custom': ['N2D AMD Custom Instance Core running in%'],
    'n2d-cud-1y': ['Commitment v1: N2D AMD Cpu in%', '%for 1 Year'],
    'n2d-cud-3y': ['Commitment v1: N2D AMD Cpu in%', '%for 3 Year'],
    'c2-predefined': ['Compute optimized Core running in%'],
    'c2-cud-1y': ['Commitment: Compute optimized Core running in%', '%for 1 Year'],
    'c2-cud-3y': ['Commitment: Compute optimized Core running in%', '%for 3 Year'],
    'c3-predefined': ['C3 Instance Core running in%'],
    'c3-cud-1y': ['Commitment v1: C3 Cpu in%', '%for 1 Year'],
    'c3-cud-3y': ['Commitment v1: C3 Cpu in%', '%for 3 Years'],
    'c3d-predefined': ['C3D Instance Core running in%'],
    'c3d-cud-1y': ['Commitment v1: C3D Cpu in%', '%for 1 Year'],
    'c3d-cud-3y': ['Commitment v1: C3D Cpu in%', '%for 3 Years'],
    'g4-predefined': ['G4 Instance Core running in%'],
    'g4-cud-1y': ['Commitment v1: G4 Cpu in%', '%for 1 Year'],
    'g4-cud-3y': ['Commitment v1: G4 Cpu in%', '%for 3 Years'],
    'c4-predefined': ['C4 Instance Core running in%'],
    'c4-cud-1y': ['Commitment v1: C4 Cpu in%', '%for 1 Year'],
    'c4-cud-3y': ['Commitment v1: C4 Cpu in%', '%for 3 Years'],
    'c4d-predefined': ['C4D Instance Core running in%'],
    'c4d-cud-1y': ['Commitment v1: C4D Cpu in%', '%for 1 Year'],
    'c4d-cud-3y': ['Commitment v1: C4D Cpu in%', '%for 3 Years'],
    'z3-predefined': ['Z3 Instance Core running in%'],
    'z3-cud-1y': ['Commitment v1: Z3 Cpu in%', '%for 1 Year'],
    'z3-cud-3y': ['Commitment v1: Z3 Cpu in%', '%for 3 Years'],
}

filtering_ram_dict = {
    'e2-predefined': ['E2 Instance Ram running in%'],
    'e2-custom': ['E2 Instance Ram running in%'],
    'e2-cud-1y': ['Commitment v1: E2 Ram in%', '%for 1 Year'],
    'e2-cud-3y': ['Commitment v1: E2 Ram in%', '%for 3 Year'],
    'a2-predefined': ['A2 Instance Ram running in%'],
    'a2-cud-1y': ['Commitment v1: A2 Ram in%', '%for 1 Year'],
    'a2-cud-3y': ['Commitment v1: A2 Ram in%', '%for 3 Year'],
    'a3-predefined': ['A3 Instance Ram running in%'],
    'a3-cud-1y': ['Commitment v1: A3 Ram in%', '%for 1 Year'],
    'a3-cud-3y': ['Commitment v1: A3 Ram in%', '%for 3 Years'],
    'g2-predefined': ['G2 Instance Ram running in%'],
    'g2-custom': ['G2 Custom Instance Ram running in%'],
    'g2-cud-1y': ['Commitment v1: G2 Ram in%', '%for 1 Year'],
    'g2-cud-3y': ['Commitment v1: G2 Ram in%', '%for 3 Years'],
    'n1-predefined': ['N1 Predefined Instance Ram running in%'],
    'n1-custom': ['Custom Instance Ram running in%'],
    'n1-cud-1y': ['Commitment v1: Ram in%', '%for 1 Year'],
    'n1-cud-3y': ['Commitment v1: Ram in%', '%for 3 Year'],
    'n2-predefined': ['N2 Instance Ram running in%'],
    'n2-custom': ['N2 Custom Instance Ram running in%'],
    'n2-cud-1y': ['Commitment v1: N2 Ram in%', '%for 1 Year'],
    'n2-cud-3y': ['Commitment v1: N2 Ram in%', '%for 3 Year'],
    'n2d-predefined': ['N2D AMD Instance Ram running in%'],
    'n2d-custom': ['N2D AMD Custom Instance Ram running in%'],
    'n2d-cud-1y': ['Commitment v1: N2D AMD Ram in%', '%for 1 Year'],
    'n2d-cud-3y': ['Commitment v1: N2D AMD Ram in%', '%for 3 Year'],
    'c2-predefined': ['Compute optimized Ram running in%'],
    'c2-cud-1y': ['Commitment: Compute optimized Ram running in%', '%for 1 Year'],
    'c2-cud-3y': ['Commitment: Compute optimized Ram running in%', '%for 3 Year'],
    'c3-predefined': ['C3 Instance Ram running in%'],
    'c3-cud-1y': ['Commitment v1: C3 Ram in%', '%for 1 Year'],
    'c3-cud-3y': ['Commitment v1: C3 Ram in%', '%for 3 Years'],
    'c3d-predefined': ['C3D Instance Ram running in%'],
    'c3d-cud-1y': ['Commitment v1: C3D Ram in%', '%for 1 Year'],
    'c3d-cud-3y': ['Commitment v1: C3D Ram in%', '%for 3 Years'],
    'g4-predefined': ['G4 Instance Ram running in%'],
    'g4-cud-1y': ['Commitment v1: G4 Ram in%', '%for 1 Year'],
    'g4-cud-3y': ['Commitment v1: G4 Ram in%', '%for 3 Years'],
    'c4-predefined': ['C4 Instance Ram running in%'],
    'c4-cud-1y': ['Commitment v1: C4 Ram in%', '%for 1 Year'],
    'c4-cud-3y': ['Commitment v1: C4 Ram in%', '%for 3 Years'],
    'c4d-predefined': ['C4D Instance Ram running in%'],
    'c4d-cud-1y': ['Commitment v1: C4D Ram in%', '%for 1 Year'],
    'c4d-cud-3y': ['Commitment v1: C4D Ram in%', '%for 3 Years'],
    'z3-predefined': ['Z3 Instance Ram running in%'],
    'z3-cud-1y': ['Commitment v1: Z3 Ram in%', '%for 1 Year'],
    'z3-cud-3y': ['Commitment v1: Z3 Ram in%', '%for 3 Years'],
}

filtering_gpu_dict = {
    'nvidia-tesla-a100': ['Nvidia Tesla A100 GPU running in%'],
    'nvidia-tesla-a100-cud-1y': ['Commitment v1: Nvidia Tesla A100 GPU running in%', '%for 1 Year'],
    'nvidia-tesla-a100-cud-3y': ['Commitment v1: Nvidia Tesla A100 GPU running in%', '%for 3 Year'],
    'nvidia-tesla-t4': ['Nvidia Tesla T4 GPU running in%'],
    'nvidia-tesla-t4-cud-1y': ['Commitment v1: Nvidia Tesla T4 GPU running in%', '%for 1 Year'],
    'nvidia-tesla-t4-cud-3y': ['Commitment v1: Nvidia Tesla T4 GPU running in%', '%for 3 Year'],
    'nvidia-l4': ['Nvidia L4 GPU running in%'],
    'nvidia-l4-cud-1y': ['Commitment v1: Nvidia L4 GPU running in%', '%for 1 Year'],
    'nvidia-l4-cud-3y': ['Commitment v1: Nvidia L4 GPU running in%', '%for 3 Years'],
    'nvidia-h100-80gb': ['Nvidia H100 80GB GPU running in%'],
    'nvidia-h100-80gb-cud-1y': ['Commitment v1: Nvidia H100 80GB GPU running in%', '%for 1 Year'],
    'nvidia-h100-80gb-cud-3y': ['Commitment v1: Nvidia H100 80GB GPU running in%', '%for 3 Years'],
    'nvidia-h100-mega-80gb': ['Reserved Nvidia H100 80GB Mega GPU in%'],
    'nvidia-h100-mega-80gb-cud-1y': ['Commitment v1: Nvidia H100 80GB Mega GPU running in%', '%for 1 Year'],
    'nvidia-h100-mega-80gb-cud-3y': ['Commitment v1: Nvidia H100 80GB Mega GPU running in%', '%for 3 Years'],
    'nvidia-rtx-pro-6000': ['RTX 6000 96GB running in%'],
    'nvidia-rtx-pro-6000-cud-1y': ['Commitment v1: RTX 6000 96GB running in%', '%for 1 Year'],
    'nvidia-rtx-pro-6000-cud-3y': ['Commitment v1: RTX 6000 96GB running in%', '%for 3 Years'],
    'nvidia-b200': ['A4 Nvidia B200 (1 gpu slice) running in%'],
    'nvidia-b200-cud-1y': ['Commitment v1: A4 Nvidia B200 (1 gpu slice) in%', '%for 1 Year'],
    'nvidia-b200-cud-3y': ['Commitment v1: A4 Nvidia B200 (1 gpu slice) in%', '%for 3 Year%'],
}

filtering_disk_dict = {
    'local-ssd': ["SSD backed Local Storage' or sku.description like 'SSD backed Local Storage in%"],
    'local-ssd-cud-1y': ['Commitment v1: Local SSD in%', '%for 1 Year'],
    'local-ssd-cud-3y': ['Commitment v1: Local SSD in%', '%for 3 Year'],
    'pd-ssd': ["SSD backed PD Capacity' or sku.description like 'SSD backed PD Capacity in%"],
    'pd-balanced': ["Balanced PD Capacity' or sku.description like 'Balanced PD Capacity in%"],
    'pd-standard': ["Storage PD Capacity' or sku.description like 'Storage PD Capacity in%"],
    'pd-custom': ["Efficient Storage PD Capacity in%"]
}

filtering_nat_dict = {
    'nat-data': ['NAT Gateway: Data processing charge in%']
}


def get_bq_pricing_sku_list(filter_str: [str]) -> json:
    if not filter_str or len(filter_str) == 0:
        return None

    pricing_query = '''
        SELECT 
            sku.id, sku.description, (case
                when lower(sku.description) like '%delhi%' and array_length(geo_taxonomy.regions)=0 then ['asia-south2']
                else geo_taxonomy.regions
                end
            ) as regions
        FROM `bytedance-cloud-216403.billing.cloud_pricing_export` 
        WHERE 
        DATE(_PARTITIONTIME) = (select max(date(_PARTITIONTIME)) from `bytedance-cloud-216403.billing.cloud_pricing_export`)
        AND service.id='6F81-5844-456A' 
        AND sku.description like '{filter_string}' \n
    '''.format(filter_string=filter_str[0])

    if len(filter_str) > 1:
        pricing_query += "AND sku.description like '{filter_string}'".format(filter_string=filter_str[-1])

    client = bigquery.Client()
    query_job = client.query(pricing_query)

    results = query_job.result()  # Waits for job to complete.

    json_obj = [dict(row) for row in results]
    # json_obj = json.dumps(str(records))
    return json_obj


def region_to_sku(json_obj: json) -> dict:
    result_dict = {}
    for item in json_obj:
        # for skus of warsaw will need special care
        if 'Warsaw' in item['description']:
            # result_dict['europe-central2'] = item['description']
            result_dict['europe-central2'] = item['id']
            continue
        for region in item['regions']:
            # result_dict[region] = item['description']
            result_dict[region] = item['id']
    return result_dict


def gen_yaml_data(filter_dict: {}) -> json:
    yaml_data = {}
    for cost_type in filter_dict:
        json_obj = get_bq_pricing_sku_list(filter_dict[cost_type])
        yaml_data[cost_type] = region_to_sku(json_obj)
    return yaml_data


yaml_cpu_data = gen_yaml_data(filter_dict=filtering_cpu_dict)
with io.open('conf/sku_cpu.yaml', 'w', encoding='utf8') as outfile:
    yaml.dump(yaml_cpu_data, outfile, default_flow_style=False, allow_unicode=False)
#
yaml_ram_data = gen_yaml_data(filter_dict=filtering_ram_dict)
with io.open('conf/sku_ram.yaml', 'w', encoding='utf8') as outfile:
    yaml.dump(yaml_ram_data, outfile, default_flow_style=False, allow_unicode=False)

yaml_gpu_data = gen_yaml_data(filter_dict=filtering_gpu_dict)
with io.open('conf/sku_gpu.yaml', 'w', encoding='utf8') as outfile:
    yaml.dump(yaml_gpu_data, outfile, default_flow_style=False, allow_unicode=False)

yaml_nat_data = gen_yaml_data(filter_dict=filtering_nat_dict)
with io.open('conf/sku_nat.yaml', 'w', encoding='utf8') as outfile:
    yaml.dump(yaml_nat_data, outfile, default_flow_style=False, allow_unicode=False)

yaml_disk_data = gen_yaml_data(filter_dict=filtering_disk_dict)
with io.open('conf/sku_disk.yaml', 'w', encoding='utf8') as outfile:
    yaml.dump(yaml_disk_data, outfile, default_flow_style=False, allow_unicode=False)
