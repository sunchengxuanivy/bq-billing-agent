"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""
import argparse
import logging
import sys
import traceback
import yaml

from lib.instance import Instance
from lib.price import get_price_list


def process_args(args: [str]) -> argparse.Namespace:
    """
    To process input arguments. and retrieve instance type and region.
    :param args:
    string list of input arguments, e.g. ['-i', 'n2-custom-128-524288', '-z', 'us-central1']
    :return:
    3-item string tuple, first of which is instance type, second of which is region,
    and the third of which is disk type.
    """
    parser = argparse.ArgumentParser(
        prog='python3 billing_price.py',
        description='To show pricing information of given instance/disk info.')
    parser.add_argument('sku_type', type=str, choices=['instance', 'disk'],
                        help='SKU type, value is either instance or disk.')
    parser.add_argument('-i', type=str, required=True,
                        help='instance/disk information, like nvidia-tesla-t4_4_custom-48-163840, local-ssd or n2-highmem-80')
    parser.add_argument('-z', type=str, default='us-east4',
                        help='region, default is us-east4')
    parser.add_argument('-d', type=str, help='<Deprecated> disk type, this option will not be used.')

    return parser.parse_args(args)


if __name__ == '__main__':
    # python3 billing_price.py instance|disk -i n2-custom-128-524288 -z us-west4 -d local-ssd-9000_pd-standard-200
    logging.basicConfig(level=logging.WARNING)

    # process input parameters
    namespace = process_args(sys.argv[1::])
    sku_type = namespace.sku_type
    info = namespace.i
    region = namespace.z

    # Get SKU mappings from yaml files, [service][region] --> SKU_ID
    with open("conf/sku_cpu.yaml", 'r') as sku_yaml:
        sku_cpu_dict = yaml.safe_load(sku_yaml)
    with open("conf/sku_ram.yaml", 'r') as sku_yaml:
        sku_ram_dict = yaml.safe_load(sku_yaml)
    with open("conf/sku_gpu.yaml", 'r') as sku_yaml:
        sku_gpu_dict = yaml.safe_load(sku_yaml)
    with open("conf/sku_disk.yaml", 'r') as sku_yaml:
        sku_disk_dict = yaml.safe_load(sku_yaml)

    # Get list price and account price of all SKUs from BigQuery pricing table
    list_price_dict, final_price_dict = get_price_list()
    machine_type_list = [
        'c3-standard-176',
        'tdx_c3-standard-176',
    ]

    for machine_type in machine_type_list:
        try:
            machine = Instance(machine_type)
        except:
            logging.error(traceback.format_exc())
            continue
        result = machine.price_result_in_json(
            'us-east4',
            list_price_dict,
            final_price_dict,
            sku_cpu_dict,
            sku_ram_dict,
            sku_gpu_dict,
            sku_disk_dict
        )
        print(
            f'{machine_type},{result.get("ondemand").get("list_price")},{result.get("ondemand").get("account_price")},{result.get("3cud").get("account_price")}')
    exit(0)
    if sku_type == 'instance':
        #  Init and do calculation
        machine = Instance(info)

        result = machine.price_result_in_json(
            region,
            list_price_dict,
            final_price_dict,
            sku_cpu_dict,
            sku_ram_dict,
            sku_gpu_dict,
            sku_disk_dict
        )
        print(result)
    else:
        result = {'disk_type': info, 'region': region}
        if info in sku_disk_dict.keys():
            sku = sku_disk_dict[info].get(region)
            if sku:
                result["ondemand"] = {
                    'list_price': list_price_dict[sku],
                    'account_price': final_price_dict[sku]
                }
        if f'{info}-cud-1y' in sku_disk_dict.keys():
            sku = sku_disk_dict[f'{info}-cud-1y'].get(region)
            if sku:
                result['1cud'] = {
                    'list_price': list_price_dict[sku],
                    'account_price': final_price_dict[sku]
                }
        if f'{info}-cud-3y' in sku_disk_dict.keys():
            sku = sku_disk_dict[f'{info}-cud-3y'].get(region)
            result['3cud'] = {
                'list_price': list_price_dict[sku],
                'account_price': final_price_dict[sku]
            }

        print(result)
