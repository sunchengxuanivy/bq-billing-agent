import datetime
from typing import Optional
from google.adk.tools import FunctionTool
import os
import sys
import yaml
from billing_agent.python_script.lib.instance import Instance
from billing_agent.python_script.lib.price import get_price_list, get_pricing_for_sku_from_bq
import json

def get_price(
    machine_type: str,
    region: str,
) -> str:
    try:
        # Load SKU mappings from yaml files
        with open("billing_agent/python_script/conf/sku_cpu.yaml", 'r') as sku_yaml:
            sku_cpu_dict = yaml.safe_load(sku_yaml)
        with open("billing_agent/python_script/conf/sku_ram.yaml", 'r') as sku_yaml:
            sku_ram_dict = yaml.safe_load(sku_yaml)
        with open("billing_agent/python_script/conf/sku_gpu.yaml", 'r') as sku_yaml:
            sku_gpu_dict = yaml.safe_load(sku_yaml)
        with open("billing_agent/python_script/conf/sku_disk.yaml", 'r') as sku_yaml:
            sku_disk_dict = yaml.safe_load(sku_yaml)

        # Get price lists
        all_prices = get_price_list()
        # The Instance class expects a dict with sku_id -> price.
        # We take the first price from the tiered rates for simplicity.
        list_price_dict = {k: v['list_price_details']['tiered_rates'][0]['usd_amount'] for k, v in all_prices.items() if v.get('list_price_details', {}).get('tiered_rates')}
        final_price_dict = {k: v['final_price_details']['tiered_rates'][0]['usd_amount'] for k, v in all_prices.items() if v.get('final_price_details', {}).get('tiered_rates')}

        # Create an Instance and calculate the price
        machine = Instance(machine_type)
        result = machine.price_result_in_json(
            region,
            list_price_dict,
            final_price_dict,
            sku_cpu_dict,
            sku_ram_dict,
            sku_gpu_dict,
            sku_disk_dict
        )
        return str(result)
    except Exception as e:
        return f"An error occurred: {e}"

pricing_tool = FunctionTool(get_price)

def get_price_for_sku(
    sku_id: str,
) -> str:
    try:
        result = get_pricing_for_sku_from_bq(sku_id)

        def default_serializer(o):
            if isinstance(o, (datetime.date, datetime.datetime)):
                return o.isoformat()
            if hasattr(o, '_asdict'): # For Row objects
                return o._asdict()
            raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

        return json.dumps(result, indent=4, default=default_serializer)
    except Exception as e:
        return f"An error occurred: {e}"

sku_pricing_tool = FunctionTool(get_price_for_sku)