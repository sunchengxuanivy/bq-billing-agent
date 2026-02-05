"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""
import json
import logging

from .disk_type import DiskType
from .gpu_type import GpuType
from .cpu_ram_type import CpuRamType

TOTAL_HOURS = 730


class Instance(object):
    def __init__(self, machine_definition: str, disk_definition: str = None):
        self.has_gpu = False
        self.is_confidential = False
        self.gpu_type = None

        gpu_type_str = None
        gpu_num = 0

        if machine_definition:
            machine_details = machine_definition.split('_')
            for item in machine_details:
                if 'nvidia' in item:
                    gpu_type_str = item
                    self.has_gpu = True
                    continue
                if item.isdigit():
                    gpu_num = int(item)
                    continue

                if 'tdx' in item:
                    self.is_confidential = True
                    continue

                self.machine_type = CpuRamType(item)

            if self.has_gpu:
                self.gpu_type = GpuType(gpu_type=gpu_type_str, gpu_no=gpu_num)
                self.machine_definition = '_'.join([
                    self.gpu_type.gpu_type,
                    str(self.gpu_type.gpu_no),
                    self.machine_type.machine_type
                ])
            else:
                self.machine_definition = self.machine_type.machine_type

        self.disk_type = DiskType(disk_definition)

    def cpu_fee(self,
                region: str,
                price_dict: dict,
                sku_cpu_dict: {},
                is_cud_3y: bool = False,
                is_cud_1y: bool = False,
                ) -> float:
        """
        This function is to calculate CPU fee

        :param region: (str) e.g. asia-sourtheast1, us-central1, us-east4
        :param price_dict: (dict) pricing reference of mapping sku_id to price.
        :param sku_cpu_dict: (dict) sku reference of mapping internal cpu names to sku_id.
            it can be generated from conf/sku_cpu.yaml.
            e.g. sku_cpu_dict['a2-cud-1y']['asia-east1'] = '38FA-6071-3D88'
        :param is_cud_3y: (bool) will return 3-year CUD pricing if set to True.
        :param is_cud_1y: (bool) will return 1-year CUD pricing if set to True.
            Please note that in case both is_cud_3y and is_cud_1y are set as True, the function will return
            3-year CUD pricing.
        :return:
            if is_cud_3y == True:
                will return 3-year CUD CPU pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
            if is_cud_1y == True:
                will return 1-year CUD CPU pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
            if is_cud_3y == is_cud_1y == False:
                will return on demand CPU pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
        """
        if is_cud_3y:
            key = f'{self.machine_type.cpu_ram_type}-cud-3y'
        elif is_cud_1y:
            key = f'{self.machine_type.cpu_ram_type}-cud-1y'
        elif self.machine_type.is_custom:
            key = f'{self.machine_type.cpu_ram_type}-custom'
        else:
            key = f'{self.machine_type.cpu_ram_type}-predefined'

        sku_id = sku_cpu_dict[key][region]
        price = price_dict.get(sku_id, 0)

        total_price = price * self.machine_type.cpu_no * TOTAL_HOURS
        if self.machine_type.cpu_ram_type == 'e2' and self.machine_type.is_custom:
            logging.debug(
                f'CPU: {key}|{sku_id}, unit price:{price}, total_price:{total_price}')
            return total_price
        else:
            logging.debug(
                f'CPU: {key}|{sku_id}, unit price:{price}, total_price:{total_price}')
            return total_price

    def ram_fee(self,
                region: str,
                price_dict: {},
                sku_ram_dict: {},
                is_cud_3y: bool = False,
                is_cud_1y: bool = False,
                ) -> float:
        """
        This function is to calculate RAM fee

        :param region: (str) e.g. asia-sourtheast1, us-central1, us-east4
        :param price_dict: (dict) pricing reference of mapping sku_id to price.
        :param sku_ram_dict: (dict) sku reference of mapping internal ram names to sku_id.
            it can be generated from conf/sku_ram.yaml.
            e.g. sku_ram_dict['a2-cud-1y']['asia-east1'] = '6B34-DDB8-7812'
        :param is_cud_3y: (bool) will return 3-year CUD pricing if set to True.
        :param is_cud_1y: (bool) will return 1-year CUD pricing if set to True.
            Please note that in case both is_cud_3y and is_cud_1y are set as True, the function will return
            3-year CUD pricing.
        :return:
            if is_cud_3y == True:
                will return 3-year CUD RAM pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
            if is_cud_1y == True:
                will return 1-year CUD RAM pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
            if is_cud_3y == is_cud_1y == False:
                will return on demand RAM pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
        """
        if is_cud_3y:
            key = f'{self.machine_type.cpu_ram_type}-cud-3y'
        elif is_cud_1y:
            key = f'{self.machine_type.cpu_ram_type}-cud-1y'
        elif self.machine_type.is_custom:
            key = f'{self.machine_type.cpu_ram_type}-custom'
        else:
            key = f'{self.machine_type.cpu_ram_type}-predefined'

        sku_id = sku_ram_dict[key][region]
        price = price_dict.get(sku_id, 0)

        total_price = price * self.machine_type.ram_gb * TOTAL_HOURS
        if self.machine_type.cpu_ram_type == 'e2' and self.machine_type.is_custom:
            logging.debug(
                f'RAM: {key}|{sku_id}, unit price:{price}, total_price:{total_price}')
            return total_price
        else:
            logging.debug(
                f'RAM: {key}|{sku_id}, unit price:{price}, total_price:{total_price}')
            return total_price

    def gpu_fee(self,
                region: str,
                price_dict: {},
                sku_gpu_dict: {},
                is_cud_3y: bool = False,
                is_cud_1y: bool = False,
                ) -> float:
        """
        This function is to calculate GPU fee

        :param region: (str) e.g. asia-sourtheast1, us-central1, us-east4
        :param price_dict: (dict) pricing reference of mapping sku_id to price.
        :param sku_gpu_dict: (dict) sku reference of mapping internal gpu names to sku_id.
            it can be generated from conf/sku_gpu.yaml.
            e.g. sku_gpu_dict['nvidia-tesla-a100']['asia-east1'] = 'DB4C-F9D7-22BB'
        :param is_cud_3y: (bool) will return 3-year CUD pricing if set to True.
        :param is_cud_1y: (bool) will return 1-year CUD pricing if set to True.
            Please note that in case both is_cud_3y and is_cud_1y are set as True, the function will return
            3-year CUD pricing.
        :return:
            if is_cud_3y == True:
                will return 3-year CUD GPU pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
            if is_cud_1y == True:
                will return 1-year CUD GPU pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
            if is_cud_3y == is_cud_1y == False:
                will return on demand GPU pricing without SUD discount lasting 730 hours, as set in TOTAL_HOURS.
        """
        if not self.has_gpu:
            return 0
        if is_cud_3y:
            key = f'{self.gpu_type.gpu_type}-cud-3y'
        elif is_cud_1y:
            key = f'{self.gpu_type.gpu_type}-cud-1y'
        else:
            key = self.gpu_type.gpu_type

        sku_id = sku_gpu_dict[key][region]
        if not sku_id:
            raise Exception(
                f'There is no {key} under {region}, please double check.')
        price = price_dict.get(sku_id, 0)
        total_price = price * self.gpu_type.gpu_no * TOTAL_HOURS
        logging.debug(
            f'GPU: {key}|{sku_id}, unit price:{price}, total_price:{total_price}')
        return total_price

    def disk_fee(self,
                 region: str,
                 price_dict: {},
                 sku_disk_dict: {},
                 is_cud_3y: bool = False,
                 is_cud_1y: bool = False,
                 ) -> float:
        """
        This function is to calculate GPU fee

        :param region: (str) e.g. asia-sourtheast1, us-central1, us-east4
        :param price_dict: (dict) pricing reference of mapping sku_id to price.
        :param sku_disk_dict: (dict) sku reference of mapping internal disk names to sku_id.
            it can be generated from conf/sku_disk.yaml.
            e.g. sku_disk_dict['local-ssd']['asia-east1'] = '62AF-A39E-269B'
        :param is_cud_3y: (bool) will return 3-year CUD pricing if set to True.
        :param is_cud_1y: (bool) will return 1-year CUD pricing if set to True.
            Please note that in case both is_cud_3y and is_cud_1y are set as True, the function will return
            3-year CUD pricing.
        :return:
            if is_cud_3y == True:
                will return 3-year CUD disk pricing without SUD discount lasting a month.
            if is_cud_1y == True:
                will return 1-year CUD disk pricing without SUD discount lasting a month.
            if is_cud_3y == is_cud_1y == False:
                will return on demand disk pricing without SUD discount lasting a month.

            Only Local SSD has CUD discount.
        """
        boot_disk_key = f'{self.disk_type.boot_disk_type}'
        book_disk_sku_id = sku_disk_dict[boot_disk_key][region]
        if not book_disk_sku_id:
            raise Exception(
                f'There is no {boot_disk_key} under {region}, please double check.')
        boot_disk_price = price_dict.get(book_disk_sku_id, 0)
        boot_disk_fee = boot_disk_price * self.disk_type.boot_disk_size
        logging.debug(
            f'BOOT_DISK: {boot_disk_key}|{book_disk_sku_id}, unit price:{boot_disk_price}, total_price:{boot_disk_fee}')
        if not self.disk_type.has_localssd:
            return boot_disk_fee

        if is_cud_3y:
            local_ssd_key = 'local-ssd-cud-3y'
        elif is_cud_1y:
            local_ssd_key = 'local-ssd-cud-1y'
        else:
            local_ssd_key = 'local-ssd'

        local_ssd_sku_id = sku_disk_dict[local_ssd_key][region]
        local_ssd_price = price_dict.get(local_ssd_sku_id, 0)
        local_ssd_fee = local_ssd_price * self.disk_type.localssd_size
        logging.debug(
            f'Local_SSD: {local_ssd_key}|{local_ssd_sku_id}, unit price:{local_ssd_price}, total_price:{local_ssd_fee}')
        logging.debug(
            f'BOOT_DISK+Local_SSD total_price:{local_ssd_fee + boot_disk_fee}')
        return local_ssd_fee + boot_disk_fee

    def price_result_in_json(self,
                             region: str,
                             list_price_dict: dict,
                             final_price_dict: dict,
                             sku_cpu_dict: dict,
                             sku_ram_dict: dict,
                             sku_gpu_dict: dict,
                             sku_disk_dict: dict,
                             ) -> json:
        """

        :param region: (str) e.g. asia-sourtheast1, us-central1, us-east4
        :param list_price_dict: (dict) The latest list price reference of COMPUTE ENGINE
            e.g. list_price_dict['1C2E-893A-C634'] = 0.0396
        :param final_price_dict: (dict) The latest account price reference of COMPUTE ENGINE
            e.g. final_price_dict['1C2E-893A-C634'] = 0.02376
        :param sku_cpu_dict: (dict) sku reference of mapping internal cpu names to sku_id.
            it can be generated from conf/sku_cpu.yaml.
            e.g. sku_cpu_dict['a2-cud-1y']['asia-east1'] = '38FA-6071-3D88'
        :param sku_ram_dict: (dict) sku reference of mapping internal ram names to sku_id.
            it can be generated from conf/sku_ram.yaml.
            e.g. sku_ram_dict['a2-cud-1y']['asia-east1'] = '6B34-DDB8-7812'
        :param sku_gpu_dict: (dict) sku reference of mapping internal gpu names to sku_id.
            it can be generated from conf/sku_gpu.yaml.
            e.g. sku_gpu_dict['nvidia-tesla-a100']['asia-east1'] = 'DB4C-F9D7-22BB'
        :param sku_disk_dict: (dict) sku reference of mapping internal disk names to sku_id.
            it can be generated from conf/sku_disk.yaml.
            e.g. sku_disk_dict['local-ssd']['asia-east1'] = '62AF-A39E-269B'
        :return:
        """
        result = {}

        logging.debug(
            "===================================================================================")
        logging.debug('Calculating on demand list price...')
        on_demand_list = self.cpu_fee(region, list_price_dict, sku_cpu_dict) + \
                         self.ram_fee(region, list_price_dict, sku_ram_dict) + \
                         self.gpu_fee(region, list_price_dict, sku_gpu_dict) + \
                         self.disk_fee(region, list_price_dict, sku_disk_dict)
        logging.debug(
            "===================================================================================")
        logging.debug('Calculating on demand account price...')
        on_demand_account = self.cpu_fee(region, final_price_dict, sku_cpu_dict) + \
                            self.ram_fee(region, final_price_dict, sku_ram_dict) + \
                            self.gpu_fee(region, final_price_dict, sku_gpu_dict) + \
                            self.disk_fee(region, final_price_dict, sku_disk_dict)
        logging.debug(
            "===================================================================================")
        logging.debug('Calculating 1-year CUD list price...')
        cud_1y_list = self.cpu_fee(region, list_price_dict, sku_cpu_dict, is_cud_1y=True) + \
                      self.ram_fee(region, list_price_dict, sku_ram_dict, is_cud_1y=True) + \
                      self.gpu_fee(region, list_price_dict, sku_gpu_dict, is_cud_1y=True) + \
                      self.disk_fee(region, list_price_dict, sku_disk_dict, is_cud_1y=True)
        logging.debug("===================================================================================")
        logging.debug('Calculating 1-year CUD account price...')
        cud_1y_account = self.cpu_fee(region, final_price_dict, sku_cpu_dict, is_cud_1y=True) + \
                         self.ram_fee(region, final_price_dict, sku_ram_dict, is_cud_1y=True) + \
                         self.gpu_fee(region, final_price_dict, sku_gpu_dict, is_cud_1y=True) + \
                         self.disk_fee(region, final_price_dict, sku_disk_dict, is_cud_1y=True)
        logging.debug("===================================================================================")
        logging.debug('Calculating 3-year CUD list price...')
        cud_3y_list = self.cpu_fee(region, list_price_dict, sku_cpu_dict, is_cud_3y=True) + \
                      self.ram_fee(region, list_price_dict, sku_ram_dict, is_cud_3y=True) + \
                      self.gpu_fee(region, list_price_dict, sku_gpu_dict, is_cud_3y=True) + \
                      self.disk_fee(region, list_price_dict,
                                    sku_disk_dict, is_cud_3y=True)
        logging.debug(
            "===================================================================================")
        logging.debug('Calculating 3-year CUD account price...')
        cud_3y_account = self.cpu_fee(region, final_price_dict, sku_cpu_dict, is_cud_3y=True) + \
                         self.ram_fee(region, final_price_dict, sku_ram_dict, is_cud_3y=True) + \
                         self.gpu_fee(region, final_price_dict, sku_gpu_dict, is_cud_3y=True) + \
                         self.disk_fee(region, final_price_dict,
                                       sku_disk_dict, is_cud_3y=True)

        if self.is_confidential:
            list_conf_cpu = list_price_dict['E5BD-12DD-EB30'] * TOTAL_HOURS * self.machine_type.cpu_no
            account_conf_cpu = final_price_dict['E5BD-12DD-EB30'] * TOTAL_HOURS * self.machine_type.cpu_no
            list_conf_ram = list_price_dict['3999-11C3-1EE4'] * TOTAL_HOURS * self.machine_type.ram_gb
            account_conf_ram = final_price_dict['3999-11C3-1EE4'] * TOTAL_HOURS * self.machine_type.ram_gb
            on_demand_list += (list_conf_cpu + list_conf_ram)
            on_demand_account += (account_conf_cpu + account_conf_ram)
            cud_3y_list += (list_conf_cpu + list_conf_ram)
            cud_3y_account += (account_conf_cpu + account_conf_ram)

        ondemand_dict = {
            "list_price": round(on_demand_list, 2),
            "account_price": round(on_demand_account, 2)
        }

        cud_1y_dict = {
            "list_price": round(cud_1y_list, 2),
            "account_price": round(cud_1y_account, 2)
        }

        cud_3y_dict = {
            "list_price": round(cud_3y_list, 2),
            "account_price": round(cud_3y_account, 2)
        }

        result['ondemand'] = ondemand_dict
        result['1cud'] = cud_1y_dict
        result['3cud'] = cud_3y_dict
        result['region'] = region
        result['instance_type'] = self.machine_definition
        result['disk_type'] = self.disk_type.disk_definition
        result['calculation_period'] = 'monthly (based on 730 hours)'
        return result
        # return json.dumps(result)
