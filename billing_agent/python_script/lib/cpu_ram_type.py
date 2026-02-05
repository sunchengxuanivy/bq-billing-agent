"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""
import logging
import re
import os
from google.cloud import compute_v1


def get_machine_type_details(project_id: str, machine_type_name: str) -> tuple:
    """
    Retrieves the details for a specific machine type from the Compute Engine API.

    Args:
        project_id: The GCP project ID.
        machine_type_name: The name of the machine type to retrieve.

    Returns:
        A tuple containing the machine type details (family, cpus, memory),
        or None if the machine type is not found.
    """
    client = compute_v1.MachineTypesClient()
    request = compute_v1.AggregatedListMachineTypesRequest(
        project=project_id,
        filter=f'name = "{machine_type_name}"'
    )

    # The aggregated_list method returns an iterator of tuples.
    for _, response in client.aggregated_list(request=request):
        if response.machine_types:
            for machine_type in response.machine_types:
                if machine_type.name == machine_type_name:
                    family = machine_type.name.split('-')[0]
                    return (
                        family,
                        machine_type.guest_cpus,
                        machine_type.memory_mb / 1024.0
                    )
    return None


class CpuRamType(object):
    def __init__(self, machine_type, project_id=os.environ.get('BQ_PROJECT_ID', 'hk-tam-playground')):
        self.machine_type = machine_type
        if 'custom' in machine_type:
            self.is_custom = True
            self.cpu_ram_type, self.cpu_no, self.ram_gb = process_custom_machine_type(
                machine_type)
        else:
            self.is_custom = False

            # in order to deal with some bytedance defined machined types
            # will need to do some string manipulation

            machine_type = machine_type.replace('-nps4', '')
            machine_type = re.sub(r'-[0-9]+lssd', '', machine_type)
            machine_type = re.sub(r'-ssd[0-9]+t', '', machine_type)
            self.cpu_ram_type, self.cpu_no, self.ram_gb = process_predefined_machine_type(
                machine_type, project_id
            )
        pass


def process_predefined_machine_type(machine_type: str, project_id: str) -> (str, float, float):
    """
    Processes a predefined machine type by looking it up using the Compute Engine API.
    """
    details = get_machine_type_details(project_id, machine_type)

    if details is None:
        logging.error(
            f'{machine_type} is not a pre-defined machine type or it is not supported in this project.'
        )
        raise Exception('Wrong pre-defined machine type.')

    return details


def process_custom_machine_type(machine_type: str) -> (str, float, float):
    detailed_info = machine_type.split('-')
    if len(detailed_info) == 4:
        return detailed_info[0], float(detailed_info[2]), float(detailed_info[-1]) / 1024
    else:
        return 'n1', float(detailed_info[-2]), float(detailed_info[-1]) / 1024,
