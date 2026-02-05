"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""


class DiskType(object):
    def __init__(self, disk_definition):
        self.disk_definition = disk_definition
        self.has_localssd = False
        self.localssd_size = 0

        if not disk_definition:
            self.boot_disk_type = 'pd-standard'
            self.boot_disk_size = 0
        else:
            disk_details = disk_definition.split('_')
            for item in disk_details:
                if 'local-ssd' in item:
                    self.has_localssd = True
                    self.localssd_size = int(item.split('-')[-1])
                    continue

                self.boot_disk_type = '-'.join(item.split('-')[:-1])
                self.boot_disk_size = int(item.split('-')[-1])
