"""
Copyright 2021 Google. This software is provided as-is, without warranty or representation for any use or purpose.
Your use of it is subject to your agreement with Google.
"""


class GpuType(object):
    def __init__(self, gpu_type: str, gpu_no: int):
        self.gpu_type = gpu_type
        self.gpu_no = gpu_no
