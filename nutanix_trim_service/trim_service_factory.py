#!/usr/bin/python
#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
# Author: gokul.kannan@nutanix.com
#
# Factory class to generate VM Trim Service class by the vm type.

__all__ = ['VMTrimServiceFactory']

class VMTrimServiceFactory(object):
  """
  Factory class to generate VM Trim Service class by the vm's type.
  """
  @staticmethod
  def get_nutanix_trim_service(run_on_uvm=False):
    """
    Get the correct VMTrim class based on the run_on_svm param.
    """
    if run_on_uvm:
      from uvm_trim_service import UVMTrim
      return UVMTrim()
    else:
      from svm_trim_service import SVMTrim
      return SVMTrim()
