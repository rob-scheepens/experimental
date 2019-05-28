#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
# Author: van@nutanix.com
#
# This is for scraping Nutanix web pages.
# StargateWeb = for scraping Stargate web page.
#
from BeautifulSoup import BeautifulSoup

import os
import datetime
import gflags
import re
import urllib2

from util.base.log import *

FLAGS = gflags.FLAGS

class NutanixWeb(object):
  """
  This is the base class for connecting to Nutanix Web pages.
  """
  def __init__(self, ip, port, timeout):
    self.url = "http://%s:%s" % (ip, port)
    self.page_data = urllib2.urlopen(self.url, timeout=timeout).read()
    self.htmldom = BeautifulSoup(self.page_data)

  def get_all_tables(self):
    """
    Return a list of all table tags.
    """
    tables = self.htmldom.findAll("table")
    return tables;

  def parse_table(self, table):
    """
    Given a table element return a list of row dictionary object.  The row
    object contain key/value of each column.  The keys are from the column
    headers and the values are data rows.
    Example:
    -----------
    |A | B | C|
    -----------
    |1 | 2 | 3|
    |4 | 5 | 6|
    -----------
    Here is the results :
    [{A:1, B:2, C:3}, {A:4, B:5, C:6}]

    @param table:  This is the table tag from BeautifulSoup.
    """
    results = []
    rows = table.findAll("tr")
    header_str_list = self.parse_table_header(table)
    for row in rows:
      data_columns = row.findAll("td")
      if not data_columns:
        # This must be a header column.  Skip it.
        continue
      item = {}
      if header_str_list:
        for header, data in zip(header_str_list, data_columns):
          item[header] = data.text
      else:
        # Headerless table; insert data using the column index as the key.
        for index, data in enumerate(data_columns):
          item[index] = data.text
      results.append(item)
    return results

  def parse_table_header(self, table):
    """
    Given a table return a list of column header strings.
    NOTE: In some cases like stargate page, the table has 2 header rows.
    This should take care of that.
    @param table:  This is the table tag from BeautifulSoup.
    """
    rows = table.findAll("tr");
    header_columns = rows[0].findAll("th")
    header_columns2 = rows[1].findAll("th")
    if header_columns2:
      hcount = 0
      results = []
      for header in header_columns:
        colspan = header.get("colspan", 0)
        # If the header has a column span then we need to include
        # the header text in the second row.
        if colspan > 0:
          for ii in range(int(colspan)):
            results.append(header.text + " - " + header_columns2[hcount].text)
            hcount += 1
        else:
          results.append(header.text)
          hcount += 1
    else:
      results = [header.text for header in header_columns]
    return results

class StargateWeb(NutanixWeb):
  """
  This class is used to scrap the content of Stargate web page at port 2009.
  The general format of the web page is a list of tables.  Each data table
  is immediately following the title table.
  """
  def __init__(self, ip, port=2009, timeout=60):
    super(StargateWeb, self).__init__(ip, port, timeout)

  def find_table_containt_text(self, text):
    """
    @summary: Given a text string return the table and index that matched.
    This is used to look up the title table.
    """
    for index, table in enumerate(self.get_all_tables()):
      if table.text == text:
        return index, table;
    return None, None

  def get_nfs_master(self):
    """
    Return the ip of the NFS master
    """
    reg_ex="NFS master handle:\\s*<.*?>"
    text_1 = re.split(reg_ex, self.page_data)
    text_1=text_1[1]
    reg_ex_2 = ":"
    ip = re.split(reg_ex_2,text_1,1)
    ip = ip[0]
    return ip

  def get_hosted_vdisk_table(self):
    """
    @return: the hosted vdisk table from the stargate web page.
    """
    stargate_title = "Hosted&nbsp;Active&nbsp;VDisks"
    # The table after the title table is the vdisk table.
    index, table = self.find_table_containt_text(stargate_title)
    CHECK(index, "Failed to find the table that contains this title : %s "
          "on this web page %s" %
          (stargate_title, self.url))
    return self.get_all_tables()[index + 1]

  def get_hosted_vdisks_list(self, contains_name=None):
    """
    @param contains_name(str): Return list of vdisks that contains this name.
    If None then return all vdisks.
    Note: Hosted vdisks table only show vdisks that are currently active with
    IO.
    See super.parse_table() for the return data type.
    """
    vdisk_table = self.get_hosted_vdisk_table()
    vdisks = self.parse_table(vdisk_table)
    if contains_name:
      filtered_vdisks = []
      for vdisk in vdisks:
        if contains_name in vdisk["VDisk Name"]:
          filtered_vdisks.append(vdisk)
      vdisks = filtered_vdisks

    return vdisks;

  def get_hosted_vdisk_names(self, contains_name=None):
    vdisks = self.get_hosted_vdisks_list(contains_name)
    vdisk_names = [vdisk["VDisk Name"] for vdisk in vdisks]
    return vdisk_names

  # TODO : Add other methods here that pretain to stargate page as needed.

  def is_oplog_flushed(self):
    """
    Return True if Oplog is flushed for all vdisks in hosted vdisk table,
    otherwise False.
    """
    hosted_vdisk_table = self.get_hosted_vdisk_table()
    hosted_vdisk_dicts = self.parse_table(hosted_vdisk_table)
    for hosted_vdisk_dict in hosted_vdisk_dicts:
      for col_name in hosted_vdisk_dict.keys():
        if "Oplog" not in col_name:
          continue
        # For "Oplog" column, check that the cell is "0".
        if hosted_vdisk_dict[col_name] != "0":
          return False
    return True

class StargateWebOptions(NutanixWeb):
  """
  This class is used to scrap the content of Stargate web page at port 2009,
  for different stargate options.
  The general format of the web page is a list of tables.  Each data table
  is immediately following the title table.
  """
  def __init__(self, cluster, port=2009, timeout=5, option_name=""):
    self.tail_string = ""
    self.option_name = option_name
    if option_name == "activity_trace":
      self.tail_string = "h/traces?low=0&high=&expand=&a=completed&c=all"
    self.cluster = cluster
    self.port = port
    self.timeout = timeout

  def save_to_file(self):
    dump_files = []
    for svm in self.cluster.svms():
      url = "http://%s:%s/%s" % (svm.ip(), self.port, self.tail_string)
      page_data = urllib2.urlopen(urllib2.Request(url)).read()
      filename = ".".join([
        "stargate_%s_%s" % (self.option_name, svm.ip()),
        "%s" % datetime.datetime.now().strftime("%Y%m%d-%H%M%S.%f"),
        "html"])
      dump_file = os.path.join(FLAGS.test_out_dir, filename)
      file = open(dump_file, "w")
      file.write(page_data)
      file.close
      # Using ERROR here is to put the message in the same error log,
      # for convinience of locating corresponding failure backtrace
      ERROR("Saved stargate activity trace to %s" % dump_file)

class StargateIscsiWeb(NutanixWeb):
  """
  This class is used to scrap the content of Stargate Iscsi web page at port
  2009, The general format of the web page is a list of tables.  Each data table
  is immediately following the title table.
  """
  def __init__(self, ip, port=2009, timeout=5):
    self.url = "http://%s:%s/iscsi" % (ip, port)
    self.page_data = urllib2.urlopen(self.url, timeout=timeout).read()
    self.htmldom = BeautifulSoup(self.page_data)

  def get_iscsi_session_info(self):
    """Parse the iscsi session info table and return a dict."""
    tables = self.get_all_tables()
    table_info = self.parse_table(tables[2])
    return table_info

  def get_iscsi_active_session_lun_info(self):
    """ Parse the iSCSI LUN from Active Session table and return a dict"""
    tables = self.get_all_tables()
    table_info = self.parse_table(tables[1])
    return table_info
