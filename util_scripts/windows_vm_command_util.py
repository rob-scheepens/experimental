#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Author: aramesh@nutanix.com
#
# Utility containing a collection of common methods for Windows VMs.
# It uses WsmanRemoteShell to execute commands on the Windows guests so it is
# necessary that the VMs allow WinRM communication using Basic Authentication.
#
__all__ = ["WindowsVMCommandUtil"]

import errno
import gflags
import os
import socket

FLAGS = gflags.FLAGS

from util.base.build import top_dir
from shutil import copyfile
from util.base.log import ERROR, FATAL, INFO
from util.net.wsman_remote_shell import BasicWsmanRemoteShell, \
  WsmanRemoteShellException

class WindowsVMCommandUtil(object):
  """
  This class contains common commands to be run on Windows VMs.
  """

  WINDOWS_VM_MODULE_PATH = "qa/py/qa/agave/windows_guest_ps_module"

  @staticmethod
  def transfer_powershell_module(uvm, out_dir, port,
                                 username="Administrator",
                                 password="nutanix/4u"):
    """
    Transfer Powershell modules to the VM at
    ${Env:ProgramFiles}\WindowsPowershell\Modules.
    uvm: Windows VM for which the Powershell module needs to be transferred.
    out_dir: Test runner output directory from where the files should be
             copied.
    port: Http server port from which the Windows VM will be able to pull the
          modules.
    username: Username of the account to access the Windows VM
    password: Password for the above account.
    Returns True on success, False on failure.
    """
    hyperv_ps_modules_dir = (
        "%s/%s" % (top_dir(), WindowsVMCommandUtil.WINDOWS_VM_MODULE_PATH))
    agave_posh_modules = []
    for (root, _, filenames) in os.walk(hyperv_ps_modules_dir):
      for filename in filenames:
        if filename.endswith(".psm1"):
          module_path = os.path.join(root, filename)
          agave_posh_modules.append(module_path)
    for agave_posh_module in agave_posh_modules:
      try:
        module_basename = os.path.basename(agave_posh_module)
        os.symlink(agave_posh_module, "%s/%s" % (out_dir, module_basename))
      except OSError as ose:
        if ose.errno != errno.EEXIST:
          ERROR("Failed to create a symlink to the Hyper-V Agave PS module "
                "in %s: %s" % (out_dir, str(ose)))

    succeeded = True
    for agave_posh_module in agave_posh_modules:
      module_basename = os.path.basename(agave_posh_module)
      module_name_no_extension = module_basename.rsplit(".psm1")[0]
      remote_path = ("${Env:ProgramFiles}\WindowsPowershell\Modules\%s" %
                     module_name_no_extension)
      INFO("Transferring the NutanixWindows Powershell modules to %s"
           % uvm.ip())

      # Determine the local IP that has reachability to this host.
      sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      sock.connect((uvm.ip(), 80)) # Port number doesn't matter.
      local_ip = sock.getsockname()[0]
      sock.close()
      install_posh_module_cmd = """
        $ErrorActionPreference = "Stop"
        $dest = New-Item -ItemType Directory -Path {remote_path} -Force
        Invoke-WebRequest http://{ip}:{port}/output/{module_name} `
          -OutFile $dest\\{module_name}""".format(remote_path=remote_path,
                                                  ip=local_ip, port=port,
                                                  module_name=module_basename)
      if FLAGS.agave_verbose:
        INFO("Transferring the %s Powershell module to %s using command: %s" %
             (module_name_no_extension, uvm.ip(), install_posh_module_cmd))
      ret, _, stderr = WindowsVMCommandUtil.execute_command(
          uvm, install_posh_module_cmd, username=username, password=password)
      if ret or stderr.strip():
        ERROR("Failed transferring %s Powershell module to %s: %s" %
              (module_name_no_extension, uvm.ip(), stderr))
        succeeded = succeeded and False
    return succeeded

  @staticmethod
  def execute_command(uvm, cmd, fatal_on_error=False, username="Administrator",
                      password="nutanix/4u", timeout_secs=180):
    """
    Execute command 'cmd' on the Windows VM with the supplied credentials
    'username' and 'password' (which is needed to access the Windows VM). If
    'fatal_on_error' is true, then we fatal if the out returns a non-zero
    return value or contains anything in the stderr stream. Returns a tuple
    consisting of the return value, stdout and stderr.
    """
    remote_shell = BasicWsmanRemoteShell(uvm.ip(), username, password)
    try:
        ret, stdout, stderr = remote_shell.posh_execute(
            cmd, timeout_secs=timeout_secs)
        INFO("cmd returned %s and %s and %s" %(ret,stdout,stderr))
        stdout = stdout.strip()
        stderr = stderr.strip()
        if FLAGS.agave_verbose:
            INFO("Result of executing command %s: ret %s, stdout %s, stderr %s"
                 % (cmd, ret, stdout, stderr))
        if fatal_on_error and (ret != 0 or stderr.strip()):
            FATAL("Execution of %s on VM with IP %s failed with ret %s, "
                  "stdout %s and stderr %s"
                  % (cmd, uvm.ip(), ret, stdout, stderr))
        return ret, stdout, stderr
    except WsmanRemoteShellException as wsre:
        if FLAGS.agave_verbose:
            INFO("Unable to execute command %s due to %s" % (cmd, str(wsre)))
        if fatal_on_error:
            FATAL("Execution of %s on VM %s failed due to %s"
                  % (cmd, uvm.ip(), str(wsre)))
        return 255, '', str(wsre)


  @staticmethod
  def execute_command_as_user(windows_vm, cmd, (username, password, domain),
                              wsman_username="Administrator",
                              wsman_password="nutanix/4u"):
    """
    Executes command 'cmd' on the Windows VM with the supplied credentials
    'username' and 'password' for the domain 'domain'. 'wsman_username' and
    'wsman_password' are the credentials for accessing the Windows VM.
    """
    powershell_cmd = \
"""$password = {password} | ConvertTo-SecureString -AsPlainText -Force
$credential = New-Object System.Management.Automation.PSCredential(
    {domain}\{username}, $password)
Invoke-Command -Credential $credential -ScriptBlock {{
    $ErrorActionPreference = "Stop"
    $ConfirmPreference = "None"
    $ProgressPreference = "SilentlyContinue"
    {cmd}
}}""".format(password=password, domain=domain, username=username, cmd=cmd)
    return WindowsVMCommandUtil.execute_command(
        windows_vm, powershell_cmd, username=wsman_username,
        password=wsman_password)
