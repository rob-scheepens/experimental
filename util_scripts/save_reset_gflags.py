
  def __save_gflags(self, svm):
    gflag_files = svm.execute("ls /home/nutanix/config/*.gflags")[
      "stdout"].strip().split()
    gflag_file_map = {}
    for gflag_file in gflag_files:
      component_filename = gflag_file.split("/").pop()
      component_flags = svm.execute("cat %s" % gflag_file)[
        "stdout"].strip().split()
      gflag_file_map[component_filename] = component_flags

    return gflag_file_map

  def __set_gflags(self, svm, gflag_file_map):
    for component_filename, gflags in gflag_file_map.iteritems():
      filename = "/home/nutanix/config/%s" % component_filename
      svm.execute("rm -rf %s" % filename)
      for gflag in gflags:
        svm.execute("echo %s >> %s" % (gflag, component_filename))

    # Restart the components for which the gflags were set.
    for component_filename, gflags in gflag_file_map.iteritems():
      component = component_filename.split(".")[0]
      svm

    return True
