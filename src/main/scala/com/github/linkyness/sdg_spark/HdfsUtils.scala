package com.github.linkyness.sdg_spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStreamReader
import java.net.URI

class HdfsUtils(hdfsURI: String) {
  def openConfigFileFromHDFS(hdfsPath: String): InputStreamReader = {
    val hdfs: FileSystem = FileSystem.get(new URI(hdfsURI), new Configuration())
    new InputStreamReader(hdfs.open(new Path(hdfsPath)))
  }
  def getHdfsPath(path: String): String = hdfsURI + path
}
