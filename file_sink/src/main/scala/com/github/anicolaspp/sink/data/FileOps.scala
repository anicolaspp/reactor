package com.github.anicolaspp.sink.data

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FileOps {
  private lazy val fs: FileSystem = {
    val conf = new Configuration()

    println(s"File System Configuration: $conf")

    FileSystem.get(conf)
  }

  def writeAsString(content: String, hdfsPath: String) {
    val path: Path = new Path(hdfsPath)

    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    val dataOutputStream = fs.create(path)
    val bw = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))

    bw.write(content)

    bw.close()
  }
}
