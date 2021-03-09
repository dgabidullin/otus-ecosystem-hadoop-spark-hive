import org.apache.hadoop.fs.{FileStatus, Path, _}
import org.apache.hadoop.conf._

import java.io.BufferedInputStream
import java.net.URI

object App {
  val ODS_PATH = "/ods"
  val STAGE_PATH = "/stage"
  val defaultFS = "hdfs://namenode:9000"
  val conf = new Configuration()
  val fs: FileSystem = FileSystem.get(new URI(defaultFS), conf)

  def main(args: Array[String]) {
    makeFolder(ODS_PATH)
    fs.listStatus(new Path(STAGE_PATH)).foreach {
      stageFolder => syncOds(stageFolder)
    }
  }

  def syncOds(stageFolder: FileStatus): Unit = {
    val readyFiles = fs.listStatus(stageFolder.getPath).filter(p => !p.getPath.getName.contains(".inprogress"))
    if (readyFiles.length > 0) {
      val fileName = readyFiles.head.getPath.getName
      val outputFilePath = s"$ODS_PATH/${stageFolder.getPath.getName}/$fileName"
      makeFile(outputFilePath)
      val outFile = fs.append(new Path(outputFilePath))
      for (f <- readyFiles) {
        mergeFiles(f.getPath, outFile)
      }
      println(s"folder processed successfully ${stageFolder.getPath}")
    } else {
      println(s"not find ready files for processing ods, skipped folder ${stageFolder.getPath}")
    }
  }

  def mergeFiles(filepath: Path, out: FSDataOutputStream): Unit = {
    val b = new Array[Byte](1024)
    val in = new BufferedInputStream(fs.open(filepath))
    try {
      var numBytes = in.read(b)
      while (numBytes > 0) {
        out.write(b, 0, numBytes)
        numBytes = in.read(b)
      }
    } finally {
      in.close()
    }
  }

  def makeFolder(folderName: String): Unit = {
    val path = new Path(folderName)
    if (fs.exists(path)) {
      println(s"already exists folder $folderName")
    } else {
      fs.mkdirs(path)
      println(s"created new folder $folderName")
    }
  }

  def makeFile(fileName: String): Unit = {
    val path = new Path(fileName)
    if (fs.exists(path)) {
      println(s"already exists file $fileName")
    } else {
      fs.createNewFile(path)
      println(s"created new file $fileName")
    }
  }
}
