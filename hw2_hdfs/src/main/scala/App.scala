import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.io.{BufferedInputStream, File, FileInputStream}
import java.net.URI

object App {

  def main(args: Array[String]) {
    val outPath = "/ods"
    val conf = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://192.168.99.100:9000"), conf)
    fs.listStatus(new Path("/stage")).foreach {
      f =>
        fs.listStatus(f.getPath).foreach(p => {
          val out: FSDataOutputStream = fs.create(new Path("/ods" + "/part-0000.csv"))
          val b = new Array[Byte](1024)
          val in = fs.open(p.getPath)
          try {
            var numBytes = in.read(b)
            while (numBytes > 0) {
              out.write(b, 0, numBytes)
              numBytes = in.read(b)
            }
          } finally {
            in.close()
          }
        })
    }
  }

  def isFolderExists(folderPath: Path, fs: FileSystem): Boolean = {
    fs.exists(folderPath)
  }

  def makeFolder(folderPath: Path, fs: FileSystem): Unit = {
    println(s"created new folder $folderPath")
    fs.mkdirs(folderPath)
  }

  def removeFile(file: Path, fs: FileSystem): Boolean = {
    println(s"removed folder $file")
    fs.delete(file, true)
  }

  def appendFile(filepath: Path, out: FSDataOutputStream, fs: FileSystem): Unit = {
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
}

case class WorkPath(from: FileStatus, to: Path)
