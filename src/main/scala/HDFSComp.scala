import java.io._
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.xerial.snappy.SnappyCodec
import org.xerial.snappy.SnappyInputStream
import org.xerial.snappy.SnappyOutputStream

object HDFSComp {
  val buffer = Array.ofDim[Byte](64 * 1024)

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Invalid Arguments... <-c | -d> <src> <dest> [OPTIONS] \n\n [Options] \t bzip2 [default], snap and gzip")
      System.exit(0)
    }
    Do(args)
  }

  def Do(args: Array[String]) = {
    val Array(arg, input, output, comp) = args.length match {
      case 3 => args.take(3) ++ Array("bzip2")
      case 4 => args.take(4)
    }

    val f: (InputStream, OutputStream, String) => Unit = (comp, arg) match {
      case (_, "-c") => Compress
      case (_, "-d") => Decompress
      case _ =>
        System.err.println("Invalid Option...\n-c to compress and -d to decompress")
        System.err.println("Invalid Option...\nsnap for snappy \ngzip for GZip \nbzip2 for BZip2")
        System.exit(0); null
    }

    val ext = comp match {
      case "bzip2" => ".bz2"
      case "snap" => ".snap"
      case "gzip" => ".gz"
      case _ =>
        System.err.println("Invalid Option...\n-c to compress and -d to decompress")
        System.err.println("Invalid Option...\nsnap for snappy \ngzip for GZip \nbzip2 for BZip2")
        System.exit(0); null
    }

    val fs = FileSystem.get(new Configuration())
    if (fs.isDirectory(new Path(input))) {
      val sourceList = fs.listFiles(new Path(input), false)
      while(sourceList.hasNext) {
        val tempPath = sourceList.next.getPath
        val outPath = arg match {
          case "-c" => new Path(output + "/" + tempPath.getName + ext)
          case "-d" => new Path(output + "/" + tempPath.getName.substring(0, tempPath.getName.lastIndexOf(".")))
        }
        f(fs.open(tempPath), fs.create(outPath), comp)
        println(outPath.getParent + "/" + outPath.getName)
        }
      }
    else {
      val outPath = new Path(output + ext)
      f(fs.open(new Path(input)), fs.create(outPath), comp)
      println(outPath.getParent + "/" + outPath.getName)
    }
  }

  def Decompress(is: InputStream, output: OutputStream, comp: String) {
    try {
      val sis: InputStream = comp match {
        case "bzip2" => new BZip2CompressorInputStream(is)
        case "snap" => new SnappyInputStream(is)
        case "gzip" => new GZIPInputStream(is)
        case _ => println("Input is not in " + comp + " format. Please check."); System.exit(0); null
      }

      val os = new BufferedOutputStream(output)
      Stream.continually(sis.read(buffer)).takeWhile(_ > 0).foreach(os.write(buffer, 0, _))
      sis.close()
      os.close()
    } catch {
      case e: Exception => println("Input is not in " + comp + " format. Please check."); System.exit(0)
    }
  }

  def Compress(input: InputStream, output: OutputStream, comp: String) {
    try {
      val outStream: OutputStream = comp match {
        case "bzip2" => new BZip2CompressorOutputStream(output)
        case "snap" => new SnappyOutputStream(output)
        case "gzip" => new GZIPOutputStream(output)
        case _ => println("Input is not in " + comp + " format. Please check."); System.exit(0); null
      }

      val inStream = new BufferedInputStream(input)
      Stream.continually(inStream.read(buffer)).takeWhile(_ > 0).foreach(outStream.write(buffer, 0, _))
      inStream.close()
      outStream.close()
    } catch {
      case e: Exception => println("Input is not in " + comp + " format. Please check."); System.exit(0)
    }
  }
}
