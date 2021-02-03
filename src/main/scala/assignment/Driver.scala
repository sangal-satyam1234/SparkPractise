package assignment

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.kohsuke.args4j.{CmdLineParser, Option}

import java.io.File
import java.net.URL
import scala.collection.JavaConverters.asJavaCollection
import scala.util.Try

object Driver {

  lazy val sparkConfig = new SparkConf()
    .setAppName("test")
    .setMaster("local[4]")
    .set("spark.driver.host", "localhost")
    .set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    .set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  lazy val spark: SparkSession = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  @Option(name = "-q", aliases = Array("--question"), usage = "1 for q1 and 2 for q2", required = true)
  var question: Int = -1
  @Option(name = "-f", aliases = Array("--file"), usage = "Full path and name of input file", required = true)
  var inputFile: String = ""

  def main(args: Array[String]): Unit = {
    val cmdLineParser = new CmdLineParser(this)
    Try(cmdLineParser.parseArgument(asJavaCollection(args))).getOrElse {
      cmdLineParser.printUsage(System.out)
      System.exit(-1)
    }
    spark.sparkContext.setLogLevel("OFF")
    val inputURL = new File(inputFile).toURI.toURL
    process(inputURL, question)
    spark.close()
  }

  private def process(url: URL, choice: Int) = {
    if (choice == 1) Question1.process(url)
    else if (choice == 2) Question2.process(url)
    else {
      println("Wrong input for question choice")
      System.exit(-1)
    }
  }
}
