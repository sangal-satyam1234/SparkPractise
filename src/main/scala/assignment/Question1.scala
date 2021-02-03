package assignment

import assignment.Driver.spark
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions.{col, when}

import java.net.URL

object Question1 {

  def process(url: URL) = {
    val df = spark.read.json(url.getFile)
    println("First 10 entries")
    df.show(10)
    val df2 = df.withColumn("r2", when(col("l1") >= 0, Literal("positive")).otherwise(Literal("negative")))
    println("First 10 transformations")
    df2.show(10)
  }
}


