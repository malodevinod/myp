package rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext 
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD

object StreamingFromFiles {
  
   def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: HdfsDirPatht <directory>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Hdfsread")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.textFileStream(args(0))

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    lines.print()
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  } 

}