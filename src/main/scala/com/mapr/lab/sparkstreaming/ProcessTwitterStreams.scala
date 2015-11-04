package com.mapr.lab.sparkstreaming

import java.util.Calendar

import com.mapr.lab.sparkstreaming.utils.TwitterHelper
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by loaner on 10/27/15.
 */
object ProcessTwitterStreams {

  val hbaseTableName = "/labapps/maprtwitterexample/hashtagstable"
  val htCFColumnName = "htcf"
  val htCNamePrefix = "htc:"
  val htCName = "htcol";
  val htcountCName = "htcountcol"

  case class HashtagData(hashtag: String, count: Int)

  def convertToPut(hastagdata: HashtagData): (ImmutableBytesWritable, Put) = {
    // create a composite row key: with salt and time stamp
    val cal = Calendar.getInstance()
    val timestamp = cal.get(Calendar.YEAR) + "" + cal.get(Calendar.MONTH) + "" + cal.get(Calendar.DAY_OF_MONTH) + "" +
      cal.get(Calendar.HOUR_OF_DAY) + "" + cal.get(Calendar.MINUTE) + "" + cal.get(Calendar.SECOND) + "" + cal.get(Calendar.MILLISECOND);
    val numb_region_server = 4;
    val salt = new Integer(timestamp.hashCode()).shortValue() % numb_region_server
    val rowkey = Bytes.toBytes(salt+"|"+timestamp)


    val put = new Put(rowkey)
    // add to column family data, column  data values to put object
    //put.add(Bytes.toBytes(htCFColumnName), Bytes.toBytes(htCNamePrefix+hastagdata.hashtag), Bytes.toBytes(hastagdata.hashtag +"="+ hastagdata.count))
    put.add(Bytes.toBytes(htCFColumnName), Bytes.toBytes(htCName), Bytes.toBytes(hastagdata.hashtag))
    put.add(Bytes.toBytes(htCFColumnName), Bytes.toBytes(htcountCName), Bytes.toBytes(hastagdata.count))

    return (new ImmutableBytesWritable(rowkey), put)
  }

  def convertToPut(hastagdata: HashtagData, rowkeystring : String): (ImmutableBytesWritable, Put) = {
    val rowkey = Bytes.toBytes(rowkeystring)
    val put = new Put(rowkey)
    put.add(Bytes.toBytes(htCFColumnName), Bytes.toBytes(htCNamePrefix+hastagdata.hashtag), Bytes.toBytes(hastagdata.hashtag +"="+ hastagdata.count))
    return (new ImmutableBytesWritable(rowkey), put)
  }

  def main(args: Array[String]): Unit = {

    //println(args(0))

    TwitterHelper.configureTwitterCredentials()

    val sparkConf = new SparkConf().setAppName("maprtwitterapp");
    val sc = new SparkContext(sparkConf);

    //Creating a streaming context to retrieve data every second
    val ssc = new StreamingContext(sc, Seconds(1))

    //Getting a continuous stream of RDD's which are tweets
    val tweets = TwitterUtils.createStream(ssc, None, args)

    tweets.foreachRDD(statusRDD => {
      statusRDD.foreach(status => {
        println(status.getCreatedAt)
        println(status.getId)
        println(status.getCurrentUserRetweetId)
        println(status.getUser.getName)
        println(status.getUser.getScreenName)
        println(status.getText)
      })
    })

    val statuses = tweets.map(status => status.getText)
    val words = statuses.flatMap(status => status.split(" "))
    val hashtags = words.filter(word => word.startsWith("#"))

    hashtags.print()

    //ssc.checkpoint("/apps/spark/checkpointdir")
    //val hashtagcounts = hashtags.map(tag => (tag, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(60 * 5), Seconds(1))

    val hashtagcounts = hashtags.map(tag => (tag, 1)).reduceByKey(_+_)

    hashtagcounts.print()

    val hashTagDataRDD = hashtagcounts.map(hashtagcount => HashtagData(hashtagcount._1.toString, hashtagcount._2.toInt))

    hashTagDataRDD.foreachRDD { rdd =>

      val hconf = HBaseConfiguration.create()
      hconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

      val jobConfig: JobConf = new JobConf(hconf, this.getClass)
      jobConfig.setOutputFormat(classOf[TableOutputFormat])
      jobConfig.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

      // convert hash tag data to put object and write to HBase table column family data
      rdd.map(convertToPut).saveAsHadoopDataset(jobConfig) //NOTE this line adds a new row for every tag in a tweet
      //rdd.map(rddData => convertToPut(rddData, rowkeyString)).saveAsHadoopDataset(jobConfig)
    }

    hashTagDataRDD.count()

    ssc.start()
    ssc.awaitTermination()
  }
}
