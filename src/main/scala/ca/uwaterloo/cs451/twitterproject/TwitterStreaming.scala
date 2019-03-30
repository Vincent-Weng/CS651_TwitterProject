package ca.uwaterloo.cs451.twitterproject

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Thpffcj on 2018/1/15.
  * 使用Spark Streaming处理Kafka过来的数据
  */

class MyPartitioner(n: Int) extends Partitioner {
  override def numPartitions: Int = n

  override def getPartition(key: Any): Int = key match {
    case (leftKey: String, _) => (leftKey.hashCode() & Integer.MAX_VALUE) % numPartitions
    case leftKey: String => (leftKey.hashCode() & Integer.MAX_VALUE) % numPartitions
  }
}

object TwitterStreaming {

  def main(args: Array[String]): Unit = {


    if (args.length != 2) {
      println("Usage: TwitterStreaming <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(groupId, topicsAr) = args

    val sparkConf = new SparkConf().setAppName("TwitterStreamingApp").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    var marginal: Double = 0

    val topics = topicsAr.split(",")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val content = stream.map(record => record.value())
      .map(item => {
        val tokens = item.split("\\t")
        val Array(time, user, content) = tokens
        Array(time, user, content)
      })
      .map(array => array(2).replaceAll("""[\p{Punct}]""", "").toLowerCase.split(" "))


    /***************************************word count***********************************/
//    content.flatMap(words => {
//      words.map(word => (word, 1))
//    }).reduceByKey(_ + _)
//      .transform(rdd => rdd.sortBy(_._2))
//      .saveAsTextFiles("output_content/output")

    /***************************************biagram count***********************************/
    content.flatMap(words => {
      if (words.length > 1) {
        words.sliding(2).map(p => (p(0), p(1))).toList ++ (for (word <- words.take(words.length - 1)) yield (word, "*"))
      }
      else
        List()
    })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .map(bigram => {
        bigram._1 match {
          // (word, "*") case should come out first
          case (_: String, "*") => {
            marginal = bigram._2
            (bigram._1, bigram._2)
          }
          // (word, word) case uses the marginal of (word, "*") case
          case (_: String, _: String) => {
            (bigram._1, bigram._2 / marginal)
          }
        }
      })
      .transform(rdd => rdd.sortBy(_._2))
      .map(resultEntry => {
        ("((" + resultEntry._1._1 + ", " + resultEntry._1._2 + ")") + " " + resultEntry._2 + ")"
      })
      .repartition(1)
      .saveAsTextFiles("output_bigram/output")

    ssc.start()
    ssc.awaitTermination()
  }
}


