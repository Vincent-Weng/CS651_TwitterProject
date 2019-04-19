package ca.uwaterloo.cs451.twitterproject

import java.io.File

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.ml.feature.StopWordsRemover

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class MyPartitioner(n: Int) extends Partitioner {
  override def numPartitions: Int = n

  override def getPartition(key: Any): Int = key match {
    case (leftKey: String, _) => (leftKey.hashCode() & Integer.MAX_VALUE) % numPartitions
  }
}

object TwitterStreaming {

  implicit class Regex(sc: StringContext) {
    def reg = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  /** *************************************test print ***********************************/
  def testPrint(content: DStream[Array[String]]): Unit = {
    // test output content
    content.foreachRDD(_.take(20).foreach(line => {
      println(line.mkString(" "))
    }))
  }

  /** *************************************word count ***********************************/
  val reg_tag = """#[a-zA-Z]\w*"""
  val reg_word = """\w*[a-zA-Z]\w*"""

  def wordCountSorted(info: DStream[(Array[String],String,String)]): Unit = {
    // set the stop-words file
    val file = scala.io.Source.fromFile("english.txt")
    val stopWords = file.getLines().toArray
    file.close()
    val toLower = (s: String) => if (s != null) s.toLowerCase else s
    val lowerStopWords = stopWords.map(toLower(_)).toSet
    // remove the stop words and get word counts, save the result to the file.
    val Count = info.
      flatMap(line => {
        line._1.flatMap(word => {
          if (!lowerStopWords.contains(toLower(word)))
            List( (word, line._2, line._3) )
          else
            List()
        })
      })
      .map((_, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 0)
      .transform(rdd => rdd.sortBy(_._2, ascending = false))

    val tagCount = Count.filter(array => array._1._1 matches reg_tag)
      .transform(_.sortBy(_._2,ascending = false))
      .map(line => line._1._1 + " " + line._1._2 + " "+ line._1._3 +" "+line._2)
      .repartition(1)

    val wordCount = Count.filter(array => array._1._1 matches reg_word)
      .transform(_.sortBy(_._2,ascending = false))
      .map(line => line._1._1 + " " + line._1._2 + " "+ line._1._3 +" "+line._2)
      .repartition(1)

    tagCount.saveAsTextFiles("output/tagCountSplit/")
    tagCount.foreachRDD(rdd => rdd.saveAsTextFile("output/tagCount/"))

    wordCount.saveAsTextFiles("output/wordCountSplit/")
    wordCount.foreachRDD(rdd => rdd.saveAsTextFile("output/wordCount/"))
  }


  /** *************************************bigram ***********************************/
  def bigram(info: DStream[(Array[String],String,String)]): Unit = {
    val bigram = info.flatMap(line => {
      if (line._1.length > 1)
         line._1.sliding(2).map(p => (p(0), p(1)))
      else
        List()
    })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .filter(_._2 > 0)
      .transform(rdd => rdd.sortBy(_._1._1))
      .map(line => line._1._1 + " " + line._1._2 + " " + line._2)
      .repartition(1)
    bigram.saveAsTextFiles("output/bigramSplit/")
    bigram.foreachRDD(rdd => rdd.saveAsTextFile("output/bigram/"))
  }

  /** *************************************main ********************************************/
  def main(args: Array[String]): Unit = {


    if (args.length != 2) {
      println("Usage: TwitterStreaming <group> <topics>")
      System.exit(1)
    }

    val Array(groupId, topicsAr) = args

    val sparkConf = new SparkConf().setAppName("TwitterStreamingApp").setMaster("local[5]")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false")


    // can't be set Seconds(1), Why?
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topics = topicsAr.split(",")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "spark.streaming.kafka.consumer.cache.enabled" -> (false: java.lang.Boolean),
      "spark.files.overwrite" -> (true: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // content is a DStreaming
    val info = stream
      .map(record => record.value())
      .map(_.split("\\t")) //time, user, content, age, gender
      .map {case Array(a1, a2, a3, a4, a5) => (a1, a2, a3, a4, a5)}
      // filter all words left
      .map(line => {
      val age = line._4
      val gender = line._5
      val content = line._3.replaceAll("""[\p{Punct}&&[^#]]""", "")
        .replaceAll("""\s+""", " ").toLowerCase.split(" ")
      (content, age, gender)
    })
      .map(line => (line._1.filter(_.matches("""\w*#*[a-zA-Z]\w*""")),line._2,line._3))
      .window(Seconds(60), Seconds(5))


    /** *************************************word count ***********************************/
    wordCountSorted(info)


    /** *************************************bigram *****************************/
    bigram(info)


    ssc.checkpoint("checkout")
    ssc.start()
    ssc.awaitTermination()
  }
}
