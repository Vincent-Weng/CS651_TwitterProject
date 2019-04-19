import org.apache.commons.io.IOUtils
import java.net.URL
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import java.io.File
import sqlContext.implicits._
import java.util.Date
import java.text.SimpleDateFormat
import org.joda.time.format.DateTimeFormat

case class WordCount(fileIndex:String, word:String, age:Int, gender:String, count:Int)
case class Bigram(fileIndex:String, word1:String, word2:String, count:Int)
case class TagCount(fileIndex:String, tag:String, age:Int, gender:String, count:Int)
val numFile = (z.textbox("number of files")+"").toInt


//get file number from user and list files sorted
def getListOfFiles(dir:String):List[String] = {
    val d = new File("/home/hweng/Documents/output/"+dir)
    if(d.exists && d.isDirectory){
        d.listFiles.toList.map(_.getPath).sortBy((-1)*_.split("-")(1).toLong).take(numFile).sortBy(_.split("-")(1))
    }else{
        List[String]()
    }
}


val wordCountFiles = getListOfFiles("wordCountSplit/")
val bigramFiles = getListOfFiles("bigramSplit/")
val tagCountFiles = getListOfFiles("tagCountSplit/")

//parse information in file
def getWrodCountDF(dir:String, fileIndex:String):DataFrame = {
    val dataText = sc.textFile(dir+"/part-00000")
    val data = dataText.map(l => l.split(" ") ).map(
        l => WordCount(fileIndex,l(0),l(1).toInt,l(2),l(3).toInt)
        )
    data.toDF()
}
def getTagCountDF(dir:String, fileIndex:String):DataFrame = {
    val dataText = sc.textFile(dir+"/part-00000")
    val data = dataText.map(l => l.split(" ") ).map(
        l => TagCount(fileIndex,l(0),l(1).toInt,l(2),l(3).toInt)
        )
    data.toDF()
}
def getBigramDF(dir:String, fileIndex:String):DataFrame = {
    val dataText = sc.textFile(dir+"/part-00000")
    val data = dataText.map(l => l.split(" ") ).map(
        l => Bigram(fileIndex,l(0),l(1),l(2).toInt)
        )
    data.toDF()
}

def timeToStr(epochMillis: Long): String =
  DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss").print(epochMillis).takeRight(8)
  
// save information into Dataframe and append together
 val fileIndex = timeToStr(wordCountFiles(0).split("-")(1).toLong)
var wordCountDF = getWrodCountDF(wordCountFiles(0),fileIndex)
var tagCountDF = getTagCountDF(tagCountFiles(0),fileIndex)
var bigramDF = getBigramDF(bigramFiles(0),fileIndex)

for(i <- 1 until numFile){
    val fileIndex = timeToStr(wordCountFiles(i).split("-")(1).toLong)
    wordCountDF = wordCountDF.union(getWrodCountDF(wordCountFiles(i),fileIndex))
    tagCountDF = tagCountDF.union(getTagCountDF(tagCountFiles(i),fileIndex))
    bigramDF = bigramDF.union(getBigramDF(bigramFiles(i),fileIndex))
}
wordCountDF.registerTempTable("wordCount")
tagCountDF.registerTempTable("tagCount")
bigramDF.registerTempTable("bigram")

