import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import scala.io.Source

object SparkAssignment461 {
def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g") //Configuration
    val sc=new SparkContext(conf) // Create Spark Context with Configuration
    val spark = SparkSession //Create Spark Session
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()

        
    val tweets = spark.read.json("tweet.JSON")
    tweets.registerTempTable("tweets")
    println("registeration complete")
    tweets.show()
    println(tweets)
    val hashtags = spark.sql("select id as id,entities.hashtags.text as words from tweets").registerTempTable("hashtags")
    val hashtag_word = spark.sql("select id as id,hashtag from hashtags LATERAL VIEW explode(words) w as hashtag").registerTempTable("hashtag_word")
    val popular_hashtags = spark.sql("select hashtag, count(hashtag) as cnt from hashtag_word group by hashtag order by cnt desc").show
    println("Success")
}

}
