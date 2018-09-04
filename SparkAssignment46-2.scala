
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import scala.io.Source

object SparkAssignment46 {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g") //Configuration
    val sc=new SparkContext(conf) // Create Spark Context with Configuration
    val spark = SparkSession //Create Spark Session
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    //Problem Statement 1
    //Find out the top 5 most visited destinations.
    
    val delayed_flight =sc.textFile("DelayedFlights.csv")
    val header = delayed_flight.first()
    val delayed_flights = delayed_flight.filter(row => row != header)   //filter out header
    val mapping = delayed_flights.map(x => x.split(",")).map(x => (x(18),1)).filter(x =>
                  x._1!=null).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x =>
                  (x._2,x._1)).take(5)
    
    mapping.foreach(println)
    
    //Problem Statement 2
    //Which month has seen the most number of cancellations due to bad weather?
    
    val canceled = delayed_flights.map(x => x.split(",")).filter(x => ((x(22).equals("1"))&& (x(23).equals("B")))).
                 map(x => (x(2),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2,x._1))
                 .take(1) //Identify the column and filter our with reason for cancellation ("B" here)
                 //count the number of occurrences and identify the month with highest count
    
    canceled.foreach(println)
    
    //Problem Statement 3
    //Top ten origins with the highest AVG departure delay
    
    val avg = delayed_flights.map(x=>(x.split(",")(17),x.split(",")(16).toDouble)).
              mapValues((_,1)).reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).mapValues{ case(sum,count)=>(1.0 * sum)/count}.
              map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(10)
  
              //Identify and sum all Arrdelay amd divide by the count and get average by group by orgins
    avg.foreach(println)
    
    
    
    // Problem Statement 4
    // Which route (origin & destination) has seen the maximum diversion?
    
    val diversion = delayed_flights.map(x => x.split(",")).filter(x => ((x(24).equals("1")))).
                   map(x => ((x(17)+","+x(18)),1)).reduceByKey(_+_).map(x => (x._2,x._1)).sortByKey(false).map(x =>
                   (x._2,x._1)).take(10).foreach(println)
    
    

  }
  
}