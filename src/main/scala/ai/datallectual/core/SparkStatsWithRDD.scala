package ai.datallectual.core

/**
  * Created by datallectual on 8/6/17.
  */
import ai.datallectual.core.SimpleApp.Flight
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.joda.time._
import org.joda.time.format._
import org.joda.time.LocalTime
import org.joda.time.LocalDate
import ai.datallectual._

object SparkStatsWithRDD {

  def main(args: Array[String]): Unit ={
    println("Spark Analytics using key pair rdd")

    val sparkConf = new SparkConf().setAppName("Spark Analytics").setMaster("local")
    val sc = new SparkContext(sparkConf)

    println("Spark Context" + sc )


    val airlinesPath="airlines.csv"
    val airportsPath="airports.csv"
    val flightsPath= "flights.csv"


    println("Following analytics generated in this code")
    println(" 1. Compute the average delays per airport")
    println(" 2. Compute top 10 airports based on delays")

    // Load one dataset
    val airlines=sc.textFile(airlinesPath)
    airlines.filter(SimpleApp.noHeader).take(10).foreach(println)

    val airlineNoHeader = airlines.filter(SimpleApp.noHeader)

    println("Printing data from flights file ")
    val flights=sc.textFile(flightsPath)
    flights.take(5).foreach(println)

    //Parse the flight data
    println("Printing parsed flight data ")
    val parsedFlights = flights.map(SimpleApp.parse)
    parsedFlights.take(10).foreach(println)

    println("Create a pair RDD with origin as a key ")
    val airportDelays = parsedFlights.map( x => (x.origin,x.dep_delay))

    println("Print few keys and values")
    airportDelays.keys.take(10).foreach(print)
    airportDelays.values.take(10).foreach(print)


    /*
    Compute the sum of all delays for the key in the pair RDD
    Key ==> Origin airport, remember reduceByKey is transformation
    while reduce is action
     */


    val airportTotalDelayRDD = airportDelays.reduceByKey((x,y) => x+y)

    println("Create a pair RDD to calculate the count of flightd by airports")

    val airportsDelayedFligtsRDD = airportDelays.mapValues( x => 1).reduceByKey( (x,y) => x+y)

    /*
    Now to calculate Aveage delpay per airport we have to use join operation on above
    two pair RDDs airportTotalDelayRDD,airportsDelayedFligtsRDD
    1) airportTotalDelayRDD => key is airport value is total delays
    2) airportsDelayedFligtsRDD ==> key is airport value is total fligts
    To do : create a Join(inner join) between these two rdds this opera tion
    will create new rdd with key as airport value is tuple of total delays and total flight
     */

    val airportsDelaysAndCounts = airportTotalDelayRDD.join(airportsDelayedFligtsRDD)

    println("Calculate the average delays ")

    val airportAvgDelays = airportsDelaysAndCounts.mapValues( x => x._1/x._2.toDouble)

    //Use interpolation to print 10 records airport and delays
    airportAvgDelays.take(10).foreach( x =>{ println(f"${x._1}%s has average delay of ${x._2}%f") })

    /*
    Now use combineByKey to achieve same results\
    There are 3 parts to combineByKey fucntion
    1. CreateCombiner
    2. merge function
    3.Shuffle is performed
    4.merge combiners
     */

    
  }


}
