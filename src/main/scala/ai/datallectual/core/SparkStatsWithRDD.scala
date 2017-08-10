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
    1) airportTotalDelayRDD => key is airport and value is total delays
    2) airportsDelayedFligtsRDD ==> key is airport and value is total flights
    To do : create a Join(inner join) between these two rdds this operation
    will create new rdd with key as airport and value is a tuple of total delays and total flights
     */

    val airportsDelaysAndCounts = airportTotalDelayRDD.join(airportsDelayedFligtsRDD)

    println("Calculate the average delays ")

    val airportAvgDelays = airportsDelaysAndCounts.mapValues( x => x._1/x._2.toDouble)

    //Use interpolation to print 10 records airport and delays
    airportAvgDelays.take(10).foreach( x =>{ println(f"${x._1}%s has average delay of ${x._2}%f") })

    //Top 10 airports with highest average delays
    //sort the values in airportAvgDelays RDD  - remember sort default is ascending sort

    println("Top 10 Average delays by airports")
    airportAvgDelays.sortBy(-_._2).take(10).foreach(record =>  { println(f"${record._1}%s has average delay of ${record._2}%f")})

    /*
    Now use combineByKey to achieve same results\
    There are 3 parts to combineByKey function
    1. CreateCombiner
    2. merge function
    3.Shuffle is performed
    4.merge combiners

    Create Combiner is a default function that gets applied when spark encounters key for the first time
    Merge function is used to combine values with the same key in given partition (No cross partition action at this time)
    Shuffle function is used to bring same keys to single partition.
    Merge combiners will combine the values with same key
     */

    val avgFlightSumAndCount = airportAvgDelays.combineByKey(
                                                  /*Create Combiner */
                                                  value => (value,1),
                                                  /*Merge function happens on same partition*/
                                                  (record :(Double,Int), value) => (record._1+value,record._2+1),
                                                  /* Shuffle operation to bring keys in same partition*/
                                                  /* Merge combiner*/
                                                  (record1 :(Double,Int), record2 :(Double,Int)) => (record1._1 + record2._1 , record1._2+record2._2))


    println("Airport with average flight delays")

    val airportAvgDelaysCombineBy = avgFlightSumAndCount.mapValues(x=> x._1/x._2)
    airportAvgDelaysCombineBy.sortBy(-_._2).take(10).foreach(record =>  { println(f"${record._1}%s has average delay of ${record._2}%f")})


    val airportsPairRDD = sc.textFile(airportsPath).filter(SimpleApp.noHeader).map(parseLookup)

    airportsPairRDD.take(10).foreach( x => { println(f" ${x._1}%s and description is ${x._2}%s") })

    /*
    Now that we have airportPairRDD - we can lookup airportPairRDD to get the description of the airport
    This can be done just as using a map operation - CollectAsMap -closure scala concepts used here.
    Spark also provides broadcast variables
     */

    val airportLookupRDD=airportsPairRDD.collectAsMap

    println("Description of CLI is ",  airportLookupRDD("CLI"))

    /*
    Broadcast airportLookupRDD to all nodes - so that lookup operation becomes faster
     */
    val airportBCRDD=sc.broadcast(airportLookupRDD)

    /*
    Now  get top 10 airports with highest delays
     */

    airportAvgDelays.map(x => (airportBCRDD.value(x._1),x._2))
                                  .sortBy(-_._2).take(10)
                                  .foreach(record =>  { println(f"${record._1}%s has average delay of ${record._2}%f")})


  }

  def parseLookup(row: String) : (String,String) = {
    val x = row.replace("\"","").split(",")
    (x(0),x(1))
  }


}
