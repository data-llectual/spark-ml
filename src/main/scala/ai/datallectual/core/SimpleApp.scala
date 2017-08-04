package ai.datallectual.core

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.joda.time._
import org.joda.time.format._
import org.joda.time.LocalTime
import org.joda.time.LocalDate


/**
  * Created by user on 8/2/17.--
  */
object SimpleApp {
  def main(args: Array[String]): Unit ={
    println("Here -- there")
    val sparkConf = new SparkConf().setAppName("Spark Analytics").setMaster("local")
    val sc = new SparkContext(sparkConf)

    println("sc1" + sc )


    val airlinesPath="airlines.csv"
    val airportsPath="airports.csv"
    val flightsPath= "flights.csv"

    // Load one dataset
    val airlines=sc.textFile(airlinesPath)
    println("airlines" + airlines)
    airlines.first().foreach(println(_))

    airlines.filter(noHeader).take(10).foreach(println)

    val airlineNoHeader = airlines.filter(noHeader)
    //airlineNoHeader.foreach(println)


    //Try understand this --
    println("Printing data with map function") ;
    val airlineMapData = airlineNoHeader.map(_.replace("\"",""))
    airlineMapData.take(10).foreach(println)

    //Now split the record where thre is comma
    println("Going to split the record for comma")

    val airlinesSplitCommaData = airlineMapData.map(_.split(','))
    airlinesSplitCommaData.take(10).foreach(x => println(x(0), x(1)))

    //Now convert the id from String to Int

    println("printing the Id as Int and Data as String")
    val airlinesConvertToInt = airlinesSplitCommaData.map(data => (data(0).toInt,data(1)))
    airlinesConvertToInt.take(10).foreach(println)

    val airLinesWithIntData = airlineNoHeader.map(_.replace("\"","")).map(_.split(',')).map(x => ( x(0).toInt,x(1)) )
    airLinesWithIntData.take(10).foreach(println)

    println("Printing data from flights file ")
    val flights=sc.textFile(flightsPath)
    flights.take(5).foreach(println)

    //Parse the flight data
    println("Printing parsed flight data ")
    val parsedFlights = flights.map(parse)
    parsedFlights.take(10).foreach(println)

    println("Calculate total distance ")
    val totalDistance = parsedFlights.map(_.distance).reduce((x,y) => x+y)
    println("Total distance travelled ", totalDistance)

    println("Average distance travelled ", totalDistance/parsedFlights.count())

    println("Total # of flights with delays",parsedFlights.filter(_.dep_delay>0).count())

    println("% flights delayed ",parsedFlights.filter(_.dep_delay>0).count().toDouble/parsedFlights.count().toDouble)

    val result = List(1,20,30,40,50,60,70,80,90,100).par.aggregate((0,0))(
      (x,y) => (x._1+y,x._2+1),
      (x,y) => (x._1+y._1,x._2+y._2)
    )

    println("The tuple returned from aggregation is ", result._1 + "," + result._2)


    val sumCount=parsedFlights.map(_.dep_delay)

    val delayedDeparture = parsedFlights.map(_.dep_delay).aggregate((0.0,0))(
      (x,y) => (x._1+y, x._2+1),
      (x,y) => (x._1+y._1, x._2+y._2)
    )


    println("Tuple consisting of aggregation of dep_delay and count ", delayedDeparture._1 + "," + delayedDeparture._2)
    println("Average delayed departure in US ", delayedDeparture._1/delayedDeparture._2)

    //Let us do some airport statistics
    println("Create a map of origin and delay")
    val delayAtAirports = parsedFlights.map(flightData => (flightData.origin,flightData.dep_delay))

    val totalDelayAtAirport = delayAtAirports.reduceByKey((x,y) => x+y)
    println("Few dealys at various airport")
    totalDelayAtAirport.take(10).foreach(x => println(x._1  + ","+ x._2))


  }

  def noHeader(row: String) : Boolean ={
    !row.contains("Description")
  }

  case class Flight(date: LocalDate,
                    airline: String ,
                    flightnum: String,
                    origin: String ,
                    dest: String ,
                    dep: LocalTime,
                    dep_delay: Double,
                    arv: LocalTime,
                    arv_delay: Double ,
                    airtime: Double ,
                    distance: Double
                   )



  def parse(row: String): Flight = {

    val fields = row.split(",")
    val datePattern = DateTimeFormat.forPattern("YYYY-mm-dd")
    val timePattern = DateTimeFormat.forPattern("HHmm")

    val date: LocalDate = datePattern.parseDateTime(fields(0)).toLocalDate()
    val airline: String = fields(1)
    val flightnum: String = fields(2)
    val origin: String  = fields(3)
    val dest: String   = fields(4)
    val dep: LocalTime = timePattern.parseDateTime(fields(5)).toLocalTime()
    val dep_delay: Double = fields(6).toDouble
    val arv: LocalTime = timePattern.parseDateTime(fields(7)).toLocalTime()
    val arv_delay: Double = fields(8).toDouble
    val airtime: Double = fields(9).toDouble
    val distance: Double = fields(10).toDouble

    Flight(date,airline,flightnum,origin,dest,dep,dep_delay,arv,arv_delay,airtime,distance)



  }
}
