package com.sangjinshin.cs378.assign11

import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import scala.util.control.Breaks._
import scala.util.Random

/**
 * User Sessions using Spark
 *
 * tsv data => ((userId, city), [list of Events])
 *
 * @author Sangjin Shin
 * UTEID: ss62273
 * Email: sangjinshin@outlook.com
 */

object UserSessions {
  def main(args: Array[String]) {

    /* Create a SparkContext */
    val sparkConf = new SparkConf().setMaster("local").setAppName("User Sessions")
    val sc = new SparkContext(sparkConf)

    /* Input and output path */
    val inputDir = args(0)
    val inputDir2 = args(1)
    val outputDir = args(2)

    // Create an inputRDD to read input file through Spark context
    val inputRDD = sc.textFile(inputDir)
      .union(sc.textFile(inputDir2))

    // Accumulators
    val numEventsUnfiltered = sc.longAccumulator("Total number of events before session filtering")
    val numEventsFiltered = sc.longAccumulator("Total number of events after session filtering")
    val numSessions = sc.longAccumulator("Total number of sessions")
    val numSessionsTypeShower = sc.longAccumulator("Total number of sessions of type SHOWER")
    val numSessionsTypeShowerFiltered = sc.longAccumulator("Total number of sessions of type SHOWER that were filtered out")

    // User session RDD
    val session = inputRDD
      .map { // Build K,V pair
        line =>
          val userId = line.split("\\t")(0)
          val eventType = line.split("\\t")(1).split(" ")(0)
          val eventSubtype = line.split("\\t")(1).split(" ", 2)(1)
          val eventTimestamp = line.split("\\t")(2)
          val city = line.split("\\t")(3)
          val vin = line.split("\\t")(4)
          val event: Event = new Event(eventType, eventSubtype, eventTimestamp, vin)
          ((userId, city), List(event))
      }
      .partitionBy(new SessionCityPartitioner(6)) // Partition user sessions by city using a custom partitioner; 6 partitions
      .reduceByKey(_.union(_).distinct) // Combine events by key (userId, city) tuple and remove duplicates
      .filter { // Sample SHOWER sessions at a rate of 1 in 10; keep all non-SHOWER sessions
        case ((userId, city), _eventList) =>
          for (event <- _eventList) {
            numEventsUnfiltered.add(1)
          }
          var isShower = false
          breakable {
            for (event <- _eventList) {
              if (event.getEventSubtype.equalsIgnoreCase("contact form") || event.getEventType.equalsIgnoreCase("click")) {
                isShower = false
                break
              }
              if (event.getEventType.equalsIgnoreCase("show") || event.getEventType.equalsIgnoreCase("display")) {
                isShower = true
              }
            }
          }
          val rand = Random
          val f = rand.nextFloat
          if (isShower) {
            numSessionsTypeShower.add(1)
            if (f > 0.1) { // 9 in 10 SHOWER events filtered out
              numSessionsTypeShowerFiltered.add(1)
            }
          }
          (!isShower) || (isShower && (f <= 0.1))
      }
      .sortBy { // Order sessions by userId, then by city
        case ((userId, city), _eventList) =>
          (userId, city)
      }
      .persist(StorageLevel.MEMORY_ONLY) // Persist the data to avoid computing RDD multiple times
      .collect {
        case ((userId, city), _eventList) =>
          numSessions.add(1)
          for (event <- _eventList) {
            numEventsFiltered.add(1)
          }
          ((userId, city), _eventList.sortBy { // Order list of events by eventTimestamp, then eventType, then eventSubtype, then VIN
            (_eventList) => (_eventList.getEventTimestamp, _eventList.getEventType, _eventList.getEventSubtype, _eventList.getVin)
          }
            .mkString("[", ", ", "]"))
      }
      .saveAsTextFile(outputDir)

    // Clear RDD stored in memory
    inputRDD.unpersist()

    // Shut down Spark Context
    sc.stop()

    // Print Accumulator counts
    println("------------------------- Accumulator Counts -------------------------")
    println(numEventsUnfiltered.name + ": " + numEventsUnfiltered.value)
    println(numEventsFiltered.name + ": " + numEventsFiltered.value)
    println(numSessions.name + ": " + numSessions.value)
    println(numSessionsTypeShower.name + ": " + numSessionsTypeShower.value)
    println(numSessionsTypeShowerFiltered.name + ": " + numSessionsTypeShowerFiltered.value)
    println("----------------------------------------------------------------------")

  }

  /**
   * Custom Partitioner to partition user sessions by city
   */
  private class SessionCityPartitioner(numParts: Int) extends Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = {
      val city = key.asInstanceOf[Product].productElement(1)
      val code = (city.hashCode() % numPartitions)
      if (code < 0) {
        code + numPartitions // Make it non-negative
      } else {
        code
      }
    }

    // Java equals method to let Spark compare Partitioner objects
    override def equals(other: Any): Boolean = other match {
      case scp: SessionCityPartitioner =>
        scp.numPartitions == numPartitions
      case _ =>
        false
    }
  }

}