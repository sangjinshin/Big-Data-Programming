package com.sangjinshin.cs378.assign10

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

/**
 * Spark application that creates an Inverted Index of verse reference
 *
 * "VerseId VerseText" => (Word, [list of referenced VerseId])
 *
 * @author Sangjin Shin
 * UTEID: ss62273
 * Email: sangjinshin@outlook.com
 */
object VerseInvertedIndex {
  def main(args: Array[String]) {

    /* Create a SparkContext */
    val sparkConf = new SparkConf().setMaster("local").setAppName("Verse Inverted Index")
    val sc = new SparkContext(sparkConf)

    /* Input and output path */
    val inputDir = args(0)
    val outputDir = args(1)
    val outputDir2 = args(2)

    // Create an inputRDD to read input file through Spark context
    val inputRDD = sc.textFile(inputDir)

    /* Inverted Index sorted by word */
    val invertedIndex = inputRDD
      .map(line => (line.split(" ")(0), line)) // Split verse ID and verse text
      .flatMap { // Lowercase verse, remove punctuations, and split each word
        case (id, verse) =>
          verse.toLowerCase().split("[\\d\\p{Punct}\\s]+") map {
            word => (word, List(id)) // Transform word to word and verse ID
          }
      }
      .persist(StorageLevel.DISK_ONLY) // Persist the data to avoid computing RDD multiple times
      .reduceByKey(_.union(_)) // Combine all verse ID's with same key (word)
      .sortByKey() // Sort index by key (word)
      .collect { // Remove duplicates, sort the List of verse ID's in lexicographical order
        case (word, _idList) =>
          (word, _idList.distinct.sortBy {
            (_idList) => (_idList.split(":")(0), _idList.split(":")(1).toInt, _idList.split(":")(2).toInt)
          }
            .toArray.mkString("[", ", ", "]")) // Format idList as [id, id, ..., id]
      }
      .saveAsTextFile(outputDir)

    /* Inverted Index sorted by descending number of references, then by word */
    val indexSortedByNumRef = inputRDD
      .map(line => (line.split(" ")(0), line)) // Split verse ID and verse text
      .flatMap { // Lowercase verse, remove punctuations, and split each word
        case (id, verse) =>
          verse.toLowerCase().split("[\\d\\p{Punct}\\s]+") map {
            word => (word, List(id)) // Transform word to word and verse ID
          }
      }
      .persist(StorageLevel.DISK_ONLY) // Persist the data to avoid computing RDD multiple times
      .reduceByKey(_.union(_)) // Combine all verse ID's with same key (word)
      .collect { // Remove duplicates, sort the List of verse ID's in lexicographical order
        case (word, _idList) =>
          (word, _idList.distinct.sortBy {
            (_idList) => (_idList.split(":")(0), _idList.split(":")(1).toInt, _idList.split(":")(2).toInt)
          })
      }
      .sortBy { // Sort by descending number of references, then by word
        case (word, _idList) =>
          (-_idList.size, word)
      }
      .collect {
        case (word, _idList) =>
          (word, _idList.toArray.mkString("[", ", ", "]")) // Format idList as [id, id, ..., id]
      }
      .saveAsTextFile(outputDir2)

    sc.stop()

  }
}