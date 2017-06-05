import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AEB on 04/06/17.
  */
object LongestSessionsDriver {

  def main(args: Array[String]) {

    // Spark configuration
    val sparkConf = new SparkConf()
      .setAppName("Distinct songs per user")
      .setMaster( "local[*]" )
    val sparkContext = new SparkContext( sparkConf )

    // read in source data
    val trackLogFile = "/data/lastfm/userid-timestamp-artid-artname-traid-traname.tsv"
    val trackLogData = sparkContext.textFile( trackLogFile )

    // count number of unique songs per user
    val uniqueTracksPerUserId = trackLogData
      .map( line => line.split("\t") )
      .map( arr => {
        if (arr.length != 6) {
          println( "Parsed record does not contain 6 elements: " + arr.mkString(", ") )
          ( arr(0), None )
        } else ( arr(0), arr(5) )
      } )
      .distinct() // remove duplicate tracks per user
      .countByKey() // count number of unique tracks per user

    // output summary to disk
    val writer = new PrintWriter( new File( "/tmp/distinct-songs.csv" ) )
    for ( (userId, numberOfTracks) <- uniqueTracksPerUserId ) writer.append( s"$userId, $numberOfTracks\n" )
    writer.close()

    sparkContext.stop()

  }
}