import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AEB on 04/06/17.
  */
object MostPopularSongsDriver {

  def main(args: Array[String]) {

    // Spark configuration
    val sparkConf = new SparkConf()
      .setAppName("Top 100 most popular songs")
      .setMaster( "local[*]" )
    val sparkContext = new SparkContext( sparkConf )

    // read in source data
    val trackLogFile = "/data/lastfm/userid-timestamp-artid-artname-traid-traname.tsv"
    val trackLogData = sparkContext.textFile( trackLogFile )

    // count number of unique songs per user
    val trackCount = trackLogData
      .map( line => line.split("\t") )
      .map( arr => {
        if (arr.length != 6) {
          println( "Parsed record does not contain 6 elements: " + arr.mkString(", ") )
          ( (None, None), 1 )
        } else ( (arr(3), arr(5)), 1 )
      } )
      .reduceByKey( _+_ )

    // retrieve top 100 songs by number of times users have listened to them
    val mostPopularSongs = trackCount.takeOrdered( 100 )( Ordering[Int].reverse.on( x => x._2 ) )

    // output list to disk
    val writer = new PrintWriter( new File( "/tmp/most-popular-songs.tsv" ) )
    for ( ((artist, track), count) <- mostPopularSongs ) writer.append( s"$artist\t$track\t$count\n" )
    writer.close()

    sparkContext.stop()

  }

}