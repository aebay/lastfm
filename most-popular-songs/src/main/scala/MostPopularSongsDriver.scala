import java.io.{File, PrintWriter}

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AEB on 04/06/17.
  *
  * Creates a list of the N most popular songs and number of times each was played.
  */
object MostPopularSongsDriver {

  def main(args: Array[String]) {

    // parameters
    val sparkParams = ConfigFactory.load( "spark.properties" );
    val appParams = ConfigFactory.load( "application.properties" );

    // Spark configuration
    val sparkConf = new SparkConf()
      .setMaster( sparkParams.getString( "spark.master" ) )
      .setAppName( sparkParams.getString("app.name") )
    val sparkContext = new SparkContext( sparkConf )

    // read in source data
    val trackLogData = sparkContext.textFile( appParams.getString( "input.file.path" ) )

    // count number of unique songs per user
    val trackCount = trackLogData
      .map( line => line.split("\t") )
      .map( fields => (parseRecord( fields ), 1) )
      .reduceByKey( _+_ )

    // retrieve top N songs by number of times users have listened to them
    val mostPopularSongs =
      trackCount.takeOrdered( appParams.getInt( "top.n.songs" ) )( Ordering[Long].reverse.on( x => x._2 ) )

    // output list to disk
    writeToFile( mostPopularSongs, appParams.getString( "output.file.path" ) )

    sparkContext.stop()

  }

  /**
    * <p>
    *   Parses the artist and track name from each line
    * </p>
    *
    * @param fields
    * @return
    */
  def parseRecord( fields : Array[String]) : (String, String) = {
    if (fields.length != 6) {
      println( "Parsed record does not contain 6 elements: " + fields.mkString(", ") )
      ("Unknown", "Unknown")
    } else (fields(3), fields(5))
  }

  /**
    * Writes the data to disk.
    *
    * @param mostPopularSongs
    * @param outputFilePath
    */
  def writeToFile( mostPopularSongs : Array[((String,String), Int)], outputFilePath : String ) {
    val writer = new PrintWriter( new File( outputFilePath ) )
    for ( ((artist, track), count) <- mostPopularSongs ) writer.append( s"$artist\t$track\t$count\n" )
    writer.close()
  }

}