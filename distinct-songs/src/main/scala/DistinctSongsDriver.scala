import java.io.{File, PrintWriter}

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AEB on 04/06/17.
  *
  * Counts the unique number of tracks played by each user.
  */
object DistinctSongsDriver {

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
    val uniqueTracksPerUserId = trackLogData
      .map( line => line.split("\t") )
      .map( fields => parseRecord( fields ) )
      .distinct() // remove duplicate tracks per user
      .map( { case( userId, _ ) => ( userId, 1 ) } )
      .reduceByKey( _+_ )
      .sortByKey()
      .collect()

    // output summary to disk
    writeToFile( uniqueTracksPerUserId, appParams.getString( "output.file.path" ) );

    sparkContext.stop()

  }

  /**
    * Parses the userId and track name from each line.
    *
    * @param fields
    * @return
    */
  def parseRecord( fields : Array[String]) : (String, String) = {
    if (fields.length != 6) {
      println( "Parsed record does not contain 6 elements: " + fields.mkString(", ") )
      ( fields(0), "Unknown" )
    } else ( fields(0), fields(5) )
  }

  /**
    * Writes the data to disk.
    *
    * @param uniqueTracksPerUserId
    * @param outputFilePath
    */
  def writeToFile(uniqueTracksPerUserId : Array[(String, Int)], outputFilePath : String ) {
    val writer = new PrintWriter( new File( outputFilePath ) )
    for ( (userId, numberOfTracks) <- uniqueTracksPerUserId ) writer.append( s"$userId\t$numberOfTracks\n" )
    writer.close()
  }
}