import java.io.{File, PrintWriter}

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AEB on 04/06/17.
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
      .map( fields => {
        if (fields.length != 6) {
          println( "Parsed record does not contain 6 elements: " + fields.mkString(", ") )
          ( fields(0), None )
        } else ( fields(0), fields(5) )
      } )
      .distinct() // remove duplicate tracks per user
      .countByKey() // count number of unique tracks per user

    // output summary to disk
    val writer = new PrintWriter( new File( appParams.getString( "output.file.path" ) ) )
    for ( (userId, numberOfTracks) <- uniqueTracksPerUserId ) writer.append( s"$userId\t$numberOfTracks\n" )
    writer.close()

    sparkContext.stop()

  }
}