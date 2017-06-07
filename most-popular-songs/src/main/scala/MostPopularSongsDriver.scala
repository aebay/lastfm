import java.io.{File, PrintWriter}

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by AEB on 04/06/17.
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
      .map( fields => {
        if (fields.length != 6) {
          println( "Parsed record does not contain 6 elements: " + fields.mkString(", ") )
          ( (None, None), 1 )
        } else ( (fields(3), fields(5)), 1 )
      } )
      .reduceByKey( _+_ )

    // retrieve top 100 songs by number of times users have listened to them
    val mostPopularSongs = trackCount.takeOrdered( 100 )( Ordering[Int].reverse.on( x => x._2 ) )

    // output list to disk
    val writer = new PrintWriter( new File( appParams.getString( "output.file.path" ) ) )
    for ( ((artist, track), count) <- mostPopularSongs ) writer.append( s"$artist\t$track\t$count\n" )
    writer.close()

    sparkContext.stop()

  }

}