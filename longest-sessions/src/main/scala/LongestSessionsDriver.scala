import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by AEB on 04/06/17.
  */
object LongestSessionsDriver {

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

    // calculate the 10 longest sessions
    val sessionDetails = trackLogData
      .map( line => line.split("\t") )
      .map( fields => {
        if (fields.length != 6) {
          println( "Parsed record does not contain 6 elements: " + fields.mkString(", ") )
          ( fields(0), (None,None) )
        } else ( fields(0), (fields(5).mkString, getTimestamp( fields(1) )) )
      } )
      .groupByKey()
      .map( playlists => {

        // sort the track-timestamp pairs into timestamp order
        val orderedTracks = playlists._2.toList.sorted( Ordering[Option[Long]].on( (x:(java.io.Serializable,Option[Long])) => x._2 ) )

        // build a list of play lists and session duration
        var firstTimestamp = orderedTracks(0)._2
        var lastTimestamp = orderedTracks(0)._2
        val playlistSession = List.newBuilder[ (Session,Long) ]
        var playlist = List.newBuilder[ String ]
        for ( ( track : String, timestamp ) <- orderedTracks ) {

          val timestampDelta : Long = timestamp.get - lastTimestamp.get

          // add new session play list and length to list and reset
          if ( timestampDelta > 1200000 ) {

            val sessionLength = lastTimestamp.get - firstTimestamp.get
            val session = Session( firstTimestamp.get, lastTimestamp.get, playlist.result() )
            playlistSession += (( session, sessionLength ))
            playlist.clear()
            firstTimestamp = timestamp

          }

          // update current session play list
          playlist += track
          lastTimestamp = timestamp

        }

        // return a tuple pair of userId mapped to an array of playlists with session length
        ( playlists._1, playlistSession.result() )

      })
      .flatMapValues( identity )
      .takeOrdered(10)(Ordering[Long].reverse.on( x => x._2._2) )

    // output summary to disk
    val writer = new PrintWriter( new File( appParams.getString( "output.file.path" ) ) )
    for ( (userId, (session, _)) <- sessionDetails )
      writer.append(s"$userId\t${setTimestamp(session.startTimestamp)}\t${setTimestamp(session.endTimestamp)}\t${session.playlist}\n")
    writer.close()

    sparkContext.stop()

  }

  /**
    * Class to represent metadata of a playlist session.
    *
    * @param startTimestamp
    * @param endTimestamp
    * @param playlist
    */
  case class Session( startTimestamp:Long, endTimestamp:Long, playlist:List[String] )

  /**
    * Converts string to a timestamp object.
    *
    * Adapted from: https://stackoverflow.com/questions/29844144/better-way-to-convert-a-string-field-into-timestamp-in-spark#37449188
    *
    * @param timestampString
    * @return
    */
  def getTimestamp(timestampString: String) : Option[Long] = timestampString match {
    case "" => None
    case _ => {
      val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      Try(new Timestamp(format.parse(timestampString).getTime)) match {
        case Success(timestamp) => Some(timestamp.getTime())
        case Failure(_) => None
      }
    }
  }

  /**
    * Formats a long/UNIX representation of the timestamp.
    *
    * @param unixTimestamp
    * @return
    */
  def setTimestamp( unixTimestamp: Long ) : String = {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    format.format(new Timestamp( unixTimestamp ) )

  }

}