import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by AEB on 04/06/17.
  *
  * <p>
  *   Calculates the top N longest sessions where a session is
  *   defined as a maximum period of time between the start
  *   times of two consecutive songs in the user's playlist log.
  * </p>
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

    // calculate the N longest sessions
    val sessionDetails = trackLogData
      .map( line => line.split("\t") )
      .map ( fields => parseRecord( fields ) )
      .groupByKey()
      .map( playlists => getUserSessions( playlists, appParams.getLong( "session.max.millisecs.between.tracks" ) ) )
      .flatMapValues( identity )
      .takeOrdered( appParams.getInt( "top.n.sessions" ) )(Ordering[Long].reverse.on( x => x._2._2) )

    // output summary to disk
    writeToFile( sessionDetails, appParams.getString( "output.file.path" ) )

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
  def parseRecord( fields : Array[String]) : (String,(String, Option[Long])) = {
    if (fields.length != 6) {
      println("Parsed record does not contain 6 elements: " + fields.mkString(", "))
      ( fields(0), ("Unknown", getTimestamp( fields(1) )) )
    } else ( fields(0), (fields(5).mkString, getTimestamp( fields(1) )) )
  }

  /**
    * Converts string to a timestamp object.
    *
    * @param timestampString
    * @return
    */
  def getTimestamp( timestampString: String ) : Option[Long] = timestampString match {
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
    * <p>
    *   Splits each user's complete playlist up into separate sessions,
    *   where a session is defined as a period of time where each song
    *   starts within the number of milliseconds given by
    *   sessionMaxMillisecsBetweenTracks.
    * </p>
    *
    * <p>
    *   A list of session metadata (start, finish, playlist) and associated
    *   session length is is returned for each user ID.
    * </p>
    *
    * @param userIdPlaylists
    * @param sessionMaxMillisecsBetweenTracks
    * @return
    */
  def getUserSessions( userIdPlaylists : (String, (Iterable[(String, Option[Long])]) ), sessionMaxMillisecsBetweenTracks : Long )
    : (String, List[(Session, Long)]) = {

      // sort the track-timestamp pairs into timestamp order
      val orderedTracks =
        userIdPlaylists._2.toList.sorted( Ordering[Option[Long]].on( (x:(java.io.Serializable,Option[Long])) => x._2 ) )

      // build a list of play lists and session duration
      var firstTimestamp = orderedTracks(0)._2
      var lastTimestamp = orderedTracks(0)._2
      val playlistSession = List.newBuilder[ (Session,Long) ]
      var playlist = List.newBuilder[ String ]
      for ( ( track : String, timestamp ) <- orderedTracks ) {

        val timestampDelta : Long = timestamp.get - lastTimestamp.get
        if ( timestampDelta < 0 ) println( "Negative timestamp ordering detected, tracks will not be in the correct order" )

        // add new session play list and length to list and reset
        if ( timestampDelta > sessionMaxMillisecsBetweenTracks ) {

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

      // return a tuple pair of userId mapped to an array of session objects with session lengths
      ( userIdPlaylists._1, playlistSession.result() )

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
    * Formats a long/UNIX representation of the timestamp.
    *
    * @param unixTimestamp
    * @return
    */
  def setTimestamp( unixTimestamp: Long ) : String = {

    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    format.format(new Timestamp( unixTimestamp ) )

  }

  /**
    * Writes the data to disk.
    *
    * @param sessionDetails
    * @param outputFilePath
    */
  def writeToFile(sessionDetails : Array[(String,(Session, Long))], outputFilePath : String ) {
    val writer = new PrintWriter( new File( outputFilePath ) )
    for ( (userId, (session, _)) <- sessionDetails )
      writer.append(s"$userId\t${setTimestamp(session.startTimestamp)}\t${setTimestamp(session.endTimestamp)}\t${session.playlist}\n")
    writer.close()
  }

}