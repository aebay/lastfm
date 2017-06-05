import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by AEB on 04/06/17.
  */
object LongestSessionsDriver {

  def main(args: Array[String]) {

    // Spark configuration
    val sparkConf = new SparkConf()
      .setAppName("The 10 longest sessions")
      .setMaster( "local[*]" )
    val sparkContext = new SparkContext( sparkConf )

    // read in source data
    val trackLogFile = "/data/lastfm/userid-timestamp-artid-artname-traid-traname.tsv"
    val trackLogData = sparkContext.textFile( trackLogFile )

    // calculate the 10 longest sessions
    val sessionDetails = trackLogData
      .map( line => line.split("\t") )
      .map( fields => {
        if (fields.length != 6) {
          println( "Parsed record does not contain 6 elements: " + fields.mkString(", ") )
          ( fields(0), (None,None) )
        } else ( fields(0), (fields(5), getTimestamp( fields(1) )) )
      } )
      .groupByKey()
      .map( playLists => {

        // sort the track-timestamp pairs into order
        //( playLists._1, playLists._2.toList.sorted( Ordering[Option[Long]].on( (x:(java.io.Serializable,Option[Long])) => x._2 ) ) )
        val orderedTracks = playLists._2.toList.sorted( Ordering[Option[Long]].on( (x:(java.io.Serializable,Option[Long])) => x._2 ) )

        // build a list of play lists and session duration
        var firstTimestamp = orderedTracks(0)._2
        var lastTimestamp = None
        val playListSession = List.newBuilder[ (List[java.io.Serializable],Long) ]
        var playList = List.newBuilder[ java.io.Serializable ]
        for ( ( track, timestamp ) <- orderedTracks ) {

          playList += track
          lastTimestamp = timestamp
          if ( timestamp - lastTimestamp > 1200000 ) {
            lastTimestamp =
          }

        }



        // iterate through the list of sorted tracks


      })
      .collect()



    // output summary to disk
    val writer = new PrintWriter( new File( "/tmp/distinct-songs.csv" ) )
    /*for ( (userId, numberOfTracks) <- uniqueTracksPerUserId ) writer.append( s"$userId, $numberOfTracks\n" )*/
    writer.close()

    sparkContext.stop()

  }

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
    * Ordering implicit for timestamps.
    *
    * https://stackoverflow.com/questions/29985911/sort-scala-arraybuffer-of-timestamp
    *
    * @return
    */
  /*implicit def ordered: Ordering[Long] = new Ordering[Long] {
    def compare(x: Long, y: Long): Int = x compareTo y
  }*/

}