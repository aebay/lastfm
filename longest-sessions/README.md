# lastfm:longest-sessions

Scala Spark application that analyses the contents of the Last.fm dataset
  that can be found at [http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html]
  
Input:  Last.fm dataset.  
Output: A list of the 10 longest sessions (where a session is defined as a gap of 20 minutes or more between the start of two consecutive songs) with the following information about each session: user ID, timestamps of the start of the first and last songs in the session and the list of songs played in the session (in order of play).

## Configuration

By default, the dataset is assumed to be located at `/data/lastfm/userid-timestamp-artid-artname-traid-traname.tsv`.  

Results are written to `/tmp/longest-sessions.tsv`.

The file paths above can be customised by modifying the `application.properties` file. 

## Build

**Note**: this will build all sources so only needs to be completed once.

1. Go to the root directory of the `lastfm` project.
2. Run `$ mvn clean package`.

## Deploy

**Note**: replace `<VERSION>` with the version of the JAR you are using.

1. Go to the root directory of the `lastfm` project.
2. `$ cd ./longest-sessions/target/`
3. Copy `longest-sessions-<VERSION>.jar` and `longest-sessions-<VERSION>-configuration-files.zip`  
to a working directory of your choice.
4. Run `$ unzip -j longest-sessions-<VERSION>-configuration-files.zip` to unpack the properties files.

## Run

1. Navigate to the working directory chosen in the **Deploy** section.
2. Run the following spark-submit command:
```
spark-submit --master local[*] \
             --class LongestSessionsDriver \
             --files 'spark.properties,application.properties' \
             --driver-class-path ./ 
             longest-sessions-<VERSION>.jar
```