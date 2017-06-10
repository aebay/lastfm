# lastfm:most-popular-songs

Scala Spark application that analyses the contents of the Last.fm dataset
  that can be found at [http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html]
  
Input:  Last.fm dataset.  
Output: A list of the 100 most popular songs (artist and title) with the number of times they've been played.

## Configuration

By default, the dataset is assumed to be located at `/data/lastfm/userid-timestamp-artid-artname-traid-traname.tsv`.  

Results are written to `/tmp/most-popular-songs.tsv`.

The file paths above can be customised by modifying the `application.properties` file. 

## Build

**Note**: this will build all sources so only needs to be completed once.

1. Go to the root directory of the `lastfm` project.
2. Run `$ mvn clean package`.

## Deploy

**Note**: replace `<VERSION>` with the version of the JAR you are using.

1. Go to the root directory of the `lastfm` project.
2. `$ cd ./most-popular-songs/target/`
3. Copy `most-popular-songs-<VERSION>.jar` and `most-popular-songs-<VERSION>-configuration-files.zip`  
to a working directory of your choice.
4. Run `$ unzip -j most-popular-songs-<VERSION>-configuration-files.zip` to unpack the properties files.

## Run

1. Navigate to the working directory chosen in the **Deploy** section.
2. Run the following spark-submit command:
```
spark-submit --master local[*] \
             --class MostPopularSongsDriver \
             --files 'spark.properties,application.properties' \
             --driver-class-path ./ 
             most-popular-songs-<VERSION>.jar
```