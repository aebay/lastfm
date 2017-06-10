# lastfm

Scala Spark application that analyses the contents of the Last.fm dataset
  that can be found at [http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html]
  
All submodules can be run independently:
   
   - **distinct-songs**: The number of distinct songs each user has listened to
   - **most-popular-songs**: A list of the top 100 songs users have listened to  
   with the number of times each song has been listened to.
   - **longest-sessions**: list of the top 10 longest sessions (where a session is defined as a gap of 20 minutes or more between the start of two consecutive songs) and the playlist.

Build instructions can be found in the indivdual README.md files within each submodule.
  
The README instructions are written assuming that the operating system is Linux. The code was developed in IntelliJ Community Edition and can be, alternatively, run from the IDE, although the JVM may need to have more heap space allocated in some situations depending on your local configuration.

## Tooling required

- Apache Maven
- Apache Spark 2.1.1
- Java JDK