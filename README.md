# Pro Spark Streaming

Code used in "Pro Spark Streaming: The Zen of Real-time Analytics using Apache Spark" published by Apress Publishing.

ISBN-13: 978-1484214800

ISBN-10: 1484214803

# Layout

Each folder contains code for a particular chapter. The repetition of code is deliberate. While this goes against most software engineering principles (held very dearly by the author as well), it is necessary to expound a topic and keep its implementation self-contained.

## Chapters

- 2:  Introduction to Spark
- 3:  DStreams: Real-time RDDs
- 4:  High Velocity Streams: Parallelism and Other Stories
- 5:  Real-time Route 66: Linking External Data Sources
- 6:  The Art of Side Effects
- 7:  Getting Ready for Prime Time
- 8:  Real-time ETL and Analytics Magic
- 9:  Machine Learning at Scale
- 10: Of Clouds, Lambdas, and Pythons

# Build

Jump to a particular folder and simply execute `sbt assembly`. This will generate an uber JAR that can directly be submitted to a Spark cluster.