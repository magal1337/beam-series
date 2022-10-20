# Beam Intro Scala Scio

## Create SBT project
```
sbt
set name := "beam-series-scala"
set scalaVersion := "2.12.15"
session save
exit
```

## Update build.sbt file
```
name:= "beam-series-scala"

scalaVersion:= "2.12.15"
val scioVersion = "0.10.3"
val beamVersion = "2.29.0"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  "com.spotify" %% "scio-extra" % scioVersion,
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
```

## Create package folder and scala object for Wordcount
```
src/main/scala/com/beam_series/beam_series_scala/beam_intro/wordCount.scala
```

## Create folder for data
```
mkdir data
cd data/
mkdir beam_intro
```

## run Scio WordCount Job
```
sbt "runMain com.beam-series.beam_series_scala.beam_intro.wordCount --input=data/beam_intro/kinglear.txt --output=data/beam_intro/counts
```