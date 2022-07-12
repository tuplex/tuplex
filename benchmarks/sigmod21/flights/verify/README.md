## Flight query verification script
This folder contains a SparkSQL job written in Scala for performance reasons to validate the output.
Build it using the Scalabuild tool sbt via

```
sbt package
```

then run via

```
spark-submit target/scala-2.11/... folderA folderB
```
