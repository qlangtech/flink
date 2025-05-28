# 开发分支维护方法
1. 添加远端仓库地址： git remote add src_flink https://github.com/apache/flink-cdc.git
   接下来可以用git remote 查看添加的远端仓库地址
2. 拉取远端仓库中最新的tag： `git fetch src_flink tag release-1.20.1`
3. 将新拉取到的tag，推送到本项目的远端仓库中去：`git push origin release-1.20.1`
4. 同步刚拉取到的远端分支，创建本地分支：`git checkout -b release-1.20.1 tis-1.20.1`
5. 查看旧版本分支修改内容 `git diff tis-1.18.1 release-1.18.1 --stat=1000 > diff-1.18.1.txt`
6. 查看两个分支中指定文件路径的差异：`git diff release-1.18.1 tis-1.18.1 -- src/utils.js`
7. 拷贝某个分支下的文件到当前分支下，例如：`git checkout tis-1.18.1 -- deploy.sh`

# Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)


### Features

* A streaming-first runtime that supports both batch processing and data streaming programs

* Elegant and fluent APIs in Java and Scala

* A runtime that supports very high throughput and low event latency at the same time

* Support for *event time* and *out-of-order* processing in the DataStream API, based on the *Dataflow Model*

* Flexible windowing (time, count, sessions, custom triggers) across different time semantics (event time, processing time)

* Fault-tolerance with *exactly-once* processing guarantees

* Natural back-pressure in streaming programs

* Libraries for Graph processing (batch), Machine Learning (batch), and Complex Event Processing (streaming)

* Built-in support for iterative programs (BSP) in the DataSet (batch) API

* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms

* Compatibility layers for Apache Hadoop MapReduce

* Integration with YARN, HDFS, HBase, and other components of the Apache Hadoop ecosystem


### Streaming Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.socketTextStream(host, port, '\n')

val windowCounts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .keyBy("word")
  .window(TumblingProcessingTimeWindow.of(Time.seconds(5)))
  .sum("count")

windowCounts.print()
```

### Batch Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.readTextFile(path)

val counts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .groupBy("word")
  .sum("count")

counts.writeAsCsv(outputPath)
```



## Building Apache Flink from Source

Prerequisites for building Flink:

* Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
* Git
* Maven (we require version 3.8.6)
* Java 8 or 11 (Java 9 or 10 may work)

```
git clone https://github.com/apache/flink.git
cd flink
./mvnw clean package -DskipTests # this will take up to 10 minutes
```

Flink is now installed in `build-target`.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala


### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

### Eclipse Scala IDE

**NOTE:** From our experience, this setup does not work with Flink
due to deficiencies of the old Eclipse version bundled with Scala IDE 3.0.3 or
due to version incompatibilities with the bundled Scala version in Scala IDE 4.4.1.

**We recommend to use IntelliJ instead (see above)**

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you find a bug in Flink.


## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## Externalized Connectors

Most Flink connectors have been externalized to individual repos under the [Apache Software Foundation](https://github.com/apache):

* [flink-connector-aws](https://github.com/apache/flink-connector-aws)
* [flink-connector-cassandra](https://github.com/apache/flink-connector-cassandra)
* [flink-connector-elasticsearch](https://github.com/apache/flink-connector-elasticsearch)
* [flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub)
* [flink-connector-hbase](https://github.com/apache/flink-connector-hbase)
* [flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)
* [flink-connector-kafka](https://github.com/apache/flink-connector-kafka)
* [flink-connector-mongodb](https://github.com/apache/flink-connector-mongodb)
* [flink-connector-opensearch](https://github.com/apache/flink-connector-opensearch)
* [flink-connector-prometheus](https://github.com/apache/flink-connector-prometheus)
* [flink-connector-pulsar](https://github.com/apache/flink-connector-pulsar)
* [flink-connector-rabbitmq](https://github.com/apache/flink-connector-rabbitmq)

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
