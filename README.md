# spark-nnd

An efficient implementation of the Nearest Neighbor Descent algorithm on
Apache Spark.

---

This is a Spark implementation of the Nearest Neighbor Descent algorithm for
building a K-nearest neighbor graph (K-NNG). The code is based in [Efficient
K-Nearest Neighbor Graph Construction Using MapReduce for Large-Scale Data
Sets](https://www.researchgate.net/publication/285839354_Efficient_K-Nearest_Neighbor_Graph_Construction_Using_MapReduce_for_Large-Scale_Data_Sets),
a master's thesis by Tomohiro Warashina. The [original paper describing
NND](https://dl.acm.org/citation.cfm?id=1963487) mentions a naive implementation
that could be used in a MapReduce environment, but this implementation suffers
from low data transmission efficiency. The mentioned thesis proposes an improved
implementation on Hadoop MapReduce. This repository contains an adaptation of
this algorithm for Apache Spark.

## Usage

The package exposes a `NND` object with a single method `buildGraph`. This
method receives a `RDD[(Long, Node)]` where the first element of the tuple is the id of the element, and `Node` is:

```scala
case class Node(features: Vector, label: Option[Long], partition: Long = 0, finished: Boolean = false)
```

and returns a `RDD[(Long, NodeWithNeighbors)]` where:

```scala
case class NodeWithNeighbors(features: Vector, label: Option[Long], neighbors: Seq[(Long, Double)], partition: Long = 0, finished: Boolean = false)
```

_(You can ignore the `partition` and `finished` fields; they are used in the [package](https://github.com/fvictorio/spark-rgt) from where this implementation was extracted.)_

The new `neighbors` field is a sequence with the id and the similarity of each neighbor.

## Example

```scala
import com.github.fvictorio.nnd.{NND, Node}
...
val K = 10
val maxIterations = 5
val earlyTermination = 0.01
val sampleRate = 1.0
val bucketsPerInstance = 4
val rdd: RDD[(Long, Node)] = ??? // build your input
val result = NND.buildGraph(rdd, K, maxIterations, earlyTermination, sampleRate, bucketsPerInstance)
```

## Installation

In your `build.sbt` add:

```scala
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.fvictorio" % "spark-nnd" % "master-SNAPSHOT"
```

_Disclaimer: I use [jitpack](https://jitpack.io) because I have no idea how to publish a package in Sonatype._

## Comparison

An implementation of the naive approach can be found
[here](https://github.com/tdebatty/spark-knn-graphs). The following table
compares that implementation with the one in this repository. The tests were
done in a Google Cloud cluster using different subsets of the EMNIST dataset.
The columns show how much time each implementation took in building the graph,
and also the maximum shuffle size (the amount of data moved between nodes in a
stage).

<table>
  <thead>
  <tr>
    <th rowspan="2">Number of elements</th>
    <th colspan="2">Time (seconds)</th>
    <th colspan="2">Max shuffle size (MB)</th>
  </tr>
  <tr>
    <th>Compared implementation</th>
    <th>This implementation</th>
    <th>Compared implementation</th>
    <th>This implementation</th>
  </tr>
  </thead>
  <tr>
    <td>2K</td>
    <td>170</td>
    <td><b>149</b></td>
    <td>366</td>
    <td><b>39</b></td>
  </tr>
  <tr>
    <td>4K</td>
    <td>139</td>
    <td><b>132</b></td>
    <td>727</td>
    <td><b>76</b></td>
  </tr>
  <tr>
    <td>8K</td>
    <td>724</td>
    <td><b>229</b></td>
    <td>1462</td>
    <td><b>147</b></td>
  </tr>
  <tr>
    <td>16K</td>
    <td>1411</td>
    <td><b>701</b></td>
    <td>2900</td>
    <td><b>290</b></td>
  </tr>
</table>
