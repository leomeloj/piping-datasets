import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ListBuffer




/**
  * Created by leonardo on 19/07/17.
  */
object pipe {
  case class FastQ (
                     id: String,
                     sequence: String,
                     description: String,
                     quality: String
                   )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("My App").config("spark.sql.warehouse.dir", "file:///usr/local/spark/").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val ds = Seq(FastQ("@SEQ_ID2", "GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT", "+", "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65"), FastQ("@SEQ_ID1", "GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT", "+", "!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65")).toDS()
    val i = 0
    val lista = ds.collect()
    val newList = new ListBuffer[FastQ]()

    for(i <- 0 to lista.length - 1){
      newList += piping(lista(i))
    }

    val dsNew = newList.toDS()

    dsNew.show()

  }

  def piping(data: FastQ): FastQ = {
    val command = "/home/leonardo/projects/sparkNew/src/main/scala/command.sh"
    val pipeInput = "/home/leonardo/projects/pipeInput"
    val proc = Runtime.getRuntime.exec(Array(command))

    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines)
          System.err.println(line)
      }
    }.start()

    new Thread("stdin writer for " + command) {
      override def run() {
        val out = new PrintWriter(proc.getOutputStream)
        for (elem <- List(data.id, data.description, data.quality, data.sequence))
          out.println(elem)
        out.close()
      }
    }.start()

    val outputLines = Source.fromInputStream(proc.getInputStream).getLines
    val outputList = outputLines.toList
    return FastQ(outputList(0), outputList(1),outputList(2),outputList(3))

  }
}
