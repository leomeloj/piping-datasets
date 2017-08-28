import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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

  val spark = SparkSession.builder.master("local[*]").appName("My App").config("spark.sql.warehouse.dir", "file:///usr/local/spark/").getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    //Creates the Dataset based on the base FastQ File
    val ds: Dataset[FastQ] = readFASTQ("file.fastq").as[FastQ]
    val i = 0
    val lista = ds.collect()
    val newList = new ListBuffer[FastQ]()

    //Creates a list of FastQ values that represents the output of the Piped application (i.e. Trimmomatic)
    for(i <- 0 to lista.length - 1){
      newList += piping(lista(i))
    }

    //Creates a new DataSet based on the output of the Piped application (i.e. Trimmomatic)
    val dsNew = newList.toDS()

    dsNew.show()

  }

  //Function that reads a FastQ and return a DataFrame (The DF can be converted to a FastQ DS by using val ds: Dataset[FastQ] = readFASTQ("file.fastq").as[FastQ])
  def readFASTQ(filePath: String): DataFrame = {
    // Reads the Text file on the filePath defined in the argument
    val myFile = spark.read.textFile(filePath)
    //Creates an array by breaking the file by line separators
    val array = myFile.collect()

    val myFile1 = new ListBuffer[FastQ]()

    for(i <- 0 to array.length - 1 by 4){
      myFile1 += FastQ(array(i),array(i+1),array(i+2),array(i+3))
    }


    return myFile1.toDF()
  }

  def printList(args: TraversableOnce[_]): Unit = {
    args.foreach(println)
  }

  //Function that creates a pipe with the content of one tuple of a Dataset and return one FastQ typed value
  def piping(data: FastQ): FastQ = {
    //CHANGE THE LINES BELOW!!!
    val command = "src/main/scala/command.sh" //The script goes here
    val pipeInput = "pipeInput" //the path to the pipe goes here
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
    //FIND OUT WHY THE VALUES COMES IN A DIFFERENT ORDER 0,3,1,2 instead of 0,1,2,3
    return FastQ(outputList(0), outputList(3),outputList(1),outputList(2))

  }
}
