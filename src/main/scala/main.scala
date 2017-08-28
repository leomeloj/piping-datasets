import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by leonardo on 19/07/17.
  */
object main {
  def main(args: Array[String]): Unit = {
    val NUM_SAMPLES = 500
    val conf = new SparkConf().setAppName("Teste").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
      val x = math.random
      val y = math.random
      x * x + y * y < 1
    }.count()
    val pi = 4.0 * count / NUM_SAMPLES
    printf("Pi is roughly %f", pi)
  }
}
