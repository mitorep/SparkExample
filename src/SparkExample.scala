import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkExample {
    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("SparkExample").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val lines = sc.textFile("test.txt")

      val words = lines.flatMap(l => l.split(" "))

      val pairs = words.map(w => (w, 1))

      val wordCount = pairs.reduceByKey((a, b) => a + b)

      wordCount.collect().foreach(println)

      wordCount.collect()
    }
  }



