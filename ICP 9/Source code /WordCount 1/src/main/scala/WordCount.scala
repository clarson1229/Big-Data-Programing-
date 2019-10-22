/**
 * Illustrates flatMap + countByValue for wordcount.
 */


import org.apache.spark._
import java.io._



object WordCount {
    def merge(l1:List[Int], l2:List[Int]): List[Int] = (l1,l2) match{
      case(Nil, _) => l2
      case(_, Nil) => l1 // returns the one that is not nil
      case (head1::tail1, head2::tail2) => // assumed sorted list
        if(head1<head2) head1:: merge(tail1,l2) // if head is bigger than second head then merge first then second otherwise other
        else head2::merge(l1,tail2)
    }

    def main(args: Array[String]) {

      //val inputFile = args(0)
      //val outputFile = args(1)
      val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val listToSort = sc.parallelize(List[Int](2,4,9,5,26,2,0,8))
      val resultList = listToSort.map(sub_l => List[Int](sub_l))
      val merged = resultList.reduce((l1, l2) => merge(l1, l2))
      val file = "output.txt"
      val writter = new BufferedWriter(new FileWriter(file))
      writter.write(merged.toString())
      writter.close()


    }
}
