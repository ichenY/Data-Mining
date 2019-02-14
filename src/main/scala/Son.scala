import java.io.{File, PrintWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object Son {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("hw2").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val csv = sc.textFile("./data/small1.csv")
    val support = 3
    val header = csv.first()
    val rawData = csv.filter(x=>x!=header).map(x=>x.split(",")).map(x=>(x(0),x(1))).distinct().groupByKey().map(x=>(x._1,x._2.toList))
    //rawData.foreach(println)
    val numofbasket = rawData.count().toDouble


    val firstPhase = rawData.mapPartitions(part=> {
      var items =ListBuffer.empty[(List[String])]
      part.toList.map(x=>items += x._2) //toList -> ListBuffer(List1,List2,List3,...)
      val result = apriori(items,support,numofbasket)
      result.toIterator
    }).distinct.collect

    val secondPhase = rawData.mapPartitions(part=> {
      var items =ListBuffer.empty[(List[String])]
      part.toList.map(x=>items += x._2)
      val itemCount = firstPhase.map(x=>{
        var count = 0
        items.foreach(y=>{
          if(x.forall(y.contains))  count += 1
        })
        (x,count)
      })
      itemCount.foreach(println)
      itemCount.toIterator
    }).reduceByKey(_+_).filter(x=>x._2.toDouble >= support).map(x=>x._1).sortBy(x=>x.size).collect

    val path = "result"
    val file = new File("OutputFiles/" + path)
    file.getParentFile().mkdir()
    val txt = new PrintWriter(file)
    var sizeof = 0
    for (r <- secondPhase) {
      //println(s,s.size)
      if (r.size != sizeof) {
        txt.write("\n\n" + r.mkString("('", "','", "')"))
        sizeof+=1
      } else {
        txt.write(","+r.mkString("('","','","')"))
      }
    }
    txt.close()


  }


  def apriori(items: ListBuffer[List[String]], support: Int, numofbasket: Double) : List[List[String]] = {
    //println("item size is ::"+items.size.toDouble)
    var result = ListBuffer.empty[List[String]]
    val sup = support * (items.size.toDouble/numofbasket)
    val singleton = items.flatten.groupBy(identity).map(x=>(List(x._1),x._2.size)).filter(x=>x._2 >= sup) //flatten to check each value
    //List() easier to process in combination
    //singleton : Map[List(String),Int]

    var itemset = singleton.keys.toList // List[String1,String2,...]   List(98, 100, 99, 103, 102, 101, 97)
    itemset.foreach(x=>result.append(x))

    var itr = 2
    var sizeofItemset = 1

    do{
      var pair = combination(itemset,itr)       //get List[List[String]]
      //pair.foreach(println)
      var candidate = pair.map(x=>{
        var count = 0
        items.foreach(y=>{
          if(x.forall(y.contains)){
            count += 1
          }
        })

        (x,count)
    }).filter(x=>x._2 >= sup)
      itemset = candidate.map(x=>x._1)
      itemset.foreach(x=>result.append(x))
      sizeofItemset = candidate.size
      itr += 1
      println("itr"+itr)
    }while(sizeofItemset != 0)
    return result.toList

  }
  def combination(itemset: List[List[String]], itr: Int) : List[List[String]] = {
    var singleton = itemset.flatten.distinct
    var candSet = Set.empty[List[String]]
    itemset.foreach(x=> {
      singleton.foreach(y => {
        var temp = ListBuffer(x: _*)
        if (!x.contains(y)) {
          temp += y
        }

        if (temp.size == itr) {
          candSet += temp.toList.sorted
          //candSet : Set(List1,List2,...)        Set(List(97, 105), List(105, 102),...)
        }
      })
    })
    //println(candSet.size) (42)
    return candSet.toList
  }


  }


