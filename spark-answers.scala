/** Question1: Given two files:
spark16/file1.txt
1,9,5
2,7,4
3,8,3
spark16/file2.txt
1,g,h
2,i,j
3,k,l
Load these two tiles as Spark RDD and join them to produce the below results
(l,((9,5),(g,h)))
(2, ((7,4), (i,j))) (3, ((8,3), (k,l)))
And write code snippet which will sum the second columns of above joined results (5+4+3)*/
/** Answer **/
val join = sc.textFile("spark16/file*.txt").map{
  _.split(",", -1) match {
    case Array(a,b,c) => (a,(b,c))
  }
}.groupByKey()
// (2,CompactBuffer((7,4), (i,j)))
// (3,CompactBuffer((8,3), (k,l)))
// (1,CompactBuffer((9,5), (g,h)))
join.mapValues(_.head).map{
  case (_, (_,n2)) => n2.toInt
}.reduce(_+_)
// =12

/** Question2: Given:
val b = sc.parallelize(List(1,2,3,4,5,6,7,8,2,4,2,1,1,1,1,1))
write code snippet for Operation_xyz which will return.
Map[lnt,Long] = Map(5 -> 1, 8 -> 1, 3 -> 1, 6 -> 1, 1 -> S, 2 -> 3, 4 -> 2, 7 ->1)
*/
/** Answer **/
def Operation_xyz (rdd: org.apache.spark.rdd.RDD[Int]) : scala.collection.Map[Int,Long] = {
   return rdd.countByValue
}
Operation_xyz(b)

/** Question3: Given:
val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"))
val b = a.keyBy(_.length)
Write a correct code snippet which will produce desired output, shown below.
Array[(Int, Seq[String])] = Array((4,ArrayBuffer(lion)), (6,ArrayBuffer(spider)),
(3,ArrayBuffer(dog, cat)), (5,ArrayBuffer(tiger, eagle}}}
*/
/** Answer **/
val b = a.keyBy(_.length)
/*(3,dog)
(3,cat)
(5,tiger)
(6,spider)
(5,eagle)
(4,lion)*/
b.groupByKey.collect()
//Array[(Int, Iterable[String])] = Array((4,CompactBuffer(lion)), (6,CompactBuffer(spider)), (3,CompactBuffer(dog, cat)), (5, CompactBuffer(tiger, eagle)))

/** Question4: Given:
val a = sc.parallelize(1 to 10, 3)
val b = a.collect

Output 1 -
Array[Int] = Array(2, 4, 6, 8,10)
operation2

Output 2 -
Array[Int] = Array(1,2,3)
*/
/** Answer **/
def op1(b: Array[Int]): Array[Int] = {
  return b.filter(_ % 2 == 0)
}
def op2(b: Array[Int]): Array[Int] = {
  return b.filter(_ < 4)
}

/** Question4: Given:
val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
val b = sc.parallelize(1 to a.count.toInt, 2)
val c = a.zip(b)

Output
Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))
*/
/** Answer **/
c.sortByKey(false).collect
//Array[(String, Int)] = Array((owl,3), (gnu,4), (dog,1), (cat,2), (ant,5))

/** Question5: Given:
patient data in csv format:
patientID,name,dateOfBirth,lastVisitDate
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20
1003,Ali,2011-01-30,2012-10-21

Output
1. Find all the patients whose lastVisitDate between current time and '2012-09-15'
2. Find all the patients who born in 2011
3. Find all the patients age
4. List patients whose last visited more than 60 days ago
5. Select patients 18 years old or younger
*/
/** Answer **/
/* echo "patientID,name,dateOfBirth,lastVisitDate
1001,Ah Teck,1991-12-31,2012-01-20
1002,Kumar,2011-10-29,2012-09-20
1003,Ali,2011-01-30,2012-10-21" >> patients.csv

hdfs dfs -mkdir sparksql
hdfs dfs -put patients.csv sparksql/
hdfs dfs -ls sparksql
*/
//use implicitly convert an RDD to a DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.sqlContext.implicits._
val schema= new StructType().
        add("patientID", IntegerType).
        add("name", StringType).
        add("dateOfBirth", DateType).
        add("lastVisitDate", DateType)
// load df
val df=spark.read.format("csv").
          option("header",true).
          schema(schema).
          load("sparksql/patients.csv")
// 1. from lastVisitDate between current time and '2012-09-15'
df.filter($"lastVisitDate".between("2012-09-15", current_date())).show()
// 2. Find all the patients who born in 2011
df.filter(year($"dateOfBirth") === 2011).show()
// 3. Find all the patients age
df.withColumn("age", round(datediff( current_date(), $"dateOfBirth") /365)).show()
// 4. List patients whose last visited more than 60 days ago
df.filter(datediff(current_date(), $"lastVisitDate") > 60).show()
//df.withColumn("lastVisitFromToday", datediff( current_date(), $"lastVisitDate")).show()
// 5. Find patients 18 years or older
df.withColumn("age", round(datediff( current_date(), $"dateOfBirth") /365)).
    filter($"age" > 18).show()
