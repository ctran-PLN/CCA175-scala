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
