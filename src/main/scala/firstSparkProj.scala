//STEPHEN A. FRIEDMAN
//CSE 398
//P5
//DEC. 2015
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.parsing.combinator._
object firstSparkProj extends RegexParsers{
def main(args: Array[String]) {
val sc = new SparkContext("local[2]", "first spark app")

def artist: Parser[String] = """(')([^']*)(')""".r ^^ {_.toString}
def song: Parser[String] = """(')([^']*)(')""".r ^^ {_.toString}
def count: Parser[Int] = """[\d]*""".r ^^ {_.toInt}

def all: Parser[Tuple3[String,String,Int]] = artist ~ ", " ~ song ~ ", " ~ count ^^ {
case artist ~ ", " ~ song ~ ", " ~ count => new Tuple3(artist.replaceAll("'",""),song.replaceAll("'",""),count)
}

def doParse(someText:String): Tuple3[String,String,Int] =
{
 parse(all, someText) match
 {
  case Success(matched,_) => matched
  case Failure(badTuple,_)=> ("","",-1)
 }
}

val data = sc.textFile("data/songList.txt").map(line => line.substring(line.indexOf("'"),line.length-2)).map(songInfo => doParse(songInfo))

val trivialWords = List("","THE","YOU","I","A","ME","TO","OF","MY","IN","AND","ON","IT","YOUR","BE","IS","FOR","I*M")

val songs=data.map{case (a,s,c)=> s.replaceAll(",","").trim}
val songsWith1Count=data.map{case (a,s,c)=> (s.replaceAll(",","").trim,c)}.reduceByKey(_+_)

val totalWordCount = songs.flatMap{song=>song.replaceAll("""[\p{Punct}&&[^.|*]]""","").split(" ")}.map{case (word)=> if(!trivialWords.contains(word)){word}else{" "}}.filter(!_.contains(" ")).map{word => (word,1)}.reduceByKey(_+_).sortBy(-_._2)

var partA = totalWordCount.take(15)//.foreach(println)

var artists=data.map{case (a,s,c)=> a.replaceAll(",","").trim}.map{case (a) => (a,1)}.reduceByKey(_+_).sortBy(-_._2)
var partB1=artists.take(15)//.foreach(println)

var num1SongCountByArtist=data.map{case (a,s,c)=> if(c>0){(a,1)}else{(" ",0)}}.reduceByKey(_+_).sortBy(-_._2)
var partB2=num1SongCountByArtist.take(15)//.foreach(println)

val partAB1B2 = new Tuple3(partA,partB1,partB2)

//val output : List[Tuple3[String,Double,Double]] = List()
val top15words = totalWordCount.take(15)
val output = for{x <- top15words
val word = x._1
val count = x._2
val songsWithWord = songsWith1Count.map{case(s,c)=>(s.replaceAll("""[\p{Punct}&&[^.|*]]""",""),c)}.filter(_._1.split(" ").contains(word))
val songsWithWordAndNum1=songsWithWord.map{case (s,c)=> if(c==1){(s,c)}else{("!!!",0)}}.filter(!_._1.contains("!!!"))
val probSongNum1GivenAWord=((songsWithWordAndNum1.count).toDouble/(songsWithWord.count).toDouble).toDouble*100
val num1Songs = songsWith1Count.map{case(s,c)=>if(c>=1){(s,c)}else{("!!!",0)}}.filter(!_._1.contains("!!!"))
val probSongContainsWordGivenASongBeingNum1=((songsWithWordAndNum1.count).toDouble/(num1Songs.count).toDouble).toDouble*100
val outputTuple = new Tuple3(word,probSongNum1GivenAWord,probSongContainsWordGivenASongBeingNum1)
} yield outputTuple



//OUTPUT EVERYTHING
println("MOST POPULAR 15 WORDS APPEARING IN SONG TITLES")
println("----------------------------------------------")
partAB1B2._1.foreach(println)
println("------------------------------------------------------------------------------------")
println("LIST OF TOP 15 ARTISTS WITH MOST TOP 40 SONGS")
println("---------------------------------------------")
partAB1B2._2.foreach(println)
println("------------------------------------------------------------------------------------")
println("LIST OF TOP 15 ARTISTS WITH MOST #1 SONGS")
println("-----------------------------------------")
partAB1B2._3.foreach(println)
println("------------------------------------------------------------------------------------")
println("PROBABILITY OF A SONG BEING #1 GIVEN THAT THE SONG TITLE CONTAINS A PARTICULAR WORD")
println("-----------------------------------------------------------------------------------")
output.foreach(x => println(x._1+ " = " + x._2))
println("-----------------------------------------------------------------------------------------")
println("PROBABILITY A SONG TITLE CONTAINS A WORD GIVEN THAT ITS A #1 SONG")
println("-----------------------------------------------------------------")
output.foreach(x => println(x._1 + " = " + x._3))

sc.stop()
}
}