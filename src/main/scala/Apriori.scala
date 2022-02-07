

import org.slf4j.{Logger, LoggerFactory}
import scala.collection.parallel._
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Set
import scala.collection
import scala.collection._
//import scala.collection.immutable.Set
//import org.wdm.core.support.AbstractSupport
//import org.wdm.core.{ItemSet, Transaction, FrequentSet}
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
//import scala.collection.mutable.BitSet
//import org.apache.hadoop.util.bloom.DynamicBloomFilter
//import java.security.MessageDigest
//import java.security.NoSuchAlgorithmException;
//import org.apache.hadoop.hbase.util.{Hash, BloomFilter, DynamicByteBloomFilter}
//import org.apache.hadoop.util.bloom.BloomFilter
//import org.apache.hadoop.util.hash.Hash
import java.util.BitSet
//import java.util.Map
//import java.util.HashMap
import java.util.TreeSet
import java.nio.charset.Charset

import scala.math.{ceil, log}

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs
import java.lang.Integer.{ rotateLeft => rotl }
//
//   Stopwatch for benchmarking 
//
class  HashTreeNode extends Serializable {
	var mapAtNode=HashMap[Integer,HashTreeNode]();
	var isLeafNode:Boolean=false;
	var itemsets=List[Set[Int]]();
	//def isLeafNode_=(): Boolean = {
	  //return isLeafNode
	//}
    //def itemsets_=(x$1: List[scala.collection.immutable.Set[Int]]): Unit = ???
    //def mapAtNode_=(x$1: java.util.Map[Integer,HashTreeNode]): Unit = ???
	/*def HashTreeNode() {
		mapAtNode = new HashMap[Integer,HashTreeNode]();
		isLeafNode = false;
		itemsets = List[Set[Int]]();
	}*/

	//@Override
	/*def toString1():String= {
		var builder = new StringBuilder();
		builder.append("IsLeaf : ").append(isLeafNode.toString()).append("\t");
		builder.append("MapKeys :").append(mapAtNode.keySet().toString()).append("\t");
		builder.append("Itemsets : ").append(itemsets.toString());

		return builder.toString();
	}*/

	def getMapAtNode():HashMap[Integer,HashTreeNode]= {
		return mapAtNode;
	}

	def setMapAtNode(mapAtNode:HashMap[Integer,HashTreeNode]):Unit= {
		this.mapAtNode = mapAtNode;
	}

	def isLeafNode1():Boolean= {
		return isLeafNode;
	}

	def setLeafNode(isLeafNode:Boolean):Unit= {
		this.isLeafNode = isLeafNode;
	}

	def getItemsets():List[Set[Int]]= {
		return itemsets;
	}

	def setItemsets(itemsets:List[Set[Int]]):Unit= {
		this.itemsets = itemsets;
	}

}
class HashTreeUtils1 extends Serializable {
	/*
	 * Builds hashtree from the candidate itemsets.
	 */
    //def compfn2(e1: Int, e2: Int) = (e1 > e2)
	def buildHashTree(candidateItemsets:List[scala.collection.Set[Int]], itemsetSize:Int):HashTreeNode =	{
		var hashTreeRoot= new HashTreeNode();
		//hashTreeRoot.HashTreeNode();
		var parentNode:HashTreeNode  = null;
		var currNode:HashTreeNode  = null;
		for(currItemset<-candidateItemsets) {
		  
		  if(currItemset.size==itemsetSize){
			parentNode = null;
			currNode = hashTreeRoot;
			var currItemset1=currItemset.toList.sortWith(_ < _)
			//println("curritemset is--- "+currItemset1)
			for(i:Int <- 1 to itemsetSize) {
			    
				var item:Integer = currItemset1(i-1);
			    //println("item is "+item)
				var mapAtNode:HashMap[Integer, HashTreeNode] = currNode.getMapAtNode();
			    //println("mapatnode---"+mapAtNode)
				parentNode = currNode;
				
				if(mapAtNode.contains(item)) {
					//currNode = mapAtNode.getOrElse(item,null);
					currNode = mapAtNode.apply(item);
					//println("currnode is --")
				}
				else {
				    //println("add new")
					currNode=new HashTreeNode();
				    //println("item"+item+"currnode"+currNode)
					mapAtNode.+=((item, currNode));
				}
				
				parentNode.setMapAtNode(mapAtNode);
			}
			
			currNode.setLeafNode(true);
			var itemsets:List[Set[Int]] = currNode.getItemsets();
			itemsets=itemsets.+:((currItemset1.toSet))
			//println("itemsets--"+itemsets)
			currNode.setItemsets(itemsets);
			//println("result leaf node is--"+currNode.getItemsets())
		  }
		}
		//println("hashtreerootmap -----------------------------"+hashTreeRoot.getMapAtNode())
		return hashTreeRoot;
	}
	
	/*
	 * Returns the set of itemsets in a transaction from the set of candidate itemsets. Used hash tree
	 * data structure for fast generation of matching itemsets.
	 */
	def findItemsets( hashTreeRoot:HashTreeNode,  t:Set[Int],  startIndex:Int):List[Set[Int]]=	{
		
		var matchedItemsets =List[Set[Int]]();
		for(i<-startIndex to ((t.size)-2)) {
		    var t1=t.toList.sorted
		    //println("t1 is"+t1)
			var item:Integer = t1(i-1);
		    //println("item ishere--------"+item)
			val mapAtNode:HashMap[Integer, HashTreeNode] = hashTreeRoot.getMapAtNode();
            //println("mapatnodeis here-------"+mapAtNode)
			if(mapAtNode.contains(item)) {
				//continue;
			//}
			var itemset:List[Set[Int]] = findItemsets1(mapAtNode.apply(item), t1, i+1);
			//println("itemset--"+itemset)
			matchedItemsets=matchedItemsets.:::(itemset);
			//println("matched"+matchedItemsets)
			}
		}
		//println("finl matched"+matchedItemsets)
		return matchedItemsets;
	}
	def findItemsets1( hashTreeRoot:HashTreeNode,  t:List[Int],  startIndex:Int):List[Set[Int]]=	{
		if(hashTreeRoot.isLeafNode1()) {
		    //println("leaf found"+hashTreeRoot.getItemsets())
			return hashTreeRoot.getItemsets();
		}

		var matchedItemsets =List[Set[Int]]();
		for(i<-startIndex to t.size) {
		    //var t1=t.toList.sorted
		    //println("t1 is"+t1)
			var item:Integer = t(i-1);
		    //println("item ishere--------"+item)
			val mapAtNode:HashMap[Integer, HashTreeNode] = hashTreeRoot.getMapAtNode();
            //println("mapatnodeis here-------"+mapAtNode)
			if(mapAtNode.contains(item)) {
				//continue;
			//}
			var itemset:List[Set[Int]] = findItemsets1(mapAtNode.apply(item), t, i+1);
			//println("itemset--"+itemset)
			matchedItemsets=matchedItemsets.:::(itemset);
			//println("matched"+matchedItemsets)
			}
		}
		//println("finl matched"+matchedItemsets)
		return matchedItemsets;
	}
	/*
	 * Prints the hashtree for debugging purposes.
	 */
	def printHashTree( hashTreeRoot:HashTreeNode):Unit={
		if(hashTreeRoot == null) {
			System.out.println("Hash Tree Empty !!");
			return;
		}
		
		System.out.println("Node " + hashTreeRoot.toString());
		var mapAtNode:Map[Integer, HashTreeNode] = hashTreeRoot.getMapAtNode();
		for( entry<- mapAtNode.keySet) {
			printHashTree(mapAtNode.apply(entry));	
		}
		
 	}
}
class Stopwatch {

  private var startTime = -1L
  private var stopTime = -1L
  private var running = false

  def start(): Stopwatch = {
    startTime = System.currentTimeMillis()
    running = true
    this
  }

  def stop(): Stopwatch = {
    stopTime = System.currentTimeMillis()
    running = false
    this 
  }

  def isRunning(): Boolean = running

  def getElapsedTime() = {
    if (startTime == -1) {
      0
    }
    if (running) {
      System.currentTimeMillis() - startTime
    }
    else {
      stopTime - startTime
    }
  }

  def reset() {
    startTime = -1
    stopTime = -1
    running = false
  }
}

class BloomFilter1(numBitsPerElement: Double, expectedSize: Int, numHashes: Int) 
  extends AnyRef with Serializable {

  val SEED = System.getProperty("shark.bloomfilter.seed","1234567890").toInt
  val bitSetSize = math.ceil(numBitsPerElement * expectedSize).toInt
  val bitSet = new BitSet(bitSetSize)

  /**
   * @param fpp is the expected false positive probability.
   * @param expectedSize is the number of elements to be contained.
   */
  def this(fpp: Double, expectedSize: Int) {
   this(BloomFilter.numBits(fpp, expectedSize), 
       expectedSize, 
       BloomFilter.numHashes(fpp, expectedSize))
  }

  /**
   * @param data is the bytes to be hashed.
   */
  def add(data: Array[Byte]) {
    val hashes = hash(data, numHashes)
    var i = hashes.size
    while (i > 0) {
      i -= 1
      bitSet.set(hashes(i) % bitSetSize, true)
    }
  }

  /**
   * Optimization to allow reusing the same input buffer by specifying
   * the length of the buffer that contains the bytes to be hashed.
   * @param data is the bytes to be hashed.
   * @param len is the length of the buffer to examine.
   */
  def add(data: Array[Byte], len: Int) {
    val hashes = hash(data, numHashes, len)
    var i = hashes.size
    while (i > 0) {
      i -= 1
      bitSet.set(hashes(i) % bitSetSize, true)
    }
  }

  def add(data: String, charset: Charset=Charset.forName("UTF-8")) {
    add(data.getBytes(charset))
  }

  def add(data: Int) {
    add(Ints.toByteArray(data))
  }
  
  def add(data: Long) {
    add(Longs.toByteArray(data))
  }

  def contains(data: String, charset: Charset=Charset.forName("UTF-8")): Boolean = {
    contains(data.getBytes(charset))
  }

  def contains(data: Int): Boolean = {
    contains(Ints.toByteArray(data))
  }
  
  def contains(data: Long): Boolean = {
    contains(Longs.toByteArray(data))
  }

  def contains(data: Array[Byte]): Boolean = {
    !hash(data,numHashes).exists {
      h => !bitSet.get(h % bitSetSize)
    } 
  }
  
  /**
   * Optimization to allow reusing the same input buffer by specifying
   * the length of the buffer that contains the bytes to be hashed.
   * @param data is the bytes to be hashed.
   * @param len is the length of the buffer to examine.
   * @return true with some false positive probability and false if the
   *         bytes is not contained in the bloom filter.
   */
  def contains(data: Array[Byte], len: Int): Boolean = {
    !hash(data,numHashes, len).exists {
      h => !bitSet.get(h % bitSetSize)
    } 
  }

  private def hash(data: Array[Byte], n: Int): Seq[Int] = {
    hash(data, n, data.length)
  }

  private def hash(data: Array[Byte], n: Int, len: Int): Seq[Int] = {
    val s = n >> 2
    val a = new Array[Int](n)
    var i = 0
    val results = new Array[Int](4)
    while (i < s) {
      MurmurHash3_x86_128.hash(data, SEED + i, len, results)
      a(i) = results(0).abs
      var j = i + 1
      if (j < n) {
        a(j) = results(1).abs
      }
      j += 1
      if (j < n) {
        a(j) = results(2).abs
      }
      j += 1
      if (j < n) {
        a(j) = results(3).abs
      }
      i += 1
    }
    a
  }
}

object BloomFilter {
  
  def numBits(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))) / log(2)
  
  def numHashes(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))).toInt

}
class HashState(var h1: Int, var h2: Int, var h3: Int, var h4: Int) {
  
  val C1 = 0x239b961b
  val C2 = 0xab0e9789
  val C3 = 0x38b34ae5 
  val C4 = 0xa1e38b93

  @inline final def blockMix(k1: Int, k2: Int, k3: Int, k4: Int) {
    h1 ^= selfMixK1(k1)
    h1 = rotl(h1, 19); h1 += h2; h1 = h1 * 5 + 0x561ccd1b
    h2 ^= selfMixK2(k2)
    h2 = rotl(h2, 17); h2 += h3; h2 = h2 * 5 + 0x0bcaa747
    h3 ^= selfMixK3(k3)
    h3 = rotl(h3, 15); h3 += h4; h3 = h3 * 5 + 0x96cd1c35
    h4 ^= selfMixK4(k4)
    h4 = rotl(h4, 13); h4 += h1; h4 = h4 * 5 + 0x32ac3b17
  }

  @inline final def finalMix(k1: Int, k2: Int, k3: Int, k4: Int, len: Int) {
    h1 ^= (if (k1 ==0) 0 else selfMixK1(k1))
    h2 ^= (if (k2 ==0) 0 else selfMixK2(k2))
    h3 ^= (if (k3 ==0) 0 else selfMixK3(k3))
    h4 ^= (if (k4 ==0) 0 else selfMixK4(k4))
    h1 ^= len; h2 ^= len; h3 ^= len; h4 ^= len

    h1 += h2; h1 += h3; h1 += h4
    h2 += h1; h3 += h1; h4 += h1

    h1 = fmix(h1)
    h2 = fmix(h2)
    h3 = fmix(h3)
    h4 = fmix(h4)

    h1 += h2; h1 += h3; h1 += h4
    h2 += h1; h3 += h1; h4 += h1
  }

  @inline final def fmix(hash: Int): Int = {
    var h = hash
    h ^= h >> 16
    h *= 0x85ebca6b
    h ^= h >> 13
    h *= 0xc2b2ae35
    h ^= h >> 16
    h
  }

  @inline final def selfMixK1(k: Int): Int = {
    var k1 = k; k1 *= C1; k1 = rotl(k1, 15); k1 *= C2
    k1
  }

  @inline final def selfMixK2(k: Int): Int = {
    var k2 = k; k2 *= C2; k2 = rotl(k2, 16); k2 *= C3
    k2
  }

  @inline final def selfMixK3(k: Int): Int = {
    var k3 = k; k3 *= C3; k3 = rotl(k3, 17); k3 *= C4
    k3
  }

  @inline final def selfMixK4(k: Int): Int = {
    var k4 = k; k4 *= C4; k4 = rotl(k4, 18); k4 *= C1
    k4
  }
}
object MurmurHash3_x86_128 {

  /**
   * @param data is the bytes to be hashed.
   * @param seed is the seed for the murmurhash algorithm.
   */
  @inline final def hash(data: Array[Byte], seed: Int)
  : Array[Int]  = {
    hash(data, seed, data.length)
  }

  /**
   * An optimization for reusing memory under large number of hash calls.
   * @param data is the bytes to be hashed.
   * @param seed is the seed for the murmurhash algorithm.
   * @param length is the length of the buffer to use for hashing.
   * @param results is the output buffer to store the four ints that are returned,
   *        should have size at least 4.
   */
  @inline final def hash(data: Array[Byte], seed: Int, length: Int,
      results: Array[Int]): Unit = {
     var i = 0
    val blocks = length >> 4
    val state = new HashState(seed, seed, seed, seed)
    while (i < blocks) {
      val k1 = getInt(data, 4*i, 4)
      val k2 = getInt(data, 4*i + 4, 4)
      val k3 = getInt(data, 4*i + 8, 4)
      val k4 = getInt(data, 4*i + 12, 4)
      state.blockMix(k1, k2, k3, k4)
      i += 1
    }
    var k1, k2, k3, k4 = 0
    val tail = blocks * 16
    val rem = length - tail
    // atmost 15 bytes remain
    rem match {
      case 12 | 13 | 14 | 15 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, 4)
        k3 = getInt(data, tail + 8, 4)
        k4 = getInt(data, tail + 12, rem - 12)
      }
      case 8 | 9 | 10 | 11 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, 4)
        k3 = getInt(data, tail + 8, rem - 8)
      }
      case 4 | 5 | 6 | 7 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, rem - 4)
      }
      case 0 | 1 | 2 | 3 => {
        k1 = getInt(data, tail, rem)
      }
    }
    state.finalMix(k1, k2, k3, k4, length)
    results(0) = state.h1
    results(1) = state.h2
    results(2) = state.h3
    results(3) = state.h4
  }

  /**
   * An optimization for reusing memory under large number of hash calls.
   * @param data is the bytes to be hashed.
   * @param seed is the seed for the murmurhash algorithm.
   * @param length is the length of the buffer to use for hashing.
   * @return is an array of size 4 that holds the four ints that comprise the 128 bit hash.
   */
  @inline final def hash(data: Array[Byte], seed: Int, length: Int)
  : Array[Int] = {
    val results = new Array[Int](4)
    hash(data, seed, length, results)
    results
  }
  
  /**
   * Utility function to convert a byte array into an int, filling in zeros
   * if the byte array is not big enough.
   * @param data is the byte array to be converted to an int.
   * @param index is the starting index in the byte array.
   * @param rem is the remainder of the byte array to examine.
   */
  @inline final def getInt(data: Array[Byte], index: Int, rem: Int): Int = {
    rem match {
      case 3 => data(index) << 24 | 
                (data(index + 1) & 0xFF) << 16 |
                (data(index + 2) & 0xFF) << 8
      case 2 => data(index) << 24 | 
                (data(index + 1) & 0xFF) << 16
      case 1 => data(index) << 24
      case 0 => 0
      case _ => data(index) << 24 | 
                (data(index + 1) & 0xFF) << 16 |
                (data(index + 2) & 0xFF) << 8 |
                (data(index + 3) & 0xFF)
    }
  }
}


object  Apriori {




  /*def combos( a:String ): List[String]  = {
      var b=a.split(" ")
      var c=a.combinations(2)
      c.asScala.toList
      return c
   }*/
  def powerrep2(s:List[scala.collection.Set[String]], l:Int):Set[Set[String]]={
             var pp2=ListBuffer[Set[String]]()
             var p=s
             val l1=(l-1)
             s map { left => {
            	 p map { right => {
            	    if((left.intersect(right)).size==l1-1){
            	    	var p=(left.union(right))
            	    	var pp=((p.toList).sorted).toSet
            	    	var pp1=(l1 to l1).flatMap(pp.toList.combinations).map(_.toSet).toList
            	    	if(pp1.intersect(s)==pp1) pp2=(pp2.+=(pp.toSet))
            	    }
            	 } }
            	 p=p.drop(1)
             } }
             var pp3=Set[Set[String]]()
             pp3=pp2.toSet
             return pp3
  } 
  def powerrep3(s:org.apache.spark.rdd.RDD[scala.collection.Set[String]], l:Int):Set[Set[String]]={
             var pp2=ListBuffer[Set[String]]()
             var p=s
             val l1=(l-1)
             s map { left => {
            	 p map { right => {
            	    println("left of rdd is"+left)
            	    println("right of rdd is---"+right)
            	    if((left.intersect(right)).size==l1-1){
            	    	var p=(left.union(right))
            	    	var pp=((p.toList).sorted).toSet
            	    	var pp1=(l1 to l1).flatMap(pp.toList.combinations).map(_.toSet).toList
            	    	//if(pp1.intersect(s)==pp1) pp2=(pp2.+=(pp.toSet))
            	    }
            	 } }
            	 //p=p.drop(1)
             } }
             var pp3=Set[Set[String]]()
             pp3=pp2.toSet
             return pp3
  } 
 
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Apriori");
//.set("spark.executor.memory", "6g")
      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

//      conf.setMaster("local[*]");

    val sc= new SparkContext(conf)
    val lines = sc.textFile(args(0))
    val minsup:Int=args(1).toInt

    def h(k:String , v:Int)=if (v>minsup) Some(k) else None
    def h1(k:scala.collection.Set[String] , v:Int)=if (v>minsup) Some(k) else None
    def h2(k:Set[Int] , v:Int)=if (v>minsup) Some(k) else None
    val broadcastVar3=sc.broadcast(lines)
    val line = (broadcastVar3.value).flatMap(_.split("\n"))
    val broadcastVar33= sc.broadcast(line)
    val wordss=line.map(_.split(" ").toList.sorted.toSet)
    val broadcastVar333= sc.broadcast(wordss)
    val timer = new Stopwatch().start
    val words=line.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val check=wordCounts.flatMap{case(k,v)=>h(k,v)}
    val list1 = Nil
    val check8=check.collect()
    //println(check8)
    //for (a<- check8){
      //println (a)
    //}
    val checkg=check8.toSeq
    val check9=checkg.sorted
    timer.stop
    val elapsedTime1110 = timer.stop.getElapsedTime
    println("elapsed time for step1-----------------------"+elapsedTime1110)

    val broadcastVar=sc.broadcast(check9)
    println("step1 complete-------------------------------------------------------------------------------")
    timer.reset
    timer.start
   	val words2=for{ s<-(broadcastVar33.value).map(_.split(" ").toSet);s1=s.toSeq.intersect(broadcastVar.value);s2=(2 to 2).flatMap(s1.combinations).map(_.toSet).toSet}yield s2
   	//val words3=for {s1<-words2;s2=(2 to 2).flatMap(s1.combinations).map(_.toSet).toSet}yield s2
   	val wordCounts1 = words2.flatMap(x=>x.map(x => (x, 1))).reduceByKey(_ + _)
   	val check2=wordCounts1.flatMap{case(k,v)=>h1(k,v)}
    println("2pairrsult-------------")
    val check11=check2.collect()
    var check12=check11.toList
    check12=check12.map(_.toList.sorted.toSet)
    val broadcastVar11=sc.broadcast(check12)
   	//for (a<- check2){
   	//	println(a)
   	//}
    println("step 2 complete---------------------------------------------------------------")
    timer.stop
    val elapsedTime110 = timer.stop.getElapsedTime
    println("elapsed time for step2-----------------------"+elapsedTime110)
    timer.reset
    timer.start
    println("powerrep-3 start here-------------------------------------------")
    var cc1=powerrep2(check12,3)
    //println("powerep output is----------------------------------"+cc1)
    println("powerep output length is----------------------------------"+cc1.size)
    timer.stop
    val elapsedTime1 = timer.stop.getElapsedTime
    println("elapsed time for old powerset3-------------------------------"+elapsedTime1)
    val bloom4 = new BloomFilter1(80, 1000,5)
    for(a<-check12)
    	  bloom4.add(a.toString)
    def powerrep222(s:List[scala.collection.Set[String]], l:Int):Set[Set[String]]={
             var pp2=ListBuffer[Set[String]]()
             var p=s
             val l1=(l-1)
             s map { left => {
            	 p map { right => {
            	    if((left.intersect(right)).size==l1-1){
            	    	var p1=(left.union(right))
            	    	var pp=((p1.toList).sorted).toSet
            	    	var pp1=(l1 to l1).flatMap(pp.toList.combinations).map(_.sorted.toSet).toList
            	    	//if(pp1.intersect(s)==pp1) pp2=(pp2.+=(pp.toSet))
            	    	var j:Int=0
            	    	pp1 map{leee=>{
            	    	  if(bloom4.contains(leee.toString)) j=j+1
            	    	}

            	    	}
            	    	if(j==l) pp2=(pp2.+=(pp.toSet))
            	    }
            	 } }
            	 p=p.drop(1)
             } }
             var pp3=Set[Set[String]]()
             pp3=pp2.toSet
             return pp3
    }
    timer.reset
    timer.start
    var check22=powerrep222(check12, 3)
    println("length of powerep new is -----------------"+check22.size)
    timer.stop
    val elapsedTime8g = timer.stop.getElapsedTime
    println("elapsed time for powerrep new-------------------------------"+elapsedTime8g)
    var check13=cc1
    val lap=3
    var broadcastVar1=sc.broadcast(check13)
   	timer.reset
    timer.start
    var hashtree1=new HashTreeUtils1()
    var check12h1=(broadcastVar1.value).map(_.map(_.toInt)).toList
    var hah1=hashtree1.buildHashTree(check12h1, lap)
    val s5=List[Set[Int]]()
   	val words3s2=for{s1<-(broadcastVar333.value);s3=if(s1.size>=lap) s1.map(_.toInt) else Set(1,2,3,4);s2<- if(s1.size>=lap) hashtree1.findItemsets(hah1, s3, 1) else s5  } yield s2
   	val wordCountsg2 = words3s2.map(x => (x, 1)).reduceByKey(_ + _)
    val check3=wordCountsg2.flatMap{case(k,v)=>h2(k,v)}
    println("result for"+lap+"-pairrsult-------------")
    for (a<- check3){}
    	//println(a)
    //}
    val check15=check3.collect()
    println("step-"+lap+"ends here--------------------------------------------------------------------")
    println("size of step" +lap+" results --------------------------------------------"+check3.count)
    timer.stop
    val elapsedTime6b = timer.stop.getElapsedTime
    println("elapsed time for step-- "+lap+"-is-------------------------------"+elapsedTime6b)
    timer.reset
    timer.start
    var check12g=check15.toList
    check12g=check12g.map(_.toList.sorted.toSet)
    val bloom1 = new BloomFilter1(80, 1000,5)
    for(a<-check12g)
      bloom1.add(a.toString)
    def powerrep22(s:List[Set[Int]], l:Int):Set[Set[Int]]={
             var pp2=ListBuffer[Set[Int]]()
             var p=s
             val l1=(l-1)
             s map { left => {
            	 p map { right => {
            	    if((left.intersect(right)).size==l1-1){
            	    	var p=(left.union(right))
            	    	var pp=((p.toList).sorted).toSet
            	    	var pp1=(l1 to l1).flatMap(pp.toList.combinations).map(_.sorted.toSet).toList
            	    	//if(pp1.intersect(s)==pp1) pp2=(pp2.+=(pp.toSet))
            	    	var j:Int=0
            	    	pp1 map{leee=>{
            	    	  if(bloom1.contains(leee.toString)) j=j+1
            	    	}

            	    	}
            	    	if(j==l) pp2=(pp2.+=(pp.toSet))
            	    }
            	 } }
            	 p=p.drop(1)
             } }
             var pp3=Set[Set[Int]]()
             pp3=pp2.toSet
             return pp3
    }

    var check22g=powerrep22(check12g,lap+1)
    println("size of new powerrep is------------"+check22g.size)
    var check13g=check22g.toSet
    if(check22g.isEmpty) println("empty powerset --------------------------------------------------")
    println("powerset for step "+lap+" ends------------------------------------------")
    timer.stop
    val elapsedTime8 = timer.stop.getElapsedTime
    println("elapsed time for powerset is-------------------------------"+elapsedTime8)
    println("count--"+lap+" ends here---------------------------------------------------------")
    if(check3.count<2) break
    for (lap<-4 to 9){
        var broadcastVarg=sc.broadcast(check13g)
   	    timer.reset
    	timer.start
    	var hashtree1=new HashTreeUtils1()
    	var check12h1=(broadcastVarg.value).map(_.map(_.toInt)).toList
        var hah1=hashtree1.buildHashTree(check12h1, lap)
        val s5=List[Set[Int]]()
   	    val words3s2=for{s1<-(broadcastVar333.value);s3=if(s1.size>=lap) s1.map(_.toInt) else Set(1,2,3,4);s2<- if(s1.size>=lap) hashtree1.findItemsets(hah1, s3, 1) else s5  } yield s2
   	    val wordCountsg2 = words3s2.map(x => (x, 1)).reduceByKey(_ + _)
    	val check3=wordCountsg2.flatMap{case(k,v)=>h2(k,v)}
    	println("result for"+lap+"-pairrsult-------------")
    	for (a<- check3){
    		//println(a)
    	}
    	val check15=check3.collect()
    	println("step-"+lap+"ends here--------------------------------------------------------------------")
    	println("size of step" +lap+" results --------------------------------------------"+check3.count)
    	timer.stop
    	val elapsedTime6b = timer.stop.getElapsedTime
    	println("elapsed time for step-- "+lap+"-is-------------------------------"+elapsedTime6b)
    	timer.reset
    	timer.start
    	var check12g=check15.toList
    	check12g=check12g.map(_.toList.sorted.toSet)
    	val bloom1 = new BloomFilter1(80, 1000,5)
    	for(a<-check12g)
    	  bloom1.add(a.toString)
    	def powerrep22(s:List[Set[Int]], l:Int):Set[Set[Int]]={
             var pp2=ListBuffer[Set[Int]]()
             var p=s
             val l1=(l-1)
             s map { left => {
            	 p map { right => {
            	    if((left.intersect(right)).size==l1-1){
            	    	var p=(left.union(right))
            	    	var pp=((p.toList).sorted).toSet
            	    	var pp1=(l1 to l1).flatMap(pp.toList.combinations).map(_.sorted.toSet).toList
            	    	//if(pp1.intersect(s)==pp1) pp2=(pp2.+=(pp.toSet))
            	    	var j:Int=0
            	    	pp1 map{leee=>{
            	    	  if(bloom1.contains(leee.toString)) j=j+1
            	    	}

            	    	}
            	    	if(j==l) pp2=(pp2.+=(pp.toSet))
            	    }
            	 } }
            	 p=p.drop(1)
             } }
             var pp3=Set[Set[Int]]()
             pp3=pp2.toSet
             return pp3
    	}

    	var check22g=powerrep22(check12g,lap+1)
    	println("size of new powerrep is------------"+check22g.size)
    	check13g=check22g.toSet
    	if(check22g.isEmpty) println("empty powerset --------------------------------------------------")
        println("powerset for step "+lap+" ends------------------------------------------")
        timer.stop
        val elapsedTime8 = timer.stop.getElapsedTime
    	println("elapsed time for powerset-------------------------------"+elapsedTime8)
        if(check3.count<2) break
    }

  }
}

