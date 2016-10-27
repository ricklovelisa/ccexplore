package com.kunyan.sentencesimilarity

import breeze.linalg._
import com.kunyandata.nlpsuit.util.{KunyanConf, TextPreprocessing}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.io.Source

/**
  * Created by qiuqiu on 16-10-24.
  */
object Convert {

  val kunyanConf = new KunyanConf
  kunyanConf.set("61.147.114.88", 16003)

  /**
    * 将句子转化为向量
    * @param sentence 需要转化的句子，由词组成
    * @param w2v 词向量
    * @param partition 分区数
    * @return
    */
  def sentence2Vec(sentence:RDD[(Int, Int, Vector[Double])],
                   w2v: RDD[Vector[Double]],
                   partition: Int) = {

    val result = sentence.cartesian(w2v).repartition(partition)

    result.map(line => {

      val sentensWords = line._1
      val word2Vec = line._2
      val tempCorr = 1 - functions.cosineDistance(sentensWords._3, word2Vec)

      (sentensWords._1, (sentensWords._2, tempCorr))
    })
  }

  /**
    * 获取所有词的集合
    * @param docs 所有新闻，新闻为分词之后的字符串数组
    * @return
    */
  def buildWordsBag(docs: RDD[Array[String]]) = {

    docs.flatMap(x => x).distinct().collect()
  }

  /**
    * 获取词袋和词向量中的词的交集
    * @param wordsBags 词袋
    * @param word2Vec 词向量中的词
    * @return
    */
  def getCoWordsBetweenWordsBagAndWord2Vec(wordsBags: Array[String], word2Vec: Array[String]) = {

    wordsBags.toSet.intersect(word2Vec.toSet).toArray
  }

//  /**
//    * 获取词向量中的所有词
//    * @param word2Vec
//    */
//  def getWordVec(word2Vec: Array[(String, String)]) = {
//
//    word2Vec
//  }

  /**
    * 将句子或者任意的文本片段转化为(docid, word_index, word)的形式
    * @param docs 新闻
    * @param wordBucketBr 词袋和词向量的交集
    */
  def transformDoc(docs: RDD[Array[String]], wordBucketBr: Broadcast[Array[String]]) = {

    docs.zipWithUniqueId().flatMap(doc => {

      val docID = doc._2

      doc._1.zipWithIndex.map(wordAndIndex => {

        val word = wordAndIndex._1
        val index = wordAndIndex._2
        if (wordBucketBr.value.contains(word)) {

          (docID, index, word)
        } else {

          (docID, index, "null")
        }
      })
    })
  }

  /**
    * 过滤掉无效的词
    * @param word 分词结果中的每一个词
    * @return
    */
  def wordFilter(word: String): Boolean = {

    var temp = word.replaceAll("[0-9]", "")
    // 判断是否为纯数字
    if (temp.length == 0) {

      return false
    }

    temp = temp.replaceAll("[a-zA-Z]", "")
    // 判断是否为英文和数字的组合
    if (temp.length == 0) {

      return false
    }

    temp = temp.replaceAll("\\.", "")
    if (temp.length == 0) {

      return false
    }

    true
  }

  /**
    * 一次性获取所有数据
    * @return
    */
  def getAllData = {

    val word2Vec = Source
      .fromFile("/home/qiuqiu/working/sentSim/input/word2vec.result")
      .getLines()
      .toArray
      .map(line => {
        val temp = line.replaceAll("[()]", "")
        temp.split(",")
      })

    val stopWords = Source
      .fromFile("/home/qiuqiu/working/topicdnt/dicts/stopWordsCN")
      .getLines()
      .toArray

    val docs = Source
      .fromFile("/home/qiuqiu/working/sentSim/input/docs.result")
      .getLines()
      .toArray
      .map(line => {
        TextPreprocessing.process(line, stopWords, kunyanConf).filter(wordFilter)
      })

    Array(docs, word2Vec)
  }

  /**
    * 计算余弦相似度（vector形式）
    *
    * @param vec1 词向量
    * @param vec2 词向量
    * @return 余弦相似度的值
    */
  def cosine(vec1: Vector[Double], vec2: Vector[Double]): Double = {

    val fenzi = vec1 dot vec2
    val fenmu = norm(vec1) * norm(vec2)

    fenzi / fenmu
  }

  /**
    * 主函数
    * @param args 外部传入变量
    */
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SENTENCE_SIM")
      .set("dfs.replication", "1")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // 获取新闻和词向量数据
    val Array(docs, word2Vec) = getAllData

    println(1,docs.map(_.toSeq).toSeq)
    println(2,word2Vec.map(_(0)).toSeq)

    val docsRDD = sc.parallelize(docs)
    val word2VecRDD = sc.parallelize(word2Vec)

    // 构建所有文本的词袋
    val wordsBag = buildWordsBag(docsRDD)
    // 计算词袋和word2vec中的词的交集
    val coWordsBucket = getCoWordsBetweenWordsBagAndWord2Vec(wordsBag, word2Vec.map(_(0)))

    val coWord2Vec = word2VecRDD.filter(x => coWordsBucket.contains(x(0)))

    coWord2Vec.foreach(x => println(x.toSeq))
    val coWordsBucketBr = sc.broadcast(coWordsBucket)
    val transformedDoc = transformDoc(docsRDD, coWordsBucketBr)

//
//    transformedDoc.flatMap(wordInfo => {
//
//      val docID = wordInfo._1
//      val wordIndex = wordInfo._2
//
//      wordInfo._3.flatMap()
//    })
  }

}
