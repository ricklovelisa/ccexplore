package com.kunyan.sentencesimilarity

import breeze.linalg._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by qiuqiu on 16-10-24.
  */
object Convert {

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

  def wordsBag(docs: RDD[Array[String]]) = {

    docs.flatMap(_).distinct().collect()
  }

  def getCoWordsBetweenWordsBagAndWord2Vec(wordsBags: Array[String], word2Vec: Array[String]) = {

    wordsBags.toSet.intersect(word2Vec.toSet).toArray
  }

  def getWordVec(word2Vec: Array[String]) = {


  }

  def transformDoc(docs: RDD[Array[String]], wordBucketBr: Broadcast[Array[String]]) = {

    val k = docs.zipWithUniqueId().flatMap(doc => {

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

}
