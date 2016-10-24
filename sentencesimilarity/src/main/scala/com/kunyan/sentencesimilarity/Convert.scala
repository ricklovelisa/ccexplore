package com.kunyan.sentencesimilarity

import breeze.linalg._
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
}
