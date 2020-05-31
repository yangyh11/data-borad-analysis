package com.offline.job.babel.log.common

import com.offline.job.babel.log.{LogParser, ParseParams}
import org.apache.spark.sql.SparkSession
import org.springframework.stereotype.Component

@Component
class UvCount extends LogParser {
    override def run(params: ParseParams, sparkSession: SparkSession): Unit = {
        //业务逻辑.
        val sc = sparkSession.sparkContext

        val words = sc.parallelize(List("hello", "java", "spark", "flink", "java", "python"))
        words.map(word => (word,1)).reduceByKey(_+_).foreach(println)
        Console.println("UvCount执行成功")
    }
}