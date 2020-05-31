package com.offline.job.babel.log

import org.apache.spark.sql.SparkSession
import org.springframework.context.support.ClassPathXmlApplicationContext

import collection.JavaConversions._

object LogParseEngine {

    def main(args: Array[String]): Unit = {
//        val outPutPath = args(0)
//        val date = args(1)
//        val className = args(2)
        val outPutPath = "/output"
        val date = "20180412"
        val className = "UvCount"

        Console.println("outPutPath:" + outPutPath)
        Console.println("date:" + date)
        Console.println("className:" + className)

        val params = new ParseParams(className,outPutPath, date)
        val sparkSession = SparkSession
            .builder()
            .appName("LogParseEngine:" + params)
//          .master("local")
            .getOrCreate()
        val appContext = new ClassPathXmlApplicationContext("applicationContext.xml")

        val logParserFactory = appContext.getBean("logParserFactory").asInstanceOf[LogParserFactory]
            val logParsers = logParserFactory.getLogParsers()
            val parsers = logParsers.filter(e => e.name().equals(className))
            parsers.foreach(i => i.run(params, sparkSession))
    }
}