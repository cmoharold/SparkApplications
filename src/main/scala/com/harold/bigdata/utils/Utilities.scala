package com.harold.bigdata.utils

import java.io.{FileNotFoundException, IOException}
import java.util.regex.Pattern
import org.apache.log4j.{Level, Logger}
import scala.util.{Failure, Success, Try}

import scala.io.Source

object Utilities {
  /** Makes sure only ERROR messages get logged to avoid log spam. */
  def setupLogging() = {
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
  def setupTwitter() = {
/*
    for (line <- Source.fromFile("../../../../../resources/twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }*/

    val filename = "twitter.txt"/*
    Try(Source.fromResource("twitter.txt").getLines()) match {
      case Success(lines) => // handling lines
      case Failure(e) => println(s"An error has occured, cause: $e")
    }*/
    //if(Source.fromResource(filename).getLines.foreach(println) == null) println("No se ha encontrado el archivo")
    try {
      for (line <- Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(filename)).getLines) {
      //for (line <- Source.fromResource(filename).getLines) {
        val fields = line.split(" ")
        if (fields.length == 2) {
          System.setProperty("twitter4j.oauth." + fields(0), fields(1))
        }
      }
    }
    catch
    {
      case e: NullPointerException => println("File not found " + e.getMessage)
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Got an IOException!")
    }
  }


  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
}