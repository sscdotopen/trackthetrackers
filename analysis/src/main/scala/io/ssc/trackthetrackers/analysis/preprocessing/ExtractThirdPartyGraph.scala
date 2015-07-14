package io.ssc.trackthetrackers.analysis.preprocessing

import java.io.{FileWriter, BufferedWriter}

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap

import scala.io.Source


object ExtractThirdPartyGraph extends App {


  val paylevelDomainIndexFile = "/home/ssc/Entwicklung/datasets/thirdparty/pld-index.orig"
  val extractedThirdpartiesFile = "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty.tsv"

  val paylevelDomainIndexAdditionsFile = "/home/ssc/Entwicklung/datasets/thirdparty/pld-index-additions"
  val thirdPartyGraphFile = "/home/ssc/Entwicklung/datasets/thirdparty/thirdparty-graph.tsv"

  val paylevelDomainIndex = new Object2IntOpenHashMap[String](45000000)

  println("Reading pld-index into memory")
  var linesProcessed = 0
  Source.fromFile(paylevelDomainIndexFile).getLines().foreach { line =>
    val tokens = line.split("\t")
    paylevelDomainIndex.put(tokens(0), tokens(1).toInt)
    linesProcessed += 1
    if (linesProcessed % 1000000 == 0) {
      println(linesProcessed + " lines processed from pld-index file...")
    }
  }
  println(linesProcessed + " lines processed from pld-index file...done.")

  var lastPaylevelDomainIndex = 42889799
  val thirdPartyGraphWriter =  new BufferedWriter(new FileWriter(thirdPartyGraphFile))
  val additionalPaylevelDomainsIndexWriter =  new BufferedWriter(new FileWriter(paylevelDomainIndexAdditionsFile))

  linesProcessed = 0
  Source.fromFile(extractedThirdpartiesFile).getLines().foreach { line =>

    val tokens = line.split("\t")

    val domain = tokens(0)
    if (!paylevelDomainIndex.containsKey(domain)) {
      lastPaylevelDomainIndex += 1
      additionalPaylevelDomainsIndexWriter.write(domain)
      additionalPaylevelDomainsIndexWriter.write("\t")
      additionalPaylevelDomainsIndexWriter.write(lastPaylevelDomainIndex.toString)
      additionalPaylevelDomainsIndexWriter.newLine()
      paylevelDomainIndex.put(domain, lastPaylevelDomainIndex)
    }

    for (n <- 2 until tokens.length) {
      val parts = tokens(n).replaceAll("\\[", "").replaceAll("\\]", "").split(",")

      val thirdPartyDomain = parts(0)
      if (!paylevelDomainIndex.containsKey(thirdPartyDomain)) {
        lastPaylevelDomainIndex += 1
        additionalPaylevelDomainsIndexWriter.write(thirdPartyDomain)
        additionalPaylevelDomainsIndexWriter.write("\t")
        additionalPaylevelDomainsIndexWriter.write(lastPaylevelDomainIndex.toString)
        additionalPaylevelDomainsIndexWriter.newLine()
        paylevelDomainIndex.put(thirdPartyDomain, lastPaylevelDomainIndex)
      }

      thirdPartyGraphWriter.write(paylevelDomainIndex.getInt(domain).toString)
      thirdPartyGraphWriter.write("\t")
      thirdPartyGraphWriter.write(paylevelDomainIndex.getInt(thirdPartyDomain).toString)
      thirdPartyGraphWriter.newLine()
    }

    linesProcessed += 1
    if (linesProcessed % 1000000 == 0) {
      println(linesProcessed + " lines processed from thirdparty file...")
    }
  }

  thirdPartyGraphWriter.close()
  additionalPaylevelDomainsIndexWriter.close()

  // cp pld-index pld-index.orig
  // cat pld-index-additions >> pld-index

}
