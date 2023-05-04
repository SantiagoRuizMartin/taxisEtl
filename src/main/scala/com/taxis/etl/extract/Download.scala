package com.taxis.etl.extract

import sys.process._
import java.net.URL
import java.io.File

import java.nio.file.{Files, Paths}
import scala.language.postfixOps

object Download {
    def fileDownloader(url: String, filename: String): String = {
        System.out.println("Downloading " + url + " to " + filename)
        new URL(url) #> new File(filename) !!
    }

    def downloadFiles(): Unit = {
        def folder: String = ReadFromEnv.readDownloadFolder()
        def baseUrl: Option[String] = ReadFromEnv.readBaseUrl()
        if(baseUrl.isDefined) {
            def path = Paths.get(folder)
            if (!(Files.exists(path) && Files.isDirectory(path)))
                Files.createDirectory(path)
            ReadFromEnv.readTaxisETLFileName().foreach((filename: String) => {
                fileDownloader(baseUrl.get + "/" + filename, folder + "/" + filename )
            })
        } else {
            throw new IllegalArgumentException("Base url was not defined")
        }
    }

    def main(args: Array[String]): Unit = {
        downloadFiles()
    }
}
