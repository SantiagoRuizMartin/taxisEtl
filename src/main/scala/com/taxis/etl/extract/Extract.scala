package com.taxis.etl.extract

object Extract {
    def getFilenames(): Array[String] = {
        def folder: String = ReadFromEnv.readDownloadFolder()
        ReadFromEnv.readTaxisETLFileName().map((x: String) => folder + "/" + x)
    }

    def getDownloadedFilenames(): Array[String] = {
        if(ReadFromEnv.readDownloadFiles()) {
            Download.downloadFiles()
        }
        getFilenames()
    }
}
