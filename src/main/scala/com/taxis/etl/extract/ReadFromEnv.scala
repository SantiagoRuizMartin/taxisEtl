package com.taxis.etl.extract

object ReadFromEnv {
    def readTaxisETLFileName(): Array[String] = {
        def files = sys.env.getOrElse("TAXIS_ETL_FILES", "")
        files.split(",")
    }

    def readDownloadFolder(): String = {
        sys.env.getOrElse("TAXIS_ETL_DOWNLOAD_FOLDER", "downloads")
    }

    // TODO
    def readList(): Array[String] = {
        readTaxisETLFileName().map((x: String) => readDownloadFolder() + "/" + x)
    }
}
