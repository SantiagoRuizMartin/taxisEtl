package com.taxis.etl.extract

object ReadFromEnv {
    def readTaxisETLFileName(separator: String = ","): Array[String] = {
        def files = sys.env.getOrElse("TAXIS_ETL_FILES", "")
        files.split(separator)
    }

    def readDownloadFolder(): String = {
        sys.env.getOrElse("TAXIS_ETL_DOWNLOAD_FOLDER", "downloads")
    }

    def readBaseUrl(): Option[String] = {
        sys.env.get("TAXIS_ETL_BASE_URL")
    }

    def readDownloadFiles(): Boolean = {
        sys.env.getOrElse("TAXIS_ETL_DOWNLOAD_FILES", "false").eq("true")
    }
}
