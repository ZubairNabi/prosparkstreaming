#!/usr/local/bin/Rscript
args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 1) {
    stop("Usage: CdrStreamingSparkRApp <master>")
}
library(SparkR)
sc <- sparkR.init(master = args[1])
hiveContext <- sparkRHive.init(sc)
f <- file("stdin")
open(f)
while(length(tableName <- readLines(f, n = 1)) > 0) {
    tryCatch({
        tableName <- trimws(tableName)
        write(paste0("Processing table: ", tableName), stderr())
        df <- table(hiveContext, tableName)
        counts <- count(groupBy(df, "countryCode"))
        outputTable <- paste0(tableName, "processed")
        write(paste0("Output written to: ", outputTable), stderr())
        saveAsTable(limit(orderBy(counts, desc(counts$count)), 5), outputTable, "parquet", "error")
    }, error = function(e) {stop(e)})
}
close(f)
sparkR.stop()