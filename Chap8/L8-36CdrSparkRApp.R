args <- commandArgs(trailingOnly = TRUE)
if(length(args) != 2) {
    stop("Usage: CdrSparkRApp <master> <filepath>")
}
library(SparkR)
Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.3.0" "sparkr-shell"')
sc <- sparkR.init(master = args[1])
sqlContext <- sparkRSQL.init(sc)
df <- read.df(sqlContext, args[2], source = "com.databricks.spark.csv", inferSchema = "true", delimiter = "\t")
cnames <- c("squareId", "timeInterval", "countryCode", "smsInActivity", "smsOutActivity", "callInActivity", "callOutActivity", "internetTrafficActivity") 
for (i in 1:NROW(cnames)) {
    df <- withColumnRenamed(df, paste0("C", i - 1), cnames[i])
}
counts <- count(groupBy(df, "countryCode"))
showDF(orderBy(counts, desc(counts$count)), numRows = 5)
sparkR.stop()