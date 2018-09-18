import de.l3s.archivespark._
import de.l3s.archivespark.implicits._
import de.l3s.archivespark.enrich.functions._
import de.l3s.archivespark.specific.warc.implicits._
import de.l3s.archivespark.specific.warc.specs.WarcCdxHdfsSpec
import de.l3s.archivespark.specific.warc.specs.WarcHdfsSpec
import de.l3s.archivespark.specific.warc.enrichfunctions._

import java.io.{FileOutputStream, PrintStream,FileInputStream,PrintWriter}
import opennlp.tools.sentdetect.SentenceDetectorME
import opennlp.tools.sentdetect.SentenceModel
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Row, SparkSession}


// load data from local file system
val warcPath = "/user/public/Demo/"
val cdxPath = "/user/public/Demo/*.cdx"
val records = ArchiveSpark.load(WarcCdxHdfsSpec(cdxPath,warcPath))

val pages = records.filter(r => r.mime == "text/html" && r.status == 200) // extract valid webpages
val earliest = pages.distinctValue(_.surtUrl) {(a, b) => if (a.time < b.time) a else b} // filter out same urls, pick the latest snap
val Title = HtmlText.of(Html.first("title")) // Define Title enrichment within HTML body
val enriched = earliest.enrich(Html.first("body")).enrich(Title) // Enrich with Terms and Title

val result = enriched.map( r => {
    val title = r.valueOrElse(Title, "").replaceAll("[\\t\\n]", " ") // get title value
    val text = r.valueOrElse(Html.first("body"), "").replaceAll("[\\t\\n]", " ") // get text value
    // concatenate URL, timestamp, title and text with in the format of tuple, tuple can be converted to Dataframe format later
    (r.originalUrl,,title, text)
})

val result_df = result.toDF("originalUrl","title","text").filter($"title" !== "").filter($"title" !== "Error Page").filter($"title" !== "403 -Forbidden") // convert to DataFrame format inorder to export Json format

result_df.repartition(1).write.mode("overwrite").format("json").save("/user/public/Demo/sentences") // export data to your local path