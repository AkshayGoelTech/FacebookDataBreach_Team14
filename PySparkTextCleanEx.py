#note that this was written to run in zeppelin which runs, as best I can tell, in the pyspark terminal.
import justext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
path = "/share_dir/sample_output/html_included/*.json"
htmlDF = spark.read.json(path)
def HtmlClean(rawtext):
    paragraphs = justext.justext(rawtext,justext.get_stoplist("English"))
    return ' '.join([p.text for p in paragraphs if not p.is_boilerplate])
pysparkClean = udf(HtmlClean, StringType())
cleanDF = htmlDF.withColumn('cleantext',pysparkClean(htmlDF.text)).drop(htmlDF.text).withColumnRenamed('cleantext','text')
#exports the text:
cleanDF.coalesce(1).write.format('json').save('/share_dir/sample_output/cleaned/cleanRecordsdups.json')