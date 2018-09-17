#note that this was written to run in zeppelin which runs, as best I can tell, in the pyspark terminal.
import justext
import time
path = "/share_dir/sample_output/html_included/*.json"
htmlDF = spark.read.json(path)
def HtmlClean(rawtext):
    paragraphs = justext.justext(rawtext,justext.get_stoplist("English"))
    return ' '.join([p.text for p in paragraphs if not p.is_boilerplate])
#makes a test dataframe with 2^(7+1) * 350 entries for timing, can change the 7 in the for loop:
testDF = htmlDF.union(htmlDF)
for x in range(7):
    testDF = testDF.union(testDF)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
pysparkClean = udf(HtmlClean, StringType())
start = time.time()
pysparkClean = udf(HtmlClean, StringType())
cleanDF = testDF.withColumn('cleantext',pysparkClean(testDF.text)).drop(testDF.text).withColumnRenamed('cleantext','text')
cleanDF.show()
end = time.time()
print end - start
#exports the text:
cleanDF.coalesce(1).write.format('json').save('/share_dir/sample_output/cleaned/cleanRecordsdups.json')