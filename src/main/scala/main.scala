import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.SentenceDetector
import org.apache.hadoop.shaded.org.checkerframework.checker.regex.qual.Regex
import org.apache.spark.ml.Pipeline

import scala.reflect.internal.util.TriState.True
import org.apache.spark.sql.functions._

object HelloWorld {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("HelloWorld")
      .master("local[*]")
      .getOrCreate()


    implicit val session = spark

    val path = "/home/jarek/Pobrane/test/zz.html"

    // Tworzymy schemat dla DataFrame
    val schema = StructType(Array(
      StructField("filename", StringType, true),
      StructField("text", StringType, true)
    ))


    val data: RDD[(String, String)] = spark.sparkContext.wholeTextFiles(path)


    val rowRDD = data.map(attributes => Row(attributes._1, attributes._2))


    val df = spark.createDataFrame(rowRDD, schema)


    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val inputColName: String = "document"
    val outputColName: String = "normalizedDocument"
    val action = "clean"
    val cleanUpPatterns = Array( //opinion-list, product-reviews
      "(?s)<div\\s+id=\"product-opinions\"[^>]*>.*?</div>",
      "(?s)<div\\s+class=\"product-related\"[^>]*>.*?</div>",
      "(?s)<div\\s+class=\"product-reviews\"[^>]*>.*?</div>",
      "(?s)<div\\s+class=\"opinion\"[^>]*>.*?</div>",
      "(?s)<div\\s+class=\"review\"[^>]*>.*?</div>",
      "(?s)<div\\s+class=\"rating\"[^>]*>.*?</div>",
      "(?s)<div\\s+class=\"opinion-list\"[^>]*>.*?</div>",
      "(?s)<div\\s+class=\"qa-item\"[^>]*>.*?</div>",
      "(?s)<footer[^>]*>.*?</footer>",
      "(?s)<script[^>]*>.*?</script>",
      "(?s)<style[^>]*>.*?</style>",
      "(?s)<head[^>]*>.*?</head>",
      "<[^>]*>"
    )
    val replacement = " "
    val removalPolicy = "pretty_all"
    val encoding = "UTF-8"

    val documentNormalizer = new DocumentNormalizer()
      .setInputCols(inputColName)
      .setOutputCol(outputColName)
      .setAction(action)
      .setPatterns(cleanUpPatterns)
      .setReplacement(replacement)
      .setPolicy(removalPolicy)
      .setEncoding(encoding)

    val sentenceDetector = new SentenceDetector()
      .setInputCols(Array("normalizedDocument"))
      .setOutputCol("sentence")


    val regexTokenizer = new Tokenizer()
      .setInputCols(Array("sentence"))
      .setOutputCol("token")

    val docPatternRemoverPipeline = new Pipeline()
      .setStages(Array(
        documentAssembler,
        documentNormalizer,
        sentenceDetector,
        regexTokenizer
      ))


    val ds = docPatternRemoverPipeline.fit(df).transform(df)


    val dsWithLength = ds.selectExpr("normalizedDocument.result as normalizedText")
      .withColumn("normalizedText", concat_ws(" ", col("normalizedText")))
      .withColumn("textLength", length(col("normalizedText")))

    dsWithLength.select("normalizedText", "textLength")
      .write
      .option("header", "true")
      .csv("/home/jarek/Pobrane/test/processed_output.csv")

  }
}

