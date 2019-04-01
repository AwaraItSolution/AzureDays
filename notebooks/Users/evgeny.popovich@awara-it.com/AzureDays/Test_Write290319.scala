// Databricks notebook source
// Инициализация EventHub 
import org.apache.spark.eventhubs._

// Build connection string with the above information
val connectionString = ConnectionStringBuilder("Endpoint=sb://azuredayseventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ezcpA4x6xsEDduGu96SAqteaxfqXg/opqpAPo6qYcgM=")
  .setEventHubName("eventhubdatabrics")
  .build

val customEventhubParameters =
  EventHubsConf(connectionString)
  .setMaxEventsPerTrigger(20)

// COMMAND ----------

// Чтение из EventHub 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

val schema = StructType(Seq(StructField("moment", TimestampType,false), StructField("sn", StringType,false), StructField("val", FloatType,false)))

val messages =
  incomingStream
  //.select(from_json($"body".cast(StringType), schema).as("json_str")).select($"json_str.*")
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
  .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
  .withColumn("Body", $"body".cast(StringType))
  .withColumn("Json_Struct", from_json($"Body", schema))  //, Map[String, String]()
  .withColumn("moment", $"Json_Struct.moment")
  .withColumn("sn", $"Json_Struct.sn")
  .withColumn("val", $"Json_Struct.val")
  .select("moment", "sn", "val")

messages.printSchema

// COMMAND ----------

// Тестируем чтение из EventHub на консоль
messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// Запись в delta таблицу 
messages
  .writeStream
  .outputMode("append")
  .option("checkpointLocation", "/tmp/checkpoints/chp_dl2_to_deltatable4")
//  .option("mergeSchema", "true")
  .format("delta")
  .table("events")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select max(moment) from events --group by moment --order by moment desc , moment
// MAGIC --select count(*) from events 
// MAGIC -- Контроль записи в delta таблицу входных сообщений
// MAGIC --truncate table events

// COMMAND ----------

// Запись из Delta таблицы в Data Lake
import org.apache.spark.sql.streaming.Trigger.ProcessingTime 

val dl2Set = 
spark.readStream
  .table("events")
  .writeStream
  .format("parquet")
  .option("checkpointLocation", "/mnt/checkpoints/events_to_dlake_3")
  .option("path", "/mnt/dl2/rawdata")
  .start()

// COMMAND ----------

// PowerBI II. Из parquet пишем в таблицу для чтения из PowerBI. СДЕЛАТЬ JOB 
// Не корректная схема работы с PowerBI!!! Потому, что то что попадает в Data Lake это уже исторические данные (согласно лямбда архитектуры), а мы их используем в качестве realtime данных.
spark.read.parquet("/mnt/dl2/rawdata").write.mode(SaveMode.Overwrite) saveAsTable("events_dataset")

// COMMAND ----------

val parquetFileDF = spark.read.parquet("/mnt/dl2/rawdata")

// Parquet files can also be used to create a temporary view and then used in SQL statements
//parquetFileDF.createOrReplaceTempView("part-00002-76267153-3102-4041-b3b5-ce5d6d5d422d-c000.snappy.parquet")
//val namesDF = spark.sql("SELECT moment, sn, val FROM part-00002-76267153-3102-4041-b3b5-ce5d6d5d422d-c000.snappy.parquet") // WHERE age BETWEEN 13 AND 19
//namesDF.map(attributes => "Moment: " + attributes(0)).show()

// COMMAND ----------

//dbutils.fs.rm("/mnt/dl2/rawdata/", true)
//dbutils.fs.mkdirs("/mnt/dl2/rawdata/")
dbutils.fs.ls("/mnt/dl2/rawdata/")
//dbutils.fs.help()