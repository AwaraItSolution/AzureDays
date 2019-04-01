// Databricks notebook source
// Инициализация EventHub 
import org.apache.spark.eventhubs._

// МОЯ учетка
val connectionString = ConnectionStringBuilder("Endpoint=sb://eventhubazuredays.servicebus.windows.net/;SharedAccessKeyName=sa-policy-eventhubdatabricks;SharedAccessKey=rPTVxfm6ZdL+Wv2yKZAeencRQfpbS+SNWHiESXNEvow=;EntityPath=eventhubdatabrics")
// Димина учетка
//val connectionString = ConnectionStringBuilder("Endpoint=sb://azuredayseventhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ezcpA4x6xsEDduGu96SAqteaxfqXg/opqpAPo6qYcgM=")
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
  .select("moment", "sn", "val").where("moment is not null")

messages.printSchema

// COMMAND ----------

// Тестируем чтение из EventHub на консоль
messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

// COMMAND ----------

// Запись в delta таблицу 
messages
  .writeStream
  .outputMode("append")
  .option("checkpointLocation", "/tmp/checkpoints/to_deltatable1")
//  .option("mergeSchema", "true")
  .format("delta")
  .table("events")

// COMMAND ----------

// MAGIC %sql 
// MAGIC --select max(moment) from events --group by moment --order by moment desc , moment
// MAGIC select count(*) from events 
// MAGIC -- Контроль записи в delta таблицу входных сообщений
// MAGIC --truncate table events

// COMMAND ----------

// Потребитель I.
// Запись из Delta таблицы в Data Lake 
import org.apache.spark.sql.streaming.Trigger.ProcessingTime 

val dl2Set = 
spark.readStream
  .table("events")
  .writeStream
  .format("parquet")
  //.option("ignoreDeletes","true")
  .option("checkpointLocation", "/mnt/checkpoints/to_dlake1")
  .option("path", "/mnt/dl2/rawdata")
  .start()

// COMMAND ----------

val data = sqlContext.read.parquet("/mnt/dl2/rawdata")
display(data)

// COMMAND ----------

// PowerBI II. Из parquet пишем в таблицу для чтения из PowerBI. СДЕЛАТЬ JOB 
// Не корректная схема работы с PowerBI!!! Потому, что то что попадает в Data Lake это уже исторические данные (согласно лямбда архитектуры), а мы их используем в качестве realtime данных.
spark.read.parquet("/mnt/dl2/rawdata").write.mode(SaveMode.Overwrite) saveAsTable("events_dataset")

// COMMAND ----------

// Потребитель II.
// Запись из Delta таблицы в Data Lake в другую папку. Тестируем, что два потока чтения могут получать одни и теже данные из Delta Table == Работает!
import org.apache.spark.sql.streaming.Trigger.ProcessingTime 

val dl2Set = 
spark.readStream
  .table("events")
  .writeStream
  .format("parquet")
  .option("checkpointLocation", "/mnt/checkpoints/to_dlakeII_1")
  .option("path", "/mnt/dl2/rawdata_double")
  .start()

// COMMAND ----------

val data = sqlContext.read.parquet("/mnt/dl2/rawdata_double")
display(data)

// COMMAND ----------

//dbutils.fs.rm("/tmp/checkpoints/", true)
dbutils.fs.mkdirs("/mnt/dl2/rawdata_double")
dbutils.fs.ls("/mnt/dl2/")
//dbutils.fs.help()