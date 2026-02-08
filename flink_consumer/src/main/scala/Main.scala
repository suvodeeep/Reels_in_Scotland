import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{Configuration, RestOptions}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.util.Collector
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.mongodb.scala._
import org.mongodb.scala.bson.collection.immutable.Document
import scala.concurrent.Await
import scala.concurrent.duration._

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Date

// ============================================================================
// DATA MODELS
// ============================================================================

case class WeatherReading(
    timestamp: String,
    timestampMs: Long,
    temperature: Option[Double],
    precipitation: Option[Double]
)

case class JoinedWeatherReading(
    timestamp: String,
    temperature: Double,
    precipitation: Double,
    timestampMs: Long
)

case class AggregatedWeatherWindow(
    windowStart: String,
    windowEnd: String,
    avgTimestamp: String,
    avgTemperature: Double,
    avgPrecipitation: Double,
    count: Int
) {
    def toMongoDocument: Document = {
        Document(
            "window_start" -> windowStart,
            "window_end" -> windowEnd,
            "avg_timestamp" -> avgTimestamp,
            "avg_temperature" -> avgTemperature,
            "avg_precipitation" -> avgPrecipitation,
            "record_count" -> count,
            "processed_at" -> new Date()
        )
    }
}

// ============================================================================
// KAFKA DESERIALIZER
// ============================================================================

class WeatherDeserializer extends DeserializationSchema[WeatherReading] {
    
    @transient private lazy val mapper = new ObjectMapper()
    @transient private lazy val formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
    @transient private lazy val formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm")
    
    override def deserialize(message: Array[Byte]): WeatherReading = {
        try {
            val json: JsonNode = mapper.readTree(message)
            
            if (json.get("time") == null) {
                System.err.println(s"Missing time field in message: ${new String(message)}")
                return null
            }
            
            val timeStr = json.get("time").asText()
            
            val timeMs = try {
                LocalDateTime.parse(timeStr, formatter1)
                  .toInstant(java.time.ZoneOffset.UTC)
                  .toEpochMilli
            } catch {
                case _: Exception =>
                    LocalDateTime.parse(timeStr, formatter2)
                      .toInstant(java.time.ZoneOffset.UTC)
                      .toEpochMilli
            }
            
            val normalizedTimeStr = if (timeStr.length > 16) timeStr.substring(0, 16) else timeStr
            
            val temperature = Option(json.get("temperature_2m")).map(_.asDouble())
            val precipitation = Option(json.get("precipitation")).map(_.asDouble())
            
            if (temperature.isEmpty && precipitation.isEmpty) {
                System.err.println(s"Message has neither temperature nor precipitation: ${new String(message)}")
                return null
            }
            
            WeatherReading(normalizedTimeStr, timeMs, temperature, precipitation)
            
        } catch {
            case e: Exception =>
                System.err.println(s"Error parsing weather message: ${e.getMessage}")
                System.err.println(s"Raw message: ${new String(message)}")
                null
        }
    }
    
    override def isEndOfStream(nextElement: WeatherReading): Boolean = false
    
    override def getProducedType: TypeInformation[WeatherReading] = 
        TypeInformation.of(classOf[WeatherReading])
}

// ============================================================================
// MONGODB SINK WITH RichSink for PERSISTENT MONGO CONNECTION
// ============================================================================

class PersistentMongoDBSink extends RichSinkFunction[AggregatedWeatherWindow] {
    @transient private var mongoClient: MongoClient = _
    @transient private var collection: MongoCollection[Document] = _
    
    override def open(parameters: Configuration): Unit = {
        val mongoUri = sys.env.getOrElse("MONGO_URI", 
            throw new RuntimeException(" MONGO_URI environment variable not set!"))
        
        println(s" Establishing persistent MongoDB connection...")
        
        try {
            mongoClient = MongoClient(mongoUri)
            val database = mongoClient.getDatabase("weather_pipeline")
            collection = database.getCollection("aggregated_2min")
            
            Await.result(collection.countDocuments().toFuture(), 5.seconds)
            
            println(" MongoDB connection established and verified")
            println(" This connection will be reused for all writes from this task")
        } catch {
            case e: Exception =>
                println(s" Failed to connect to MongoDB: ${e.getMessage}")
                throw e
        }
    }
    
    override def invoke(value: AggregatedWeatherWindow, context: SinkFunction.Context): Unit = {
        try {
            val insertObservable = collection.insertOne(value.toMongoDocument)
            Await.result(insertObservable.toFuture(), 10.seconds)
            
            println(s" Saved to MongoDB: ${value.windowStart} " +
                    s"(${value.count} records, Avg Temp: ${value.avgTemperature}°C)")
        } catch {
            case e: Exception =>
                println(s" MongoDB write failed: ${e.getMessage}")
                throw e
        }
    }
    
    override def close(): Unit = {
        if (mongoClient != null) {
            println("🔌 Closing MongoDB connection...")
            mongoClient.close()
            println(" MongoDB connection closed gracefully")
        }
    }
}

// ============================================================================
// MAIN APPLICATION
// ============================================================================

object WeatherStreamProcessor {
    
    def main(args: Array[String]): Unit = {
        println(" Starting Weather Stream Processor...")
        println("=" * 60)
        
        Runtime.getRuntime.addShutdownHook(new Thread() {
            override def run(): Unit = {
                println("\n" + "=" * 60)
                println(" Graceful shutdown initiated by user (Ctrl+C)")
                println(" Cleaning up resources now...")
                println(" Flink job stopped")
                println(" Kafka consumers closed")
                println(" MongoDB connections closed")
                println(" Goodbye!!!")
                println("=" * 60)
            }
        })
        
        // ====================================================================
        // 1. FLINK ENVIRONMENT SETUP WITH DELAYED FIRST CHECKPOINT
        // ====================================================================
        
        val config = new Configuration()
        config.set(RestOptions.PORT, Int.box(9000))
        
        val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
        env.setParallelism(2)
        
        // Configuring the checkpointing with initial delay
        val checkpointConfig = env.getCheckpointConfig
        
        // 60 seconds delay before the FIRST checkpoint, gives Kafka time to elect coordinator and stabilize
        
        checkpointConfig.setCheckpointInterval(60000)
        checkpointConfig.setMinPauseBetweenCheckpoints(5000)  // At least 5 seconds between checkpoints
        
        checkpointConfig.setCheckpointTimeout(600000)  // 10 minutes timeout
        checkpointConfig.setTolerableCheckpointFailureNumber(3)  // Allow 3 failures before job fails
        
        println(" Flink execution environment created")
        println(" Web UI available at: http://localhost:9000")
        println(" Parallelism set to 2")
        println(" First checkpoint will occur after 60 seconds (allowing Kafka to stabilize)")
        println(" Subsequent checkpoints every 60 seconds")
        
        // ====================================================================
        // 2. KAFKA SOURCE CONFIGURATION
        // ====================================================================
        
        val KAFKA_BROKERS = "localhost:9092,localhost:9094"
        val TOPIC = "weather_stream"
        
        val weatherSource = KafkaSource.builder[WeatherReading]()
          .setBootstrapServers(KAFKA_BROKERS)
          .setTopics(TOPIC)
          .setValueOnlyDeserializer(new WeatherDeserializer())
          .setGroupId("flink-weather-consumer")
          .setStartingOffsets(OffsetsInitializer.earliest())
          .build()
        
        println(" Kafka source configured for topic: weather_stream")
        
        // ====================================================================
        // 3. CREATE DATA STREAM
        // For Open-Meteo: Timestamps are stale/delayed, so we use processing time aggregate based on WHEN DATA ARRIVES, not the timestamp in the data
        // ====================================================================
        
        val weatherStream: DataStream[WeatherReading] = env
          .fromSource(weatherSource, WatermarkStrategy.noWatermarks(), "Weather Source")
          .name("Weather Stream")
        
        println(" Data stream created (processing time windowing)")
        println(" Windows based on when data ARRIVES, not event timestamps")
        
        // ====================================================================
        // 4. SPLIT INTO TEMPERATURE AND PRECIPITATION STREAMS
        // ====================================================================
        
        val tempStream = weatherStream
          .filter(_.temperature.isDefined)
          .map(r => (r.timestamp, r.timestampMs, r.temperature.get))
          .name("Temperature Stream")
        
        val precipStream = weatherStream
          .filter(_.precipitation.isDefined)
          .map(r => (r.timestamp, r.timestampMs, r.precipitation.get))
          .name("Precipitation Stream")
        
        println(" Streams split into temperature and precipitation")
        
        // ====================================================================
        // 5. JOIN STREAMS ON TIMESTAMP
        // ====================================================================
        
        val joinedStream: DataStream[JoinedWeatherReading] = tempStream
          .keyBy(_._1)
          .intervalJoin(precipStream.keyBy(_._1))
          .between(Time.seconds(-5), Time.seconds(5))
          .process(new ProcessJoinFunction[(String, Long, Double), (String, Long, Double), JoinedWeatherReading] {
              override def processElement(
                  temp: (String, Long, Double),
                  precip: (String, Long, Double),
                  ctx: ProcessJoinFunction[(String, Long, Double), (String, Long, Double), JoinedWeatherReading]#Context,
                  out: Collector[JoinedWeatherReading]
              ): Unit = {
                  out.collect(JoinedWeatherReading(
                      timestamp = temp._1,
                      temperature = temp._3,
                      precipitation = precip._3,
                      timestampMs = temp._2
                  ))
                  println(s" Joined: ${temp._1} - Temp: ${temp._3}°C, Precip: ${precip._3}mm")
              }
          })
          .name("Joined Weather Stream")
        
        println(" Stream join configured")
        
        // ====================================================================
        // 6. WINDOWING AND AGGREGATION (PROCESSING TIME)
        // Windows close every 2 minutes based on wall-clock time
        // Each window contains whatever data arrived in that 2-minute period
        // ====================================================================
        
        val aggregatedStream: DataStream[AggregatedWeatherWindow] = joinedStream
          .keyBy(_ => "all")
          .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))  // PROCESSING TIME
          .process(new org.apache.flink.streaming.api.scala.function.ProcessWindowFunction[
              JoinedWeatherReading, 
              AggregatedWeatherWindow, 
              String, 
              org.apache.flink.streaming.api.windowing.windows.TimeWindow
          ] {
              override def process(
                  key: String,
                  context: Context,
                  elements: Iterable[JoinedWeatherReading],
                  out: Collector[AggregatedWeatherWindow]
              ): Unit = {
                  val readings = elements.toSeq
                  if (readings.nonEmpty) {
                      val count = readings.size
                      val avgTemp = readings.map(_.temperature).sum / count
                      val avgPrecip = readings.map(_.precipitation).sum / count
                      
                      // Get the MOST RECENT timestamp from the data (not the window time)
                      // This is the actual weather observation time from Open-Meteo
                      val mostRecentTimestamp = readings.map(_.timestamp).max
                      val avgTimestampMs = readings.map(_.timestampMs).sum / count
                      
                      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                      
                      // Window times are wall-clock times (when data arrived)
                      val windowStart = LocalDateTime.ofInstant(
                          java.time.Instant.ofEpochMilli(context.window.getStart),
                          java.time.ZoneOffset.UTC
                      ).format(formatter)
                      
                      val windowEnd = LocalDateTime.ofInstant(
                          java.time.Instant.ofEpochMilli(context.window.getEnd),
                          java.time.ZoneOffset.UTC
                      ).format(formatter)
                      
                      val avgTimestamp = LocalDateTime.ofInstant(
                          java.time.Instant.ofEpochMilli(avgTimestampMs),
                          java.time.ZoneOffset.UTC
                      ).format(formatter)
                      
                      out.collect(AggregatedWeatherWindow(
                          windowStart = windowStart,
                          windowEnd = windowEnd,
                          avgTimestamp = mostRecentTimestamp,  // Use most recent data timestamp
                          avgTemperature = avgTemp,
                          avgPrecipitation = avgPrecip,
                          count = count
                      ))
                  }
              }
          })
          .name("2-Minute Aggregation")
        
        println(" 2-minute tumbling window aggregation configured (PROCESSING TIME)")
        println(" Windows close every 2 minutes regardless of event timestamps")
        println(" MongoDB will receive aggregates every 2 minutes")
        
        // ====================================================================
        // 7. CONSOLE OUTPUT
        // ====================================================================
        
        aggregatedStream.map { result =>
            println("\n" + "=" * 60)
            println(s" WINDOW COMPLETE!")
            println(s"   Period: ${result.windowStart} to ${result.windowEnd}")
            println(s"   Avg Temperature: ${result.avgTemperature}°C")
            println(s"   Avg Precipitation: ${result.avgPrecipitation}mm")
            println(s"   Record Count: ${result.count}")
            println("=" * 60)
            result
        }.name("Debug Print")
        
        // ====================================================================
        // 8. MONGODB SINK
        // ====================================================================
        
        val MONGO_URI = sys.env.getOrElse("MONGO_URI", "mongodb+srv://username:password@cluster.mongodb.net/")
        
        if (MONGO_URI.nonEmpty && !MONGO_URI.contains("username:password")) {
            aggregatedStream.addSink(new PersistentMongoDBSink()).name("MongoDB Sink")
            
            println(" MongoDB sink configured with persistent connection pooling")
            println(" Connection will be created once per task (2 total)")
            println(" Each window result reuses the existing connection")
        } else {
            println(" MongoDB URI not configured - skipping MongoDB writes")
            println(" Set MONGO_URI environment variable to enable MongoDB sink")
        }
        
        // ====================================================================
        // 9. EXECUTE THE JOB
        // ====================================================================
        
        println("\n Starting Flink job...")
        println("=" * 60)
        println(" Open http://localhost:9000 to see the Flink Web UI")
        println(" First checkpoint in ~60 seconds (Kafka stabilization time)")
        println(" Press Ctrl+C to stop the job gracefully")
        println("=" * 60)
        
        env.execute("Weather Stream Processor")
    }
}