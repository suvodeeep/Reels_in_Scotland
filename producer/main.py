from kafka import KafkaProducer
import json, requests, time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092', 'localhost:9094'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

try:
    while True:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": 55.95,
                "longitude": -3.95,
                "current": ["temperature_2m", "precipitation"]
            }
        )

        current = response.json().get("current", {})

        temp_data = {"time": current.get("time"), "temperature_2m": current.get("temperature_2m")}
        rain_data = {"time": current.get("time"), "precipitation": current.get("precipitation")}

        # Send temperature to partition 0
        producer.send("weather_stream", value=temp_data, partition=0)

        # Send rainfall to partition 1
        producer.send("weather_stream", value=rain_data, partition=1)

        print("Sent temperature and rainfall data")
        time.sleep(30)

except KeyboardInterrupt:
    print("Gracefull shutdown initiated by user.")

finally:
    producer.flush()
    producer.close()
    print("All done!")