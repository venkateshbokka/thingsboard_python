import random
import time
import datetime
import json
import paho.mqtt.client as mqtt

# MQTT broker details
broker = "mqtt.thingsboard.cloud" # you need change here
port = 1883
topic = "v1/devices/me/telemetry" #v1/devices/me/telemetry # you need change here
username = "venkat1" # you need change here
password = "venkat1"# you need change here
client_id = "watermeter1"  # Replace with your desired client ID

# Original data with constant fields
data = {
    "device_id": 1001,
    "timestamp": "",
    "instant_flow": "",
    "accumulated_flow_totalizer": "",
    "flow_meter_temp": "",
    "flow_meter_battery": "",
    "latitude": "70.635533",
    "longitude": "-92.115525"
}

def generate_random_data():
    # Generate random values for each field
    data["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat() + 'Z'
    data["instant_flow"] = f"{random.uniform(0, 100):.2f}"
    data["accumulated_flow_totalizer"] = f"{random.uniform(0, 1000):.2f}"
    data["flow_meter_temp"] = f"{random.uniform(0, 100):.2f}"
    data["flow_meter_battery"] = f"{random.uniform(0, 100):.2f}"
    
    # Return the generated data as a JSON string
    return json.dumps(data)

# Define the on_connect callback function
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print(f"Failed to connect, return code {rc}\n")

# Create MQTT client with client ID
client = mqtt.Client(client_id=client_id)
client.username_pw_set(username, password)
client.on_connect = on_connect

# Connect to the broker
client.connect(broker, port, 60)

# Start the loop
client.loop_start()

# Publish data in a loop with a delay
try:
    while True:
        random_data = generate_random_data()
        client.publish(topic, json.dumps(random_data))  # Publish the JSON-encoded data
        print(f"Published: {random_data}")
        time.sleep(5)  # Delay in seconds
except KeyboardInterrupt:
    print("Exiting...")
finally:
    client.loop_stop()
    #client.disconnect()
