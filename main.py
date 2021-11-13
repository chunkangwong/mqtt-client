# Copyright 2017 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Python sample for connecting to Google Cloud IoT Core via MQTT, using JWT.
This example connects to Google Cloud IoT Core via MQTT, using a JWT for device
authentication. After connecting, by default the device publishes 100 messages
to the device's MQTT topic at a rate of one per second, and then exits.
Before you run the sample, you must follow the instructions in the README
for this sample.
"""

import argparse
import datetime
import logging
import re
import os
import random
import ssl
import time
import json

from google.cloud import storage
from picamera import PiCamera
import RPi.GPIO as GPIO
import jwt
import paho.mqtt.client as mqtt

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.CRITICAL)

minimum_backoff_time = 1

MAXIMUM_BACKOFF_TIME = 32

should_backoff = False

# Setup camera
camera = PiCamera()
camera.resolution = (150, 150)

# Setup GPIO
PIR_PIN = 14
GPIO.setmode(GPIO.BCM)
GPIO.setup(PIR_PIN, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)

# Topic Regex
topic_regex = re.compile(r"^\/devices/(.+?)\/(.+)$")

def create_jwt(project_id, private_key_file, algorithm):
    """Creates a JWT (https://jwt.io) to establish an MQTT connection.
    Args:
     project_id: The cloud project ID this device belongs to
     private_key_file: A path to a file containing either an RSA256 or
             ES256 private key.
     algorithm: The encryption algorithm to use. Either 'RS256' or 'ES256'
    Returns:
        A JWT generated from the given project_id and private key, which
        expires in 20 minutes. After 20 minutes, your client will be
        disconnected, and a new JWT will have to be generated.
    Raises:
        ValueError: If the private_key_file does not contain a known key.
    """

    token = {
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=20),
        "aud": project_id,
    }

    # Read the private key file.
    with open(private_key_file, "r") as f:
        private_key = f.read()

    print(
        "Creating JWT using {} from private key file {}".format(
            algorithm, private_key_file
        )
    )

    return jwt.encode(token, private_key, algorithm=algorithm)

def error_str(rc):
    """Convert a Paho error to a human readable string."""
    return "{}: {}".format(rc, mqtt.error_string(rc))


def on_connect(unused_client, unused_userdata, unused_flags, rc):
    """Callback for when a device connects."""
    print("on_connect", mqtt.connack_string(rc))

    global should_backoff
    global minimum_backoff_time
    should_backoff = False
    minimum_backoff_time = 1


def on_disconnect(unused_client, unused_userdata, rc):
    """Paho callback for when a device disconnects."""
    print("on_disconnect", error_str(rc))

    global should_backoff
    should_backoff = True


def on_publish(unused_client, unused_userdata, unused_mid):
    """Paho callback when a message is sent to the broker."""
    print("on_publish at " + str(datetime.datetime.now()))


def on_message(unused_client, unused_userdata, message):
    """Callback when the device receives a message on a subscription."""
    payload = str(message.payload.decode("utf-8"))
    topic = message.topic
    print(
        "Received message '{}' on topic '{}' with Qos {}".format(
            payload, topic, str(message.qos)
        )
    )
    mo = topic_regex.search(topic)
    (device_id, main_topic) = mo.groups()
    if main_topic == "config":
        config = json.loads(payload)
        status = config["status"]


def get_client(
    project_id,
    cloud_region,
    registry_id,
    device_id,
    private_key_file,
    algorithm,
    ca_certs,
    mqtt_bridge_hostname,
    mqtt_bridge_port,
):
    """Create our MQTT client. The client_id is a unique string that identifies
    this device. For Google Cloud IoT Core, it must be in the format below."""
    client_id = "projects/{}/locations/{}/registries/{}/devices/{}".format(
        project_id, cloud_region, registry_id, device_id
    )
    print("Device client_id is '{}'".format(client_id))

    client = mqtt.Client(client_id=client_id)

    client.username_pw_set(
        username="unused", password=create_jwt(project_id, private_key_file, algorithm)
    )

    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    client.connect(mqtt_bridge_hostname, mqtt_bridge_port)

    mqtt_config_topic = "/devices/{}/config".format(device_id)

    client.subscribe(mqtt_config_topic, qos=1)

    mqtt_command_topic = "/devices/{}/commands/#".format(device_id)

    print("Subscribing to {}".format(mqtt_command_topic))
    client.subscribe(mqtt_command_topic, qos=0)

    return client


def event_handler(client, mqtt_topic):
    now = str(datetime.datetime.now())
    now = now.split('.')[0]
    now = now.replace(' ', '_')
    now = now.replace(':', '-')
    filename = 'img_' + now + '.jpg'
    filepath = os.path.join('temp', filename)
    camera.capture(filepath)
    time.sleep(2)
    client.publish(mqtt_topic, json.dumps({'status': 1, 'timestamp': time.time()}), qos=1)


def mqtt_device_demo(args):
    """Connects a device, sends data, and receives data."""
    global minimum_backoff_time
    global MAXIMUM_BACKOFF_TIME

    sub_topic = "events" if args["message_type"] == "event" else "state"

    mqtt_topic = "/devices/{}/{}".format(args["device_id"], sub_topic)

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args["jwt_expires_minutes"]
    client = get_client(
        args["project_id"],
        args["cloud_region"],
        args["registry_id"],
        args["device_id"],
        args["private_key_file"],
        args["algorithm"],
        args["ca_certs"],
        args["mqtt_bridge_hostname"],
        args["mqtt_bridge_port"],
    )
    GPIO.add_event_detect(PIR_PIN, GPIO.RISING, lambda x: event_handler(client, mqtt_topic))

    while 1:
        client.loop()

        if should_backoff:
            if minimum_backoff_time > MAXIMUM_BACKOFF_TIME:
                print("Exceeded maximum backoff time. Giving up.")
                break

            delay = minimum_backoff_time + random.randint(0, 1000) / 1000.0
            print("Waiting for {} before reconnecting.".format(delay))
            time.sleep(delay)
            minimum_backoff_time *= 2
            client.connect(args["mqtt_bridge_hostname"], args["mqtt_bridge_port"])

        seconds_since_issue = (datetime.datetime.utcnow() - jwt_iat).seconds
        if seconds_since_issue > 60 * jwt_exp_mins:
            print("Refreshing token after {}s".format(seconds_since_issue))
            jwt_iat = datetime.datetime.utcnow()
            client.loop()
            client.disconnect()
            client = get_client(
                args["project_id"],
                args["cloud_region"],
                args["registry_id"],
                args["device_id"],
                args["private_key_file"],
                args["algorithm"],
                args["ca_certs"],
                args["mqtt_bridge_hostname"],
                args["mqtt_bridge_port"],
            )
        for i in range(0, 60):
            time.sleep(1)
            client.loop()


def main():
    with open('config.json') as f:
       config_content = f.read()
    config = json.loads(config_content)
    mqtt_device_demo(config)
    print("Finished.")


if __name__ == "__main__":
    main()
