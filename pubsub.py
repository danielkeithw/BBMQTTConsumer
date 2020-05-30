# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
#from google.cloud import pubsub_v1
#from google.cloud import bigquery
#from __future__ import absolute_import
import base64
import json
import logging
import os
import httplib2
from google.appengine.api import urlfetch
from pprint import pprint

from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

#from oauth2client.contrib.appengine import OAuth2DecoratorFromClientSecrets
#import googleapiclient.discovery

#from apiclient import discovery
#from oauth2client.contrib import appengine

_SCOPE = 'https://www.googleapis.com/auth/bigquery'



from flask import current_app, Flask, render_template, request

app = Flask(__name__)

# Configure the following environment variables via app.yaml
# This is used in the push request handler to veirfy that the request came from
# pubsub and originated from a trusted source.
app.config['PUBSUB_VERIFICATION_TOKEN'] = \
    os.environ['PUBSUB_VERIFICATION_TOKEN']
app.config['PUBSUB_TOPIC'] = os.environ['PUBSUB_TOPIC']
app.config['PROJECT'] = os.environ['GCLOUD_PROJECT']


credentials = GoogleCredentials.get_application_default()
bigquery = build('bigquery', 'v2', credentials=credentials)
http = credentials.authorize(httplib2.Http())
#datasets = service.datasets().list(projectId="burner-board").execute()

#credentials = appengine.AppAssertionCredentials(scope=_SCOPE)
#http = credentials.authorize(httplib2.Http())
#bigquery = discovery.build('bigquery', 'v2', http=http)

# Create the method decorator for oauth.
#decorator = OAuth2DecoratorFromClientSecrets(
#    os.path.join(os.path.dirname(__file__), 'client_secrets.json'),
#    scope='http://www.googleapis.com/auth/bigquery')
#pprint (decorator)
# Create the bigquery api client
#bigquery = googleapiclient.discovery.build('bigquery', 'v2')
#http = decorator.http()



# Global list to storage messages received by this instance.
MESSAGES = []

urlfetch.set_default_fetch_deadline(45)
httplib2.Http(timeout=45) 


# Convert BQ current to signed integer
def fixCurrent(current):
  c = float(current)
  if (c > 32767):
    return (c - 65535) / 100
  else:
    return c / 100

def bit_is_set(value, bit):
  word = 1 << bit
  return (value & word) > 0

def stream_data(dataset_name, table_name, data):

#    bigquery_client = bigquery.Client()
#    dataset = bigquery_client.dataset(dataset_name)
#    table = dataset.table(table_name)

    # Reload the table to get the schema.
#    table.reload()

  rows = data
  body = {"rows":[
    {"json": rows}
  ]}

#  pprint(body)
  response = bigquery.tabledata().insertAll(
    projectId=app.config['PROJECT'],
    datasetId=dataset_name,
    tableId=table_name,
    body=body).execute()

#  pprint(response)

#    errors = table.insert_data(rows)

#    if errors:
#        print('Errors:')
#        pprint(errors)

# [START index]
@app.route('/', methods=['GET', 'POST'])
def index():
#    if request.method == 'GET':
#        return render_template('index.html', messages=MESSAGES)

#    data = request.form.get('payload', 'Example payload').encode('utf-8')

#    publisher = pubsub_v1.PublisherClient()
#    topic_path = publisher.topic_path(
#        current_app.config['PROJECT'],
#        current_app.config['PUBSUB_TOPIC'])
#
#    publisher.publish(topic_path, data=data)

    return 'OK', 200
# [END index]




# [START push]
@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
    if (request.args.get('token', '') !=
            current_app.config['PUBSUB_VERIFICATION_TOKEN']):
        return 'Invalid request', 400

    envelope = json.loads(request.data.decode('utf-8'))
#    print ("envelope=<" + str(envelope) + ">")
    payload = base64.b64decode(envelope['message']['data'])
    if 'attributes' in envelope['message'].keys():
        attributes = envelope['message']['attributes']
        if 'deviceId' in attributes.keys():
            deviceId = attributes['deviceId']
        if 'subFolder' in attributes.keys():
            subFolder = attributes['subFolder']
        else:
            subFolder = ""

# 2017-10-01T18:15:41.902Z 
# YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
    if 'publish_time' in envelope['message'].keys():
        publish_time = envelope['message']['publish_time'][:-1]
#    publish_time = "2017-10-01T18:15:41.9022"
    payload_withdate = payload.split(b',', 1)
    messageDate = payload_withdate[0].decode("utf-8")
    
#    print ("msgData=<" + str(messageDate) + ">")
#    print ("attributes=<" + str(attributes) + ">")
#    print ("deviceId=<" + str(deviceId) + ">")
#    print ("subFolder=<" + str(subFolder) + ">")
#    print ("payload=<" + str(payload) + ">")

#    print ("message_date=<"+ str(messageDate) + "> publish_time=<" + publish_time + "> " + " deviceId=<" + str(deviceId) + "> " + str(payload))

    battery_data = {}
    if "bbtelemetery" in subFolder:  
        metrics = payload_withdate[1][1:].split(b',')

	battery_data = {
         "board_name": deviceId[3:],
         "voltage": float(metrics[5]) / 1000.0,
         "current": 0,
         "full_capacity": float(metrics[4]) / 100.0,
         "remaining_capacity": float(metrics[3]) / 100.0,
         "time_stamp": publish_time,
         "average_current": fixCurrent(float(metrics[6])),
         "instant_current": fixCurrent(float(metrics[9])),
         "battery_level": float(metrics[1]),
         "coulomb_counter": bit_is_set(int(metrics[0]), 11),
         "rupdate_disable": bit_is_set(int(metrics[0]), 2),
         "v_ok_for_qmax": bit_is_set(int(metrics[0]), 1),
         "qmax_updates_enable": bit_is_set(int(metrics[0]), 0),
         "otemp_charge": bit_is_set(int(metrics[8]), 15),
         "otemp_discharge": bit_is_set(int(metrics[8]), 14),
         "bat_hi": bit_is_set(int(metrics[8]), 13),
         "bat_low": bit_is_set(int(metrics[8]), 12),
         "charge_inh": bit_is_set(int(metrics[8]), 11),
         "charge_notallow": bit_is_set(int(metrics[8]), 10),
         "full_charge": bit_is_set(int(metrics[8]), 9),
         "fastcharge_allowed": bit_is_set(int(metrics[8]), 8),
         "ocv_taken": bit_is_set(int(metrics[8]), 7),
         "condition_flag": bit_is_set(int(metrics[8]), 4),
         "state_of_charge_1": bit_is_set(int(metrics[8]), 2),
         "state_of_charge_final": bit_is_set(int(metrics[8]), 1),
         "discharge": bit_is_set(int(metrics[8]), 0),
         "first_dod": bit_is_set(int(metrics[10]), 13),
         "dod_end_of_charge": bit_is_set(int(metrics[10]), 10),
         "drtc": bit_is_set(int(metrics[10]), 9),
         "raw_status": int(metrics[0]),
         "raw_flags": int(metrics[8]),
         "raw_flagsb": int(metrics[10]),
         "message_timestamp": messageDate.replace("T", " ")
        }
        stream_data('telemetry', 'battery_Data', battery_data)
    event_data = {}
    if "bbevent" in subFolder:  
        metrics = payload_withdate[1][1:-1].split(b',')

	event_data = {
         "board_name": deviceId[3:],
         "time_stamp": publish_time,
         "originator": metrics[0],
         "sig_strength": float(metrics[1]),
         "lat": float(metrics[2]),
         "lon": float(metrics[3])
        }
        stream_data('telemetry', 'events', event_data)
     


        # soh_recalc
        #battery_data.append(bit_is_set(int(metrics[10]), 15))

#        pprint (battery_data)

#    MESSAGES.append(envelope)
#    MESSAGES.append(json.loads(battery_data))


    # Returning any 2xx status indicates successful receipt of the message.
    return 'OK', 200
# [END push]


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)


# [END app]
