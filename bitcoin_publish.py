import paho.mqtt.client as mqtt
import ssl, datetime, json, jwt, sys
from time import sleep
import hashlib
from bit import PrivateKeyTestnet

REMOTE_SERVER = 'www.google.com'
DIR = '/home/robi/.ssh/'

ca_certs = DIR + 'roots.pem'
private_key_file = DIR + 'ec_private.pem'

mqtt_url = "mqtt.googleapis.com"
mqtt_port = 8883
mqtt_topic = "/projects/myproject-diplomski2/topics/temperature_topic2"
project_id = "myproject-diplomski2"
cloud_region = "us-central1"
registry_id = "temperature_registryid2"
device_id = "temperature_device_id"
connflag = False
tx_hash = "Data is NOT on blockchain!"
blockchain = "Bitcoin"

def post_to_blockchain(dataHash):

	with open("/home/robi/bitcoin/.keys/pk1.txt", 'r') as f:
	    pr_key1 = f.read()
	with open("/home/robi/bitcoin/.keys/pk2.txt", 'r') as f:
	    pr_key2 = f.read()

	address_1 = PrivateKeyTestnet(pr_key1[:-1])
	address_2 = PrivateKeyTestnet(pr_key2[:-1])
	
	print("Data Hash: ", dataHash)

	tx_hash = (address_2.send([(address_1.address, 1, 'satoshi')], 
		message=dataHash))
	print ("TxHash: ", tx_hash)

	return tx_hash

def calculate_hashes(deviceID, temperature, currentTime):
    new_input = deviceID + "; " + temperature + "; " + currentTime
    dataHash = hashlib.sha256(new_input.encode('utf-8')).hexdigest()
    return dataHash

def create_jwt(project_id, private_key_file, algorithm):
    token = {
            'iat': datetime.datetime.utcnow(),
            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
            'aud': project_id
    }
    with open(private_key_file, 'r') as f:
        private_key = f.read()

    print('Creating JWT using {} from private key file {}'.format(
            algorithm, private_key_file))
    return jwt.encode(token, private_key, algorithm=algorithm)

def error_str(rc):
    return "Some error occurred. {}: {}".format(rc, mqtt.error_string(rc))

def on_disconnect(unused_client, unused_userdata, rc):
    print("on_disconnect", error_str(rc))

def on_connect(client, userdata, flags, response_code):
    global connflag
    connflag = True
    print("Connected with status: {0}".format(response_code))

def on_publish(client, userdata, mid):
    print("Published")

def datetime_handler(x):
    if isinstance(x, datetime.datetime):
        return x.isoformat()
    raise TypeError("Unknown type")

if __name__ == "__main__":
    client = mqtt.Client("projects/{}/locations/{}/registries/{}/devices/{}".format(
                         project_id,
                         cloud_region,
                         registry_id,
                         device_id))

    client.username_pw_set(username='unused',
                           password=create_jwt(project_id,
                                               private_key_file,
                                               algorithm="ES256"))

    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect

    print("Connecting to Google IoT Broker...")
    client.connect(mqtt_url, mqtt_port, keepalive=60)

    while True:
        client.loop()
        if connflag == True:
            deviceID = sys.argv[1]
            temperature = sys.argv[2]
            currentTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            dataHash = calculate_hashes(deviceID, temperature, currentTime)                       
            txHash = post_to_blockchain(dataHash)
            sleep(1)
            
            raw_payload_cloud = { "deviceID": deviceID,
                                  "temperature": temperature,
                                  "timecollected": currentTime,
                                  "blockchain": blockchain,
                                  "txHash": txHash }
            payload_cloud = json.dumps(raw_payload_cloud, default=datetime_handler)

            res = client.publish('/devices/{}/events'.format(device_id),
                                payload_cloud, 
                                qos=1)
            sleep(1)
            print ("Publishing Finished!\n")
            print ("DONE!")
            break
        else:
            print("Waiting for connection...")
