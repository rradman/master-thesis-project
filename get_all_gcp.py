from google.cloud import bigquery
import hashlib
import sys, os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/robi/ethereum/myProject-diplomski2-868d724aa252.json"

client = bigquery.Client()

query_string = "SELECT deviceID, temperature, timecollected, blockchain, txHash FROM `myproject-diplomski2.temperatureDataset2.temperatureDataset2Table2`" \
               " ORDER BY timecollected"

query = (query_string)

query_job = client.query(
    query,
    location="US",
)  # API request - starts the query

for row in query_job:  # API request - fetches results
    print("Stored data:       ", "deviceID: ", row[0], ", temperature: ", row[1], ", timecollected: ", str(row[2])[:-6])
    storedData = str(row[0]) + "; " + str(row[1]) + "; " + str(row[2])[:-6]
    dataHash = hashlib.sha256(storedData.encode('utf-8')).hexdigest()
    print("Hash of the data:  ", dataHash)
    print(row[3], "Tx Hash:   ", row[4], "\n")
