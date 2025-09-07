import functions_framework
from google.cloud import bigquery
from urllib.request import urlopen
from google.cloud import storage
from google.cloud.storage import Blob
import requests
import re
import json 
import pandas as pd
import pandas_gbq
from time import perf_counter
import math
from datetime import datetime, timedelta
from flask import Response
from google.cloud import pubsub_v1
from google.cloud.exceptions import NotFound
from liquid import Template
from datetime import datetime, timedelta
from google.auth import default

@functions_framework.http
def prospectEstimator(request):
    project_id =default()[1]
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        print("responding to preflight")
        return ("", 204, headers)

    request_json = request.get_json(silent=True)
    print(request_json)
    #region save data into request logging table
    ip_address = request.headers.get("x-forwarded-for","ip")
    current_timestamp = datetime.now()
    publisher = pubsub_v1.PublisherClient()
    request_logging_topic = f"projects/{project_id}/topics/logging"
    request_logging_topic_data = json.dumps({"cloudfunction":"prospectEstimator","ipaddress":f"{ip_address}","payload":request_json,"createdon":f"{current_timestamp}"})
    data_bytes = request_logging_topic_data.encode("utf-8")
    publisher.publish(request_logging_topic,data_bytes)
    #endregion

    start_time = datetime.now()
    bqclient = bigquery.Client()

    try:
        #region extract data from payload
        orderId = 0
        if"orderId" in request_json:
            orderId = request_json.get("orderId")
        else:
            raise Exception("orderId is missing in payload")

        version = 0
        if"version" in request_json:
            version = request_json.get("version")
        else:
            raise Exception("varsion is missing in payload")
        
        logKey = 0
        if"logKey" in request_json:
            logKey = request_json.get("logKey")
        else:
            raise Exception("logKey is missing in payload")

        filePath=''
        if "filePath" in request_json:
            filePath = request_json.get("filePath",'')

        orderQuantity = ""
        if"orderQuantity" in request_json:
            orderQuantity = request_json.get("orderQuantity")
        else:
            raise Exception("orderQuantity is missing in payload")

        segments = ""
        if"segments" in request_json:
            segments = request_json.get("segments")
        else:
            raise Exception("segments are missing in payload")


        b1Quantity = orderQuantity
        print(b1Quantity)

        segmentsJson = json.dumps(segments)
        orderQuantityJson = json.dumps(orderQuantity)
        #endregion


        #region upsert order detail into metadata table
        print("region upsert order detail into metadata table")
        mergeQuery = f'''
        MERGE Spectacle.OrdersMetaDataV2 T
        USING (select {orderId} as orderid, {version} as version) S
        ON T.orderid = S.orderid
        AND T.version = S.version
        WHEN MATCHED THEN
        UPDATE SET 
        segments = """{segmentsJson}"""
        dataset = "B2C",
        OrderQuantity = """{orderQuantityJson}"""
        status = "in progress",
        CreatedDate = current_datetime()
        WHEN NOT MATCHED THEN
        INSERT (orderId, version, segments, dataset, OrderQuantity, status, CreatedDate) 
        Values({orderId}, {version}, """{segmentsJson}""", "B2C", """{orderQuantityJson}""" , "Quantity Estimator", current_datetime());
        '''

        print("mergequery:", mergeQuery.replace('\n', ' '))
        result = bqclient.query(mergeQuery).to_dataframe()

        orderByQuery = " ORDER BY Age_B"
        segmentsWithQuantity = []

        for segment in segments:
            if "quantity" in segment:
                if int(segment['quantity']) >= 0:
                    segmentsWithQuantity.append(segment)
            else:
                    raise Exception("quantity is missing in segments")

        print("segs:",segmentsWithQuantity)

        segmentsWithQuantityQuery = ''
        for idx,segment in enumerate(segmentsWithQuantity):
            if idx !=0:
                segmentsWithQuantityQuery = segmentsWithQuantityQuery + """
                UNION ALL
                """
            name = segment['StoreNumber']
            segQuantity = int(segment['quantity'])  

            query_template = f"""
                SELECT 
                {name} as store,
                CASE 
                    WHEN AGE_B >=60 AND AGE_B <=69 THEN '60-69' 
                    WHEN Age_B>=70 THEN '70+' 
                    ELSE 'NA' 
                END AS age_Group
                ,COUNT(1) cnt           
                FROM Prospect.PoolData
                WHERE location='{name}'
                GROUP BY 
                CASE 
                    WHEN AGE_B >=60 AND AGE_B <=69 THEN '60-69' 
                    WHEN Age_B>=70 THEN '70+' 
                    ELSE 'NA' 
                END
                HAVING age_Group!='NA'
                
            """

            print("query_template:", query_template)
            segmentsWithQuantityQuery = segmentsWithQuantityQuery +  query_template
        
        # Outer OrderBy of UNION ALL
        segmentsWithQuantityQuery = segmentsWithQuantityQuery + "ORDER BY store"

        print(" final query for segmentsWithQuantityQuery: ", segmentsWithQuantityQuery.replace('\n', ' '))

        segmentsWithQuantityQueryResult = pd.DataFrame()

        if len(segmentsWithQuantity) > 0:
            segmentsWithQuantityQueryResult = bqclient.query(segmentsWithQuantityQuery).to_dataframe()
 
        segmentsWithQuantityQueryResult=segmentsWithQuantityQueryResult.fillna(0)
        segmentsWithQuantityQueryResult = segmentsWithQuantityQueryResult.sort_values(["store", "age_Group"])
        print(segmentsWithQuantityQueryResult)


        #Inset columns
        segmentsWithQuantityQueryResult.insert(0,'JobId',logKey)
        segmentsWithQuantityQueryResult['CreatedDate']=datetime.now()


        # Upload df as csv file to bucket and gets its presigned url to download it 
        client = storage.Client()
        bucket = client.bucket('spectacle-order')
        exportFilePath=f'{filePath}/AvailableProspects_{orderId}_{version}_{logKey}.csv'
        blob=bucket.blob(exportFilePath)
        blob.upload_from_string(segmentsWithQuantityQueryResult.to_csv(index=False, sep=','), 'text/csv')


        end_time = datetime.now()
        total_time_taken = end_time - start_time
        minsTaken = total_time_taken.total_seconds()

        return json.dumps({
                    "status":"Success"
                    ,"message": "Success"
                    ,"timeTaken": str(round(minsTaken,2)) + ' sec'
                    ,"logKey": logKey
                    ,"fileLocation":f'gs://spectacle-order/{exportFilePath}'
                }),200,{'Content-Type': 'application/json', 'Access-Control-Allow-Origin' : '*', 'Access-Control-Allow-Headers': 'Content-Type, Authorization'}

    except Exception as e:
            print("exception:",  e)    
            return json.dumps({"status":"Failed","message":str(e)}), 400,{'Content-Type': 'application/json', 'Access-Control-Allow-Origin' : '*', 'Access-Control-Allow-Headers': 'Content-Type, Authorization'}