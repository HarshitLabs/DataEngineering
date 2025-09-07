import functions_framework
from google.cloud import bigquery
import json 
import pandas as pd
from google.cloud import pubsub_v1
from datetime import datetime
import uuid
from google.auth import default
from google.cloud import storage
import io
from pandas_gbq import to_gbq
import requests

def log_error(project_id,error_message,error_details,log_uuid):
    # Extract error message and details
    error_message = str(error_message)
    error_details=str(error_details)
    publisher = pubsub_v1.PublisherClient()
    request_logging_topic = f'projects/{project_id}/topics/logging'
    request_logging_topic_data = json.dumps({"log_uuid":f"{log_uuid}","error_message":f"{error_message}","error_details":f"{error_details}"})
    data_bytes = request_logging_topic_data.encode("utf-8")
    publisher.publish(request_logging_topic,data_bytes)

def load_csv_to_dataframe(bucket_name, file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data))
    return df

@functions_framework.http
def spectacleOrderIngestion(request):
    project_id=default()[1]

    if request.method=='OPTIONS':
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Max-Age": "3600",
        }
        print("responding to preflight")
        return ("", 204, headers)
    
    request_json=request.get_json(silent=True)

    if not request_json or 'fileURL' not in request_json:
        print("Invalid request/fileURL")
        return 'Invalid request/fileURL', 400, {'Content-Type': 'application/json', 'Access-Control-Allow-Origin' : '*','Access-Control-Allow-Headers': 'Content-Type, Authorization'}
    
    try:
        bqclient=bigquery.Client()
        project_id=default()[1]
        uuid_value=str(uuid.uuid1()).upper()
        bucket=request_json['fileURL'].replace('gs://','').split('/')[0]
        filename=request_json['fileURL'].replace('gs://','').split('/',1)[1]
        table_id="Spectacle.OrderFile"
        orderId = request_json.get('orderId','')
        version = request_json.get('version','')
        monthYear = request_json.get('monthYear','')
    
        print_dict={
             'logKey':uuid_value,
             'projectid':project_id,
             'bucket':bucket,
             'filename':filename,
             'tableid':table_id,
             'orderId':orderId,
             'version':version,
             'monthYear':monthYear
        }   
        print(print_dict)

        #Start-region save data into request logging table: Pub-Sub
        ip_address = request.headers.get("x-forwarded-for","ip") 
        current_timestamp = datetime.now()
        publisher = pubsub_v1.PublisherClient()
        request_logging_topic = f"projects/{project_id}/topics/logging"
        request_logging_topic_data = json.dumps({"cloudfunction":"spectacleOrderIngestion","log_uuid":f"{uuid_value}","ipaddress":f"{ip_address}","payload":request_json,"createdon":f"{current_timestamp}"})
        data_bytes = request_logging_topic_data.encode("utf-8")
        publisher.publish(request_logging_topic,data_bytes)
        #endregion: Pub-Sub

        # Load File to dataframe
        df=load_csv_to_dataframe(bucket,filename)

        # Add a new column with same UUID for all rows at index 0
        df.insert(0, "LogKey", uuid_value)

        #Add Column monthYear
        df["MonthYear"]=monthYear

        # Add a new CreatedDate column at the end
        df["CreatedDate"]=datetime.now()


        # Load Data to table Spectacle.OrderFile
        to_gbq(df,
             destination_table=table_id,
             project_id=project_id,
             if_exists='append'
        )


        # Query to return the required JSON
        query_sp=f"""
            WITH Aggregated AS (
                SELECT 
                    StoreNumber,
                    SpectacleCode,
                    SUM(SumOfTotalQty) AS quantity
                FROM `{table_id}`
                WHERE LogKey='{uuid_value}'
                GROUP BY StoreNumber, SpectacleCode
                ),
                SegmentsArray AS (
                SELECT ARRAY_AGG(STRUCT(StoreNumber, SpectacleCode, quantity)) AS segments
                FROM Aggregated
                ),
                TotalQuantity AS (
                SELECT SUM(quantity) AS orderQuantity FROM Aggregated
                )
                SELECT TO_JSON_STRING(STRUCT(
                    {orderId} AS orderId,                               
                    {version} AS version,                                 
                    '{uuid_value}' AS logKey,                                      
                    (SELECT orderQuantity FROM TotalQuantity) AS orderQuantity,
                    (SELECT segments FROM SegmentsArray) AS segments
                )) AS payload_json;

        """
        
        print(query_sp)
        query_sp_output=bqclient.query(query_sp).result()
        order_json = json.loads(next(query_sp_output)['json_result'])
        order_json["filePath"]='/'.join(filename.split('/')[:-1])
        print(order_json)


        if order_json:
            print("Making POST request to Prospects Estimator")
            prospect_estimator_url=f'https://us-central1-{project_id}.cloudfunctions.net/prospectEstimator'
            print(prospect_estimator_url)
            response=requests.post(prospect_estimator_url,json=order_json,headers={'Content-Type': 'application/json'})
            response_status_code=response.status_code
            response_json=response.json()
            print(response_json)
            return json.dumps(response_json,indent=2),response_status_code
        else:
            return json.dumps({"Status":"Failed! No order_json found"},indent=2),404


    except Exception as e:
            print("exception:",  e)    
            error_message="Exception block executed"
            log_error(project_id,error_message,e,uuid_value)
            return json.dumps({"status":"Failed","message":str(e)}), 400,{'Content-Type': 'application/json', 'Access-Control-Allow-Origin' : '*', 'Access-Control-Allow-Headers': 'Content-Type, Authorization'}
        

