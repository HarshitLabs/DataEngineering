import base64
import functions_framework
from google.cloud import bigquery
import json
from datetime import datetime



@functions_framework.cloud_event
def requestlogging(cloud_event):
    bq_client = bigquery.Client()

    if 'data' in cloud_event.data['message']:
        request = base64.b64decode(cloud_event.data["message"]["data"])
        request = json.loads(request.decode('utf-8'))
        print(request)
    else:
        request = json.loads(cloud_event.data['message']['attributes']['json_data'])

    # Extract necessary fields from the request
    log_uuid = request.get("log_uuid", "uuid not pass")
    cloudfunction = request.get("cloudfunction", "unknown_function")
    error_message = request.get("error_message", None)
    error_details = request.get("error_details", None)
    payload = json.dumps(request.get("payload", {}))
    ipaddress=request.get("ipaddress", "IP")
    created_on = request.get("createdon",datetime.now())

    try:
        # Set the job configuration with query parameters
        query_params = [
            bigquery.ScalarQueryParameter("log_uuid", "STRING", log_uuid),
            bigquery.ScalarQueryParameter("cloudfunction", "STRING", cloudfunction),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("error_details", "STRING", error_details),
            bigquery.ScalarQueryParameter("payload", "STRING", payload),
            bigquery.ScalarQueryParameter("ipaddress", "STRING", ipaddress),
            bigquery.ScalarQueryParameter("created_on", "TIMESTAMP", created_on)
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        

        # The UPSERT query using MERGE with log_uuid condition
        merge_query = f'''
        MERGE `Spectacle.requestlogging` T
        USING (
            SELECT 
                CAST(@log_uuid AS STRING) AS log_uuid,
                CAST(@cloudfunction AS STRING) AS CloudFunction,
                CAST(@error_message AS STRING) AS error_message,
                CAST(@error_details AS STRING) AS error_details,
                CAST(@payload AS STRING) AS Payload,
                CAST(@ipaddress AS STRING) AS ipaddress,
                @created_on  AS CreatedOn
        ) S
        ON T.log_uuid = S.log_uuid AND S.log_uuid != 'uuid not pass'    
        WHEN MATCHED THEN 
            UPDATE SET 
                error_message = S.error_message,
                error_details = S.error_details
        WHEN NOT MATCHED THEN
            INSERT (log_uuid, CloudFunction, error_message, error_details, Payload, ipaddress, CreatedOn)
            VALUES (
                S.log_uuid, 
                S.CloudFunction, 
                S.error_message, 
                S.error_details, 
                S.Payload, 
                S.ipaddress,
                S.CreatedOn
            );
        '''

        print(merge_query)
        query_exec = bq_client.query(merge_query, job_config=job_config)
        query_exec.result()

        return "Success"
    
    except Exception as e:
        print(f"Error logging failed for uuid {log_uuid} due to {e}")
        return "Failed"
