import json
import boto3
import gzip
import pandas as pd
from io import StringIO

s3 = boto3.client('s3')


def get_data(item, data):
    try:
        data["Event_time"] = item.get("eventTime", "NA")
        data["Resource_Name"] = item.get("eventSource", "NA")
        data["Event_name"] = item.get("eventName", "NA")
        data["Region"] = item.get("awsRegion", "NA")
        
        user_identity = item.get("userIdentity", {})
        data["account_id"] = user_identity.get("accountId", "NA")
        
        user_identity = item.get("userIdentity", {})
        data["User_name"] = user_identity.get("userName", "NA")
        
        user_identity = item.get("userIdentity", {})
        session_context = user_identity.get("sessionContext", {})
        attributes = session_context.get("attributes", {})
        data["Creation_Date"] = attributes.get("creationDate", "NA")
        
        resources=item.get("resources", {})
        print("Resource_Type--------",resources)
        
        if not resources:
            data["Resource_Type"] = "NA"
        else:
            # If 'resources' is a non-empty dictionary, update 'Resource_Type' with its value
            resource_types = [res.get("type", "NA") for res in resources]
            data["Resource_Type"] = resource_types
        responseElements=item.get("responseElements",{})
        if responseElements!=None:
            data["Resource_ID"]=responseElements.get("groupId","NA")
        else:
            data["Resource_ID"]="NA"
        
        data["delta"] = item.get("requestParameters", "NA")
        data["Request_ID"] = item.get("requestID", "NA")
        return data
    except Exception as e:
        print("Exception while getting the data in get_data function", e)



def lambda_handler(event, context):
    print(event)
    output_csv_key = 'Outputfile/cloudtrial_output.csv'
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        print("bucket_name",bucket_name)
        print("object_key",object_key)
        
        try:
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            # Access the JSON content from the object
            compressed_data = response['Body'].read()

            # Decompress the Gzip file
            decompressed_data = gzip.decompress(compressed_data).decode('utf-8')
    
            # Process the JSON content
            json_content = json.loads(decompressed_data)

            # Process the JSON content (for example, print it)
            print("json content",json_content)
            extracted_data = []
            
            for item in json_content.get("Records", []):
                if isinstance(item, dict):  # Check if 'item' is a dictionary
                    data = {}
                    data["Type"] = item["userIdentity"]["type"]
                    data = get_data(item, data)
                    extracted_data.append(data)
                else:
                    print("Skipping invalid record:", item)
                
            print("extracted_data",extracted_data)
            df = pd.DataFrame(extracted_data)
            
            existing_df = pd.DataFrame()
            print("existing_df",existing_df)
            try:
                existing_csv_object = s3.get_object(Bucket=bucket_name, Key=output_csv_key)
                existing_df = pd.read_csv(existing_csv_object['Body'])
                print("existing_df------",existing_df)
            except Exception as e:
                print(e)
        
            # Append new data to existing DataFrame
            updated_df = pd.concat([existing_df, df], ignore_index=True)
        
            # Write the updated DataFrame to CSV
            csv_buffer = StringIO()
            updated_df.to_csv(csv_buffer, index=False)
        
            s3.put_object(Bucket=bucket_name, Key=output_csv_key, Body=csv_buffer.getvalue())

            return {
                'statusCode': 200,
                'body': 'CSV updated and uploaded to S3'
            }

                    
        except Exception as e:
            print(e)
            return {
                'statusCode': 500,
                'body': 'Error retrieving JSON file from S3'
            }

        
