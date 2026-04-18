def run_argo(bucket_name, aws_access_key_id, aws_secret_access_key):
    import boto3
    import requests
    import re

    # Initialize the S3 client
    s3 = boto3.client('s3', 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key)

    # The unique keys found in the SEANOE dataset table
    file_keys = ['127233', '127234', '126470', '126471', '125529']
    base_url = 'https://www.seanoe.org/data/00961/107233/data/'

    for key in file_keys:
        download_url = base_url + key
        print(f"Connecting to {download_url}...")
        
        with requests.get(download_url, stream=True) as response:
            response.raise_for_status()
            
            content_disposition = response.headers.get('Content-Disposition', '')
            filename = f"argo_{key}.nc"
            
            if 'filename=' in content_disposition:
                matches = re.findall('filename="?([^";]+)"?', content_disposition)
                if matches:
                    filename = matches[0]
                    
            s3_key = f"easy_one_argo/{filename}"
            s3.upload_fileobj(response.raw, bucket_name, s3_key)

        print("finished uploads - EASY ONE ARGO")
