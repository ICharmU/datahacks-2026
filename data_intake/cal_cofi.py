def run_cofi(bucket_name, aws_access_key_id, aws_secret_access_key):
    import boto3
    from pathlib import Path
    import os

    s3 = boto3.client('s3', 
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)


    local_file_path = Path("data_intake/data") / "cal_cofi" 

    bucket_name = 'your-target-bucket'
    for fname in os.listdir(local_file_path):
        s3_object_key = f'raw_data/{fname.replace(" ", "_")}'

        try:
            s3.upload_file(local_file_path / fname, bucket_name, s3_object_key)
        except Exception as e:
            print(f"Uploaded failed for {local_file_path / fname}")
            print(f"Upload failed: {e}")

    print("finished uploads - CAL COFI")