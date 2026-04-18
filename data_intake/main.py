from dotenv import load_dotenv
import os 

def download_primary(bucket_name, aws_access_key_id, aws_secret_access_key):
    from cal_cofi import run_cofi
    from cce_moorings import run_cce
    from easy_one_argo import run_argo

    runners = [run_cofi, run_cce, run_argo]
    for runner in runners:
        runner(bucket_name, aws_access_key_id, aws_secret_access_key)





if __name__ == "__main__":
    load_dotenv()
    raw_bucket_name = os.getenv("raw_bucket_name")
    aws_access_key_id = os.getenv("aws_access_key_id")
    aws_secret_access_key = os.getenv("aws_secret_access_key")
    download_primary(raw_bucket_name, aws_access_key_id, aws_secret_access_key)