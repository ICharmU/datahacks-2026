def run_cce(bucket_name, aws_access_key_id, aws_secret_access_key):
    import boto3
    import urllib.request
    from siphon.catalog import TDSCatalog

    s3 = boto3.client('s3', 
                    aws_access_key_id=aws_access_key_id, 
                    aws_secret_access_key=aws_secret_access_key)

    catalog_urls= [
        'https://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/CCE1/catalog.xml',
        'https://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/CCE2/catalog.html',
    ]

    for catalog_url in catalog_urls:
        cat = TDSCatalog(catalog_url)

        for dataset_name, dataset_obj in cat.datasets.items():
            if 'HTTPServer' in dataset_obj.access_urls:
                download_url = dataset_obj.access_urls['HTTPServer']
                with urllib.request.urlopen(download_url) as response:
                    s3.upload_fileobj(response, bucket_name, f"noaa_cce1/{dataset_name}")

    print("finished uploads - CCE MOORINGS")