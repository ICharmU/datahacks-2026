import asyncio
import boto3
import aioboto3
import aiohttp
import botocore
from siphon.catalog import TDSCatalog
from pathlib import Path
import re
import os
import aiofiles
import tarfile
from urllib.parse import urlparse
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
import concurrent.futures
import shutil
from datetime import datetime

# ==============================================================================
# DIMENSION 2 & 3: ISOLATED MULTI-PROCESSING & RUST-BASED PARSING
# ==============================================================================
def validate_dataset(file_path: Path, expectations: dict):
    file_size_kb = file_path.stat().st_size / 1024

    if file_size_kb < 0.1:
        raise ValueError(f"[{file_path.name}] Size {file_size_kb:.2f}KB below 0.1KB limit.")

    ext = file_path.suffix.lower()
    if ext not in expectations:
        return True 

    expected = expectations[ext]

    if ext == '.csv':
        import polars as pl
        lazy_df = pl.scan_csv(file_path, ignore_errors=True)
        actual_cols = len(lazy_df.columns)
        actual_rows = lazy_df.select(pl.len()).collect().item()
        
        if actual_rows < expected["min_rows"] or actual_cols < expected["min_cols"]:
            raise ValueError(
                f"[{file_path.name}] Dimensions failed. "
                f"Expected >={expected['min_rows']}x{expected['min_cols']}. "
                f"Got {actual_rows}x{actual_cols}."
            )

    elif ext == '.nc':
        import xarray as xr
        with xr.open_dataset(file_path, engine='netcdf4') as ds:
            actual_dims = len(ds.dims)
            actual_vars = len(ds.data_vars)
            if actual_dims < expected["min_dims"] or actual_vars < expected["min_data_vars"]:
                raise ValueError(
                    f"[{file_path.name}] NetCDF constraints failed. "
                    f"Got {actual_dims} dims, {actual_vars} vars."
                )

    elif ext in ['.gz', '.tar']:
        if not tarfile.is_tarfile(file_path):
            raise ValueError(f"[{file_path.name}] Corrupted tar archive.")
        
        with tarfile.open(file_path, "r:*") as tar:
            if len(tar.getmembers()) < expected["min_members"]:
                raise ValueError(f"[{file_path.name}] Archive is empty.")

    return True

# ==============================================================================
# MAIN DOWNLOAD ENGINE
# ==============================================================================
class download_s3:
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region):
        self.bucket_name = bucket_name.split(":::")[-1] if ":::" in bucket_name else bucket_name
        self.session_kwargs = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "region_name": region
        }
        self.session = aioboto3.Session()
        
        self.cache_dir = Path("data_intake/cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.semaphore = asyncio.Semaphore(30) 
        
        self.process_pool = concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count())

        self.expectations = {
            ".csv": {"min_rows": 10, "min_cols": 2},
            ".nc": {"min_dims": 1, "min_data_vars": 1},
            ".gz": {"min_members": 1},
            ".tar": {"min_members": 1}
        }

    async def _s3_file_exists_async(self, s3_client, s3_key):
        try:
            await s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return False
            raise e

    # ==============================================================================
    # GENERALIZED DOWNLOADER (For Argo, CalCOFI, CCE, NOAA)
    # ==============================================================================
    async def _download_sequential(self, http_session, url, cache_path):
        try:
            async with http_session.get(url) as response:
                response.raise_for_status()
                async with aiofiles.open(cache_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(5 * 1024 * 1024):
                        await f.write(chunk)
            return True
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                if cache_path.exists(): cache_path.unlink()
                return False
            raise e

    async def _download_to_cache(self, http_session, url, cache_path, num_parts=6):
        if cache_path.exists():
            return True 

        try:
            async with http_session.head(url, allow_redirects=True) as head_resp:
                headers = head_resp.headers
                content_length = int(headers.get('Content-Length', 0))
                accept_ranges = headers.get('Accept-Ranges', '')

            if accept_ranges != 'bytes' or content_length < 10 * 1024 * 1024:
                return await self._download_sequential(http_session, url, cache_path)

            with open(cache_path, 'wb') as f:
                f.truncate(content_length)

            chunk_size = content_length // num_parts
            ranges = [(i * chunk_size, (i * chunk_size + chunk_size - 1) if i < num_parts - 1 else content_length - 1) for i in range(num_parts)]

            async def download_part(start, end):
                req_headers = {'Range': f'bytes={start}-{end}'}
                async with http_session.get(url, headers=req_headers) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(cache_path, 'r+b') as f:
                        await f.seek(start)
                        async for chunk in resp.content.iter_chunked(2 * 1024 * 1024):
                            await f.write(chunk)

            await asyncio.gather(*[download_part(start, end) for start, end in ranges])
            return True

        except aiohttp.ClientResponseError as e:
            if e.status in [405, 403, 501]: 
                return await self._download_sequential(http_session, url, cache_path)
            if e.status == 404:
                return False
            raise e
        except Exception as e:
            if cache_path.exists(): cache_path.unlink()
            raise e

    # ==============================================================================
    # NASA-SPECIFIC HIGH PERFORMANCE DOWNLOADER
    # ==============================================================================
    async def _download_nasa_optimized(self, http_session, url, cache_path, num_parts=10):
        if cache_path.exists():
            return True 

        try:
            async with http_session.head(url, allow_redirects=True) as head_resp:
                final_url = str(head_resp.url)
                headers = head_resp.headers
                content_length = int(headers.get('Content-Length', 0))
                accept_ranges = headers.get('Accept-Ranges', '')

            if accept_ranges != 'bytes' or content_length < 5 * 1024 * 1024:
                async with http_session.get(final_url) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(cache_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(5 * 1024 * 1024):
                            await f.write(chunk)
                return True

            with open(cache_path, 'wb') as f:
                f.truncate(content_length)

            chunk_size = content_length // num_parts
            ranges = [(i * chunk_size, (i * chunk_size + chunk_size - 1) if i < num_parts - 1 else content_length - 1) for i in range(num_parts)]

            async def download_part(start, end):
                req_headers = {'Range': f'bytes={start}-{end}'}
                async with http_session.get(final_url, headers=req_headers) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(cache_path, 'r+b') as f:
                        await f.seek(start)
                        async for chunk in resp.content.iter_chunked(2 * 1024 * 1024):
                            await f.write(chunk)

            await asyncio.gather(*[download_part(start, end) for start, end in ranges])
            return True

        except aiohttp.ClientResponseError as e:
            if e.status in [405, 403, 501]: 
                return await self._download_sequential(http_session, url, cache_path)
            if e.status == 404:
                return False
            raise e
        except Exception as e:
            if cache_path.exists(): cache_path.unlink()
            raise e

    async def _process_and_upload(self, s3_client, local_path, s3_key):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.process_pool, validate_dataset, local_path, self.expectations)
        
        config = boto3.s3.transfer.TransferConfig(multipart_threshold=50 * 1024 * 1024, max_concurrency=10)
        await s3_client.upload_file(str(local_path), self.bucket_name, s3_key, Config=config)

    # -----------------------------------------------------------------------------
    # DATASET RUNNERS
    # -----------------------------------------------------------------------------
    async def run_cofi(self):
        local_file_path = Path("data_intake/data/cal_cofi")
        tasks = []
        async with self.session.client('s3', **self.session_kwargs) as s3_client:
            if not local_file_path.exists(): return
            for file_path in local_file_path.iterdir():
                if file_path.is_file():
                    async def wrapped_task(fp=file_path):
                        async with self.semaphore:
                            s3_object_key = f'cal_cofi/{fp.name.replace(" ", "_")}'
                            if not await self._s3_file_exists_async(s3_client, s3_object_key):
                                await self._process_and_upload(s3_client, fp, s3_object_key)
                    tasks.append(wrapped_task())
            
            if tasks:
                pbar = tqdm(total=len(tasks), desc="CAL COFI", position=0, leave=True)
                for f in asyncio.as_completed(tasks):
                    await f; pbar.update(1)
                pbar.close()

    async def run_cce(self):
        catalog_urls = [
            'https://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/CCE1/catalog.xml',
            'https://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/CCE2/catalog.xml', 
        ]
        def fetch_catalogs():
            downloads = []
            for catalog_url in catalog_urls:
                cat = TDSCatalog(catalog_url)
                for name, dataset_obj in cat.datasets.items():
                    if 'HTTPServer' in dataset_obj.access_urls:
                        downloads.append((name, dataset_obj.access_urls['HTTPServer']))
            return downloads

        loop = asyncio.get_running_loop()
        urls_to_download = await loop.run_in_executor(None, fetch_catalogs)

        tasks = []
        connector = aiohttp.TCPConnector(limit=0, use_dns_cache=True)
        async with aiohttp.ClientSession(connector=connector) as http_session:
            async with self.session.client('s3', **self.session_kwargs) as s3_client:
                for name, url in urls_to_download:
                    async def wrapped_task(n=name, u=url):
                        async with self.semaphore:
                            s3_key = f"noaa_cce1/{n}"
                            if await self._s3_file_exists_async(s3_client, s3_key): return
                            cache_path = self.cache_dir / n
                            if await self._download_to_cache(http_session, u, cache_path):
                                await self._process_and_upload(s3_client, cache_path, s3_key)
                    tasks.append(wrapped_task())

                if tasks:
                    pbar = tqdm(total=len(tasks), desc="CCE MOORINGS", position=1, leave=True)
                    for f in asyncio.as_completed(tasks):
                        await f; pbar.update(1)
                    pbar.close()

    async def run_argo(self):
        file_keys = ['127233', '127234', '126470', '126471', '125529']
        base_url = 'https://www.seanoe.org/data/00961/107233/data/'

        tasks = []
        connector = aiohttp.TCPConnector(limit=0, use_dns_cache=True)
        timeout = aiohttp.ClientTimeout(total=3600, sock_read=300) 
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as http_session:
            async with self.session.client('s3', **self.session_kwargs) as s3_client:
                for key in file_keys:
                    async def wrapped_task(k=key):
                        async with self.semaphore:
                            download_url = f"{base_url}{k}.tar.gz"
                            filename = f"argo_{k}.tar.gz"
                            s3_key = f"easy_one_argo/{filename}"
                            
                            if await self._s3_file_exists_async(s3_client, s3_key): return
                            cache_path = self.cache_dir / filename
                            if await self._download_to_cache(http_session, download_url, cache_path):
                                await self._process_and_upload(s3_client, cache_path, s3_key)
                    tasks.append(wrapped_task())
                
                if tasks:
                    pbar = tqdm(total=len(tasks), desc="EASY ONE ARGO", position=2, leave=True)
                    for f in asyncio.as_completed(tasks):
                        await f; pbar.update(1)
                    pbar.close()

    async def run_ob_daac(self):
        catalog_urls = [
            "https://www.earthdata.nasa.gov/data/catalog/ob-daac-b05-0",
            "https://www.earthdata.nasa.gov/data/catalog/ob-daac-san-diego-coastal-project-0",
            "https://www.earthdata.nasa.gov/data/catalog/ob-daac-volpe-0"
        ]

        tasks = []
        connector = aiohttp.TCPConnector(limit=0, use_dns_cache=True)
        async with aiohttp.ClientSession(connector=connector) as http_session:
            async with self.session.client('s3', **self.session_kwargs) as s3_client:
                download_list = []
                
                for url in catalog_urls:
                    try:
                        async with http_session.get(url) as resp:
                            resp.raise_for_status()
                            html = await resp.text()
                            match = re.search(r'(C\d+-[A-Z_a-z]+)', html)
                            if not match: continue
                            concept_id = match.group(1)
                            
                        cmr_url = f"https://cmr.earthdata.nasa.gov/search/granules.json?collection_concept_id={concept_id}&page_size=2000"
                        
                        async with http_session.get(cmr_url) as cmr_resp:
                            cmr_resp.raise_for_status()
                            cmr_data = await cmr_resp.json()
                            hits = int(cmr_resp.headers.get('CMR-Hits', 0))
                            
                            for granule in cmr_data.get('feed', {}).get('entry', []):
                                title = granule.get('title', 'unknown_granule')
                                for link in granule.get('links', []):
                                    href = link.get('href', '')
                                    if link.get('rel', '').endswith('data#') or href.endswith(('.csv', '.nc', '.txt', '.sb', '.tar.gz')):
                                        download_list.append((title, href))
                                        break 

                            if hits > 2000:
                                pages = (hits // 2000) + 1
                                async def fetch_cmr_page(page_num):
                                    page_links = []
                                    async with http_session.get(f"{cmr_url}&page_num={page_num}") as p_resp:
                                        p_data = await p_resp.json()
                                        for granule in p_data.get('feed', {}).get('entry', []):
                                            title = granule.get('title', 'unknown_granule')
                                            for link in granule.get('links', []):
                                                href = link.get('href', '')
                                                if link.get('rel', '').endswith('data#') or href.endswith(('.csv', '.nc', '.txt', '.sb', '.tar.gz')):
                                                    page_links.append((title, href))
                                                    break
                                    return page_links

                                results = await asyncio.gather(*[fetch_cmr_page(p) for p in range(2, pages + 1)])
                                for res in results:
                                    download_list.extend(res)

                    except Exception as e: pass

                for name, url in download_list:
                    async def wrapped_task(n=name, u=url):
                        async with self.semaphore:
                            safe_name = re.sub(r'[^\w\s-]', '', n).replace(' ', '_')
                            exts = "".join(Path(urlparse(u).path).suffixes)
                            if exts:
                                if not safe_name.lower().endswith(exts.lower()): safe_name += exts
                            else:
                                if not safe_name.lower().endswith('.txt'): safe_name += '.txt'
                                
                            s3_key = f"ob_daac/{safe_name}"
                            if await self._s3_file_exists_async(s3_client, s3_key): return
                            cache_path = self.cache_dir / safe_name
                            
                            if await self._download_nasa_optimized(http_session, u, cache_path):
                                await self._process_and_upload(s3_client, cache_path, s3_key)
                    
                    tasks.append(wrapped_task())

                if tasks:
                    pbar = tqdm(total=len(tasks), desc="OB DAAC", position=3, leave=True)
                    for f in asyncio.as_completed(tasks):
                        await f; pbar.update(1)
                    pbar.close()

    async def run_noaa_tides(self):
        station = "9410170"
        tasks = []
        connector = aiohttp.TCPConnector(limit=0, use_dns_cache=True)
        
        async with aiohttp.ClientSession(connector=connector) as http_session:
            async with self.session.client('s3', **self.session_kwargs) as s3_client:
                download_list = []
                current_year = datetime.now().year
                current_date = datetime.now().strftime("%Y%m%d")
                
                # Dynamically generate API calls from 2000 up to the current date
                for year in range(2000, current_year + 1):
                    begin_date = f"{year}0101"
                    end_date = current_date if year == current_year else f"{year}1231"
                    
                    url = f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?begin_date={begin_date}&end_date={end_date}&station={station}&product=hourly_height&datum=MLLW&time_zone=gmt&units=metric&application=DataPipeline&format=csv"
                    name = f"noaa_tides_{station}_{year}.csv"
                    download_list.append((name, url))

                for name, url in download_list:
                    async def wrapped_task(n=name, u=url):
                        async with self.semaphore:
                            s3_key = f"noaa_tides/{n}"
                            if await self._s3_file_exists_async(s3_client, s3_key): return
                            cache_path = self.cache_dir / n
                            
                            # Use sequential stream since these CSVs are small (~350KB)
                            if await self._download_sequential(http_session, u, cache_path):
                                await self._process_and_upload(s3_client, cache_path, s3_key)
                                
                    tasks.append(wrapped_task())

                if tasks:
                    pbar = tqdm(total=len(tasks), desc="NOAA TIDES", position=4, leave=True)
                    for f in asyncio.as_completed(tasks):
                        await f; pbar.update(1)
                    pbar.close()

async def download_primary(bucket_name, aws_access_key_id, aws_secret_access_key, region):
    downloader = download_s3(bucket_name, aws_access_key_id, aws_secret_access_key, region)
    
    # 5 distinct tqdm bars
    print("\n\n\n\n\n") 
    
    try:
        await asyncio.gather(
            downloader.run_cofi(),
            downloader.run_cce(),
            downloader.run_argo(),
            downloader.run_ob_daac(),
            downloader.run_noaa_tides()
        )
    finally:
        downloader.process_pool.shutdown(wait=True)

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    
    load_dotenv()
    asyncio.run(download_primary(
        os.getenv("raw_bucket_name"),
        os.getenv("aws_access_key_id"),
        os.getenv("aws_secret_access_key"),
        os.getenv("region")
    ))