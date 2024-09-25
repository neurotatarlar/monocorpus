import requests

from file_utils import read_config
from rich.progress import track

CHUNK_SIZE = 256


def download_file_from_yandex_disk(public_key: str, output_file: str):
    """
    Download file from the Yandex Disk
    """
    config = read_config()
    resp = requests.get(
        url=config['yandex_disk']['download_url'],
        headers={'Authorization': config['yandex_disk']['token']},
        params={
            "public_key": public_key,
        },
        timeout=30
    )
    resp.raise_for_status()
    download_link = resp.json()["href"]
    resp = requests.get(download_link, timeout=30, stream=True)
    resp.raise_for_status()
    iterations = (int(resp.headers.get("Content-Length", 0)) / CHUNK_SIZE) or None
    with open(output_file, "wb") as f:
        for chunk in track(resp.iter_content(chunk_size=256), total=iterations, description=f"Downloading book..."):
            f.write(chunk)

    return output_file
