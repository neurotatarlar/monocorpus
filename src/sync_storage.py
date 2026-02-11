"""Storage-side operations used by the sync flow."""

from __future__ import annotations

import os

from rich import print


def move_to_filtered_out(file, config, ya_client, parent_dir, entry_point):
    """Move a Yandex Disk file into a filtered-out folder or delete it."""
    filtered_out_dir = config['yandex']['disk']['filtered_out']

    if parent_dir == 'void':
        print(f"[magenta]Removing file '{file.md5}'('{file.path}')[/magenta]")
        ya_client.remove(file.path, n_retries=5, retry_interval=30)
    else:
        old_path = file.path.removeprefix('disk:')
        rel_path = os.path.relpath(old_path, entry_point)
        new_path = os.path.join(filtered_out_dir, parent_dir, rel_path)
        print(f"[cyan]Moving file '{file.md5}' from '{old_path} to '{new_path}'[/cyan]")
        ya_client.create_folders(os.path.dirname(new_path))
        ya_client.move(file.path, new_path, n_retries=5, retry_interval=30, overwrite=True)
        ya_client.unpublish(new_path)


def remove_from_s3(md5s, s3client, config):
    """Remove S3 objects related to the provided MD5s."""
    if not md5s:
        return
    content_bucket = config["yandex"]["cloud"]['bucket']['content']
    content_chunks_bucket = config["yandex"]["cloud"]['bucket']['content_chunks']
    documents_bucket = config["yandex"]["cloud"]['bucket']['document']
    images_bucket = config["yandex"]["cloud"]['bucket']['image']
    upstream_metadatas_bucket = config["yandex"]["cloud"]['bucket']['upstream_metadata']
    metadatas_bucket = config["yandex"]["cloud"]['bucket']['metadata']
    buckets = [content_bucket, content_chunks_bucket, documents_bucket, images_bucket, upstream_metadatas_bucket, metadatas_bucket]
    for bucket in buckets:
        paginator = s3client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket)

        keys_to_remove = []
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if any(key.startswith(md5) for md5 in md5s):
                    keys_to_remove.append({'Key': key})

        if keys_to_remove:
            print(f"Removing {len(keys_to_remove)} objects from bucket '{bucket}'")
            for i in range(0, len(keys_to_remove), 1000):
                batch = keys_to_remove[i:i + 1000]
                s3client.delete_objects(Bucket=bucket, Delete={'Objects': batch, 'Quiet': True})


def publish_file(client, path):
    """Publish a file on Yandex Disk and return its public keys."""
    _ = client.publish(path)
    resp = client.get_meta(path, fields=['public_key', 'public_url'])
    return resp['public_key'], resp['public_url']
