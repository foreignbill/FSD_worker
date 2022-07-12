import hashlib
import os, sys
from torch.hub import load_state_dict_from_url
import zipfile, tarfile
from tqdm import tqdm
import shutil
import tempfile
import numbers

if sys.version_info[0] == 2:
    from urlparse import urlparse
    from urllib2 import urlopen
else:
    from urllib.request import urlopen
    from urllib.parse import urlparse


def zipdir(path, dstzip):
    # shutil.make_archive(path, 'zip', path)
    path = os.path.abspath(path)
    dir_name = os.path.dirname(path)
    with zipfile.ZipFile(dstzip, 'w', zipfile.ZIP_DEFLATED) as ziph:
        for root, dirs, files in os.walk(path):
            for file in files:
                f_path = os.path.join(root, file)
                ziph.write(os.path.join(root, file), arcname=f_path[len(dir_name):])


def _download_url_to_file(url, dst, hash_prefix=None, progress=True):
    file_size = None
    # We use a different API for python2 since urllib(2) doesn't recognize the CA
    # certificates in older Python
    u = urlopen(url)
    meta = u.info()
    if hasattr(meta, 'getheaders'):
        content_length = meta.getheaders("Content-Length")
    else:
        content_length = meta.get_all("Content-Length")
    if content_length is not None and len(content_length) > 0:
        file_size = int(content_length[0])

    # We deliberately save it in a temp file and move it after
    # download is complete. This prevents a local working checkpoint
    # being overriden by a broken download.
    dst = os.path.expanduser(dst)
    dst_dir = os.path.dirname(dst)
    f = tempfile.NamedTemporaryFile(delete=False, dir=dst_dir)

    try:
        if hash_prefix is not None:
            sha256 = hashlib.sha256()
        with tqdm(total=file_size, disable=not progress,
                  unit='B', unit_scale=True, unit_divisor=1024) as pbar:
            while True:
                buffer = u.read(8192)
                if len(buffer) == 0:
                    break
                f.write(buffer)
                if hash_prefix is not None:
                    sha256.update(buffer)
                pbar.update(len(buffer))

        f.close()
        if hash_prefix is not None:
            digest = sha256.hexdigest()
            if digest[:len(hash_prefix)] != hash_prefix:
                raise RuntimeError('invalid hash value (expected "{}", got "{}")'
                                   .format(hash_prefix, digest))
        shutil.move(f.name, dst)
    finally:
        f.close()
        if os.path.exists(f.name):
            os.remove(f.name)


def download_url_to_file(url, dst_dir, progress=True):
    os.makedirs(dst_dir, exist_ok=True)
    parts = urlparse(url)
    filename = os.path.basename(parts.path)
    cached_file = os.path.join(dst_dir, filename)
    if not os.path.exists(cached_file):
        sys.stderr.write('Downloading: "{}" to {}\n'.format(url, cached_file))
        # hash_prefix = HASH_REGEX.search(filename).group(1) if check_hash else None
        _download_url_to_file(url, cached_file, None, progress=progress)

    if zipfile.is_zipfile(cached_file):
        with zipfile.ZipFile(cached_file) as cached_zipfile:
            members = cached_zipfile.infolist()
            extraced_name = members[0].filename.split('/')[0]
            cached_file = os.path.join(dst_dir, extraced_name)
            if os.path.exists(cached_file):
                return cached_file
            cached_zipfile.extractall(dst_dir)

    elif tarfile.is_tarfile(cached_file):
        with tarfile.open(cached_file) as cached_tarfile:
            cached_tarfile.extractall(dst_dir)
            extraced_name = cached_tarfile.getnames()[0].split('/')[0]
            cached_file = os.path.join(dst_dir, extraced_name)
    return cached_file


def load_model_from_url(url, model_dir=None, map_location=None, progress=True, check_hash=False):
    if model_dir is None:
        atlas_home = os.path.expanduser('~/FSD_cache')
        model_dir = os.path.join(atlas_home, 'model')
    cached_file = download_url_to_file(url, model_dir, progress=progress)
    return cached_file  # serialize.load( cached_file )


def load_dataset_from_url(url, data_dir=None, progress=True, check_hash=False):
    if data_dir is None:
        atlas_home = os.path.expanduser('~/FSD_cache')
        data_dir = os.path.join(atlas_home, 'data')
    cached_file = download_url_to_file(url, data_dir, progress=progress)
    return cached_file

def load_algorithm_from_url(url, algorithm_dir=None, progress=True, check_hash=False):
    if algorithm_dir is None:
        atlas_home = os.path.expanduser('~/FSD_cache')
        algorithm_dir = os.path.join(atlas_home, 'algorithm')
    cached_file = download_url_to_file(url, algorithm_dir, progress=progress)
    return cached_file
