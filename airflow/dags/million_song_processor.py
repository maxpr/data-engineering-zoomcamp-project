import pandas as pd
import numpy as np
import helpers_million_songs
from typing import List
import glob
from tqdm import tqdm
from google.cloud import storage


def get_total_features(path: str) -> List[str]:
    ''' 
    Load the features name from the metadata into a list so that we don't have to insert them manually.
    
    :param: path: The path of one file to fethc metadata from.
    
    :return: The list of feature name
    '''

    h5_summary = helpers_million_songs.open_h5_file_read(path)

    metadata = h5_summary.get_node('/metadata/songs/').colnames
    metadata.remove('genre')
    metadata.remove('analyzer_version')
    metadata = [w.replace('idx_', '') for w in metadata]

    analysis = h5_summary.get_node('/analysis/songs/').colnames
    analysis = [w.replace('idx_', '') for w in analysis]

    musicbrainz = h5_summary.get_node('/musicbrainz/songs/').colnames
    musicbrainz = [w.replace('idx_', '') for w in musicbrainz]

    total_features = np.array(metadata + analysis + musicbrainz).ravel()

    total_features = np.append(total_features, ['artist_terms_freq', 'artist_terms_weight', 'artist_mbtags_count'])


    total_features = np.sort(total_features)

    return total_features

def process_one_file(path, categories):
    h5file = helpers_million_songs.open_h5_file_read(path)
    datapoint = {}
    for cat in categories:
        datapoint[cat] = getattr(helpers_million_songs, "get_"+cat)(h5file)
    h5file.close()
    return datapoint

def load_song_data(file_paths: List[str]):
    categories = get_total_features(file_paths[0])
    data = []

    for p in tqdm(file_paths):
        data.append(process_one_file(p, categories))

    df = pd.DataFrame(data)
    return df


def million_to_parquet(original_path: str, output_path: str) -> None:    
    all_paths = glob.glob(original_path + '/**/*.h5', recursive=True)
    df = load_song_data(all_paths)
    
    df.drop(columns=['segments_pitches'], inplace=True)
    df.drop(columns=['segments_timbre'], inplace=True)
    df['artist_mbtags'] = df['artist_mbtags'].apply(lambda x : x if x != [] else [''])
    df['artist_terms'] = df['artist_terms'].apply(lambda x : x if x != [] else [''])

    df.to_parquet(output_path)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file) -> None:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
