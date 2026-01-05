import io
from typing import Iterator, Set

import pandas as pd
import pendulum
from azure.storage.blob import ContainerClient


def get_csv_as_df(container_client: ContainerClient, blob_name: str) -> pd.DataFrame:
    """
    Load a csv from Azure Blob Storage into a pandas DataFrame.
    Helper function to reduce boilerplate.

    :param container_client: Client pointing to the desired container (bucket).
    :param blob_name: Name of the blob to download.
    :return: DataFrame containing the csv data.
    """
    with container_client.get_blob_client(blob_name) as blob_client:
        data = blob_client.download_blob().readall()
        return pd.read_csv(io.StringIO(data.decode()))


def date_prefixes_for_container(container_client: ContainerClient) -> Iterator[str]:
    """
    Deterministic iterator that yields date prefixes in chronological order (YYYY/MM/DD).
    It scans blob names, extracts the first three path components, deduplicates them
    and yields them sorted by parsed date.
    """
    prefixes: Set[str] = set()

    for blob in container_client.list_blobs():
        parts = blob.name.split("/")
        if (
            len(parts) >= 3
            and parts[0].isdigit()
            and parts[1].isdigit()
            and parts[2].isdigit()
        ):
            # normalize zero-padding
            prefix = f"{parts[0]}/{parts[1].zfill(2)}/{parts[2].zfill(2)}"
            prefixes.add(prefix)

    for prefix in sorted(prefixes, key=lambda s: pendulum.from_format(s, "YYYY/MM/DD")):
        yield prefix
