import io
from typing import Iterator

import pandas as pd
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
    Lazy iterator which yields available dates based on prefixes present in the container.
    This assumes a file structure in Azure Blob Storage like <container>/YYYY/MM/DD/...

    :param container_client: Client pointing to the desired container (bucket).
    :return: Iterator of date prefixes in the format 'YYYY/MM/DD'.
    """
    for year in container_client.walk_blobs(delimiter="/"):
        if not year.name or not year.name.endswith("/"):
            continue

        for month in container_client.walk_blobs(
            delimiter="/", name_starts_with=year.name
        ):
            if not month.name or not month.name.endswith("/"):
                continue
            for day in container_client.walk_blobs(
                delimiter="/", name_starts_with=month.name
            ):
                if day.name and not day.name.endswith("/"):
                    yield day.name
