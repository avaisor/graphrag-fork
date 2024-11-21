from google.cloud import storage
import re
import logging

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any


from datashaper import Progress

from graphrag.logging import ProgressReporter

from .pipeline_storage import PipelineStorage

log = logging.getLogger(__name__)


class GcsPipelineStorage(PipelineStorage):
    """The GCS Storage implementation."""

    _bucket_name: str
    _path_prefix: str
    _encoding: str
    _storage_client: storage.Client

    def __init__(
        self,
        connection_string: str | None,
        bucket_name: str,
        encoding: str | None = None,
        path_prefix: str | None = None,
        storage_account_blob_url: str | None = None,
    ):
        """Create a new GcsStorage instance."""
        if not connection_string:
            raise ValueError("A connection_string is required for GCS access.")
        self._connection_string = connection_string
        self._storage_client = storage.Client.from_service_account_json(connection_string)
        self._encoding = encoding or "utf-8"
        self._bucket_name = bucket_name
        self._path_prefix = path_prefix or ""
        log.info(
            "creating GCS storage at bucket=%s, path=%s",
            self._bucket_name,
            self._path_prefix,
        )
        self.create_container()

    def create_container(self) -> None:
        """Create the bucket if it does not exist."""
        if not self.container_exists():
            self._storage_client.create_bucket(self._bucket_name)

    def delete_container(self) -> None:
        """Delete the bucket."""
        if self.container_exists():
            bucket = self._storage_client.bucket(self._bucket_name)
            bucket.delete(force=True)

    def container_exists(self) -> bool:
        """Check if the bucket exists."""
        return self._storage_client.lookup_bucket(self._bucket_name) is not None

    def find(
        self,
        file_pattern: re.Pattern[str],
        base_dir: str | None = None,
        progress: ProgressReporter | None = None,
        file_filter: dict[str, Any] | None = None,
        max_count=-1,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Find blobs in a bucket using a file pattern and custom filter."""
        base_dir = base_dir or ""

        log.info(
            "search bucket %s for files matching %s",
            self._bucket_name,
            file_pattern.pattern,
        )

        def blobname(blob_name: str) -> str:
            if blob_name.startswith(self._path_prefix):
                blob_name = blob_name.replace(self._path_prefix, "", 1)
            if blob_name.startswith("/"):
                blob_name = blob_name[1:]
            return blob_name

        def item_filter(item: dict[str, Any]) -> bool:
            if file_filter is None:
                return True
            return all(re.match(value, item[key]) for key, value in file_filter.items())

        try:
            bucket = self._storage_client.bucket(self._bucket_name)
            all_blobs = list(bucket.list_blobs(prefix=base_dir))

            num_loaded = 0
            num_total = len(all_blobs)
            num_filtered = 0
            for blob in all_blobs:
                match = file_pattern.match(blob.name)
                if match and blob.name.startswith(base_dir):
                    group = match.groupdict()
                    if item_filter(group):
                        yield (blobname(blob.name), group)
                        num_loaded += 1
                        if max_count > 0 and num_loaded >= max_count:
                            break
                    else:
                        num_filtered += 1
                else:
                    num_filtered += 1
                if progress is not None:
                    progress(_create_progress_status(num_loaded, num_filtered, num_total))
        except Exception:
            log.exception(
                "Error finding blobs: base_dir=%s, file_pattern=%s, file_filter=%s",
                base_dir,
                file_pattern,
                file_filter,
            )
            raise

    async def get(self, key: str, as_bytes: bool | None = False, encoding: str | None = None) -> Any:
        """Get a value from storage."""
        try:
            key = self._keyname(key)
            bucket = self._storage_client.bucket(self._bucket_name)
            blob = bucket.blob(key)
            blob_data = blob.download_as_bytes()
            if not as_bytes:
                coding = encoding or "utf-8"
                blob_data = blob_data.decode(coding)
        except Exception:
            log.exception("Error getting key %s", key)
            return None
        else:
            return blob_data

    async def set(self, key: str, value: Any, encoding: str | None = None) -> None:
        """Set a value in storage."""
        try:
            key = self._keyname(key)
            bucket = self._storage_client.bucket(self._bucket_name)
            blob = bucket.blob(key)
            if isinstance(value, bytes):
                blob.upload_from_string(value)
            else:
                coding = encoding or "utf-8"
                blob.upload_from_string(value.encode(coding))
        except Exception:
            log.exception("Error setting key %s: %s", key)

    def set_df_json(self, key: str, dataframe: Any) -> None:
        """Set a JSON dataframe."""
        dataframe.to_json(
            f"gs://{self._bucket_name}/{self._keyname(key)}",
            storage_options={"client": self._storage_client},
            orient="records",
            lines=True,
            force_ascii=False,
        )

    def set_df_parquet(self, key: str, dataframe: Any) -> None:
        """Set a Parquet dataframe."""
        dataframe.to_parquet(
            f"gs://{self._bucket_name}/{self._keyname(key)}",
            storage_options={"client": self._storage_client},
        )

    async def has(self, key: str) -> bool:
        """Check if a key exists in storage."""
        key = self._keyname(key)
        bucket = self._storage_client.bucket(self._bucket_name)
        blob = bucket.blob(key)
        return blob.exists()

    async def delete(self, key: str) -> None:
        """Delete a key from storage."""
        key = self._keyname(key)
        bucket = self._storage_client.bucket(self._bucket_name)
        blob = bucket.blob(key)
        blob.delete()

    async def clear(self) -> None:
        """Clear all keys in the bucket."""
        bucket = self._storage_client.bucket(self._bucket_name)
        blobs = bucket.list_blobs(prefix=self._path_prefix)
        for blob in blobs:
            blob.delete()

    def child(self, name: str | None) -> "PipelineStorage":
        """Create a child storage instance."""
        if name is None:
            return self
        path = str(Path(self._path_prefix) / name)
        return GcsPipelineStorage(
            self._connection_string,
            self._bucket_name,
            self._encoding,
            path,
        )

    def keys(self) -> list[str]:
        """Return the keys in the storage."""
        msg = "GCS storage does not yet support listing keys."
        raise NotImplementedError(msg)

    def _keyname(self, key: str) -> str:
        """Get the key name."""
        return str(Path(self._path_prefix) / key)


def _create_progress_status(num_loaded: int, num_filtered: int, num_total: int) -> Progress:
    return Progress(
        total_items=num_total,
        completed_items=num_loaded + num_filtered,
        description=f"{num_loaded} files loaded ({num_filtered} filtered)",
    )
