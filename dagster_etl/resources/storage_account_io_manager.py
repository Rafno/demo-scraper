from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime
from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr
import json

class StorageAccountIoManager(ConfigurableResource):
    account_name : str
    account_key : str
    container_name : str

    _blob_service_client:BlobServiceClient = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._blob_service_client = BlobServiceClient(account_url=f"https://{self.account_name}.blob.core.windows.net", credential=self.account_key)
        

    def upload_blob(self, context, json_data: str, blob_name: str, competitor: str) -> None:
        context.log.info(f"Attempting to store {blob_name}")
        try:
            # Get a reference to the container
            container_client = self._blob_service_client.get_container_client(self.container_name)

            current_date = datetime.now()
            year_month_date = current_date.strftime("%Y/%m/%d")
            blob_name = f"{competitor}/{year_month_date}/{blob_name}.json"

            # Create a blob client using the container client and blob name
            blob_client = container_client.get_blob_client(blob_name)

            blob_client.upload_blob(json_data, overwrite=True)

            context.log.info(f"Data uploaded successfully to blob: {blob_name}")

        except Exception as e:
            context.log.warn(f"Error uploading data to blob: {blob_name}")
            context.log.error(e)

    def download_blob(self, context, blob_name:str):
        context.log.info(f"Attempting to download {blob_name}")
        try:
            # Get a reference to the container
            container_client = self._blob_service_client.get_container_client(self.container_name)

            # Get a blob client using the blob name
            blob_client = container_client.get_blob_client(blob_name)

            # Download the blob data
            blob_data = blob_client.download_blob().readall()

            context.log.info(f"Blob downloaded successfully: {blob_name}")

            return blob_data.decode("utf-8")

        except ResourceNotFoundError as e:
            context.log.warn(f"Blob not found: {blob_name}")
            context.log.error(e)
            return None

        except Exception as e:
            context.log.warn(f"Error downloading blob: {blob_name}")
            context.log.error(e)
            return None

    def download_blobs_from_path(self, context, path: str, target_action=None):
        context.log.info(f"Attempting to download blobs from path: {path}")
        try:
            # Get a reference to the container
            container_client = self._blob_service_client.get_container_client(self.container_name)

            # List all blobs with the specified path prefix
            blobs_list = container_client.list_blobs(name_starts_with=path)
            downloaded_blobs = []
            
            if not blobs_list:
                context.log.error("No blobs found for this path.")
                return []


            for blob in blobs_list:
                blob_name = blob.name
                if target_action and target_action not in blob_name: # TODO, make this better
                    context.log.info(f"Skipping blob: {blob_name}")
                    continue

                context.log.info(f"Downloading blob: {blob_name}")

                # Get a blob client using the blob name
                blob_client = container_client.get_blob_client(blob_name)

                # Download the blob data
                blob_data = blob_client.download_blob().readall().decode("utf-8")

                # Split the blob data into individual JSON messages
                json_messages = blob_data.strip().split('\n')

                # Parse each JSON message and add it to the list
                for message in json_messages:
                    try:
                        downloaded_blobs.append(json.loads(message))
                    except json.JSONDecodeError as e:
                        context.log.warn(f"Error decoding JSON message in blob {blob_name}: {e}")

            context.log.info(f"Blobs downloaded successfully from path: {path}")
            return downloaded_blobs

        except Exception as e:
            context.log.warn(f"Error downloading blobs from path: {path}")
            context.log.error(e)
            return []