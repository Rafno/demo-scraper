import http.client
from dagster import ConfigurableResource,InitResourceContext, InitResourceContext
from pydantic import PrivateAttr



class AlgoliaAPI(ConfigurableResource):
    algolia_url: str
    algolia_api_key: str
    algolia_application_id: str
    _headers_list: dict[str, str] = PrivateAttr()
    _client: http.client.HTTPSConnection = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = http.client.HTTPSConnection(self.algolia_url)
        self._headers_list = {
            "Accept": "*/*",
            "User-Agent": "Thunder Client (https://www.thunderclient.com)",
            "X-Algolia-API-Key": f"{self.algolia_api_key}",
            "X-Algolia-Application-Id": f"{self.algolia_application_id}",
            "Content-Type": "application/json",
            "Referer": "https://www.silversea.com/" 
            }

    def request(self, action, url, payload):
        self._client.request(action, url, payload, self._headers_list)
        return self._client.getresponse()
