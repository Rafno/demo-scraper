from dagster import ConfigurableResource,InitResourceContext
from pydantic import PrivateAttr
import websocket
import uuid
import json


class SilverSeaWebSocketClient(ConfigurableResource):
    
    # Copy the web brower header and input as a dictionary
    _headers = json.dumps({
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'Upgrade',
        'Host': 'api-ws.booking.digital.silversea.com',
        'Origin': 'https://quote.silversea.com',
        'Pragma': 'no-cache',
        'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits',
        'Sec-WebSocket-Key': 'in6dCK/XY9j+9rWAeNRDXQ==',
        'Sec-WebSocket-Version': '13',
        'Upgrade': 'websocket',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
    })

    _url = 'wss://api-ws.booking.digital.silversea.com/'
    _ws: websocket.WebSocket = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._ws = websocket.create_connection(self._url, headers=self._headers)

    def teardown_after_execution(self, context:InitResourceContext) -> None:
        if self._ws:
            self._ws.close()

    def send_and_receive(self,context, action, fare_code, sail_code, adults, kids, currency, cabin_category) -> str:
        context.log.info(f"sending socket message for {sail_code}, {cabin_category}, {action}-{fare_code}, {adults}, {kids}")
        self.send_message(action=action, fare_code=fare_code, sail_code=sail_code, adults=adults, kids=kids, currency=currency, cabin_category=cabin_category)
        received_message = self.receive_message()   
        return str(received_message)

    def send_message(self, action, sail_code, adults:int, kids:int, fare_code, cabin_category=None, currency='US'):
        string_uuid = str(uuid.uuid4())
        data = {
                "cruiseCode":f"{sail_code}",
                "occupancy":{"adults":adults,"kids":kids},
                "fareCode":f"{fare_code}",
                "suiteCategory":f"{cabin_category}",
                "preferredSuiteNumber":None,
                "vsMember":False,
                "availabilities":["standard","guaranteed","partial"],
                "air":{"type":"notAvailable"}
                }
        if self._ws:
            self._ws.send(json.dumps({"country":f"{currency}", "action":action,
            "data":data,
            "requestId":string_uuid}))

    def receive_message(self):
        if self._ws:
            return self._ws.recv()