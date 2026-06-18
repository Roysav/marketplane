from .. import sdk


class Controller:
    def __init__(self, client: sdk.MarketplaneClient):
        self._client = client
