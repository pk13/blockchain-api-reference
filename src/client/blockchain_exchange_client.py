from uuid import uuid4
from websocket import create_connection
import requests
from src.utils.utils import construct_order_object


class BlockchainWebsocketClient():
    def __init__(self, env, url, api_key, api_secret, symbol, spread_multiplier):
        self.ws = None
        self.env = env
        self.url = url
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.spread_multiplier = spread_multiplier

    def connect(self):
        options = {}
        options['origin'] = 'https://exchange.blockchain.com'
        self.ws = create_connection(self.url, **options)

    def authenticate(self):
        msg = '{"token": "%s"' % (self.api_secret,) + ', "action": "subscribe", "channel": "auth"}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def heartbeat(self):
        msg = '{"action": "subscribe", "channel": "heartbeat"}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def trading(self):
        msg = '{"action": "subscribe", "channel": "trading"}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def get_ticker(self, symbol):
        msg = '{"action": "subscribe", "channel": "ticker", "symbol":"%s"' % (symbol,) + '}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def get_price(self, symbol):
        msg = '{"action": "subscribe", "channel": "prices", "symbol":"%s"' % (symbol,) + ', "granularity":60}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def get_symbols(self):
        msg = '{"action": "subscribe", "channel": "symbols"}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def unsubscribe_symbols(self):
        msg = '{"action": "unsubscribe", "channel": "symbols"}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def get_l2_ob(self, symbol):
        msg = '{"action": "subscribe", "channel": "l3", "symbol":"%s"' % (symbol,) + '}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return result

    def get_trades(self, symbol):
        msg = '{"action": "subscribe", "channel": "trades", "symbol":"%s"' % (symbol,) + '}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)

    def open_orders(self):
        msg = '{"action": "subscribe", "channel": "trading"}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)

    def get_balances(self):
        msg = '{"action": "subscribe", "channel": "balances"}'
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)

    def create_new_limit_order(self, side, orderQty, price, symbol):
        prefix = "AK_"
        ordType = 'limit'
        clOrdID = str(prefix + uuid4().hex[0:10]) + '"'
        netPrice = 0
        order = construct_order_object(self.symbol, ordType, side, orderQty, price)
        if order.side == "buy":
            # netPrice = order.price /(1.0+self.netSpread/10000.0)
            netSpread = (symbol['min_price_increment'] * 10 ** (
                -symbol['min_price_increment_scale'])) * self.spread_multiplier
            netPrice = order.price - netSpread
        else:
            # netPrice = order.price *(1.0+self.netSpread/10000.0)
            netSpread = (symbol['min_price_increment'] * 10 ** (
                -symbol['min_price_increment_scale'])) * self.spread_multiplier
            netPrice = order.price + netSpread
        msg = '{"action": "NewOrderSingle", "channel": "trading", "clOrdID":"%s, "symbol":"%s", "ordType":"%s", "timeInForce": "GTC", "side":"%s", "orderQty":"%s", "price":"%s", "execInst":"ALO"' % (
            clOrdID, order.symbol, order.ordType, order.side, order.orderQty, netPrice) + '}'
        # print(msg)
        self.ws.send(msg)
        result = self.ws.recv()
        print('Submitting new order')
        print(result)
        return clOrdID

    def create_new_market_order(self, side, orderQty, price):
        prefix = "AK_"
        ordType = 'market'
        clOrdID = prefix + uuid4().hex[0:10]
        order = construct_order_object(self.symbol, ordType, side, orderQty, price)
        msg = '{"action": "NewOrderSingle", "channel": "trading", "clOrdID":"%s, "symbol":"%s", "ordType":"%s", "timeInForce": "GTC", "side":"%s", "orderQty":"%s", "execInst":"ALO"' % (
            clOrdID, order.symbol, order.ordType, order.side, order.orderQty) + '}'
        # print(msg)
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)
        return clOrdID

    def cancel_order(self, orderID):
        msg = '{"action": "CancelOrderRequest", "channel": "trading", "orderID":"%s"' % orderID + '}'
        # print(msg)
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)

    def mass_cancel(self):
        msg = '{"action": "OrderMassCancelRequest", "channel": "trading"}'
        # print(msg)
        self.ws.send(msg)
        result = self.ws.recv()
        print(result)

    def get_single_order(self, orderID, api_key):
        base = 'https://api.blockchain.com/exchange/order/'
        url = base + orderID
        response = requests.get(
            url,
            headers={'auth_token': api_key},
        )
        json_response = response.json()
        return json_response