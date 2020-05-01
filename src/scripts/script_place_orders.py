import codecs
import time
import json
import configparser

from client.blockchain_exchange_client import BlockchainWebsocketClient


def derive_keys_from_config(env):
    url = ''
    api_key = ''
    api_secret = ''
    if env == 'prod':
        url = 'wss://ws.prod.blockchain.info/mercury-gateway/v1/ws'
        api_key = parser.get('DEFAULT', 'ApiKeyProd')
        api_secret = parser.get('DEFAULT', 'ApiSecretProd')
    elif env == 'staging':
        url = 'wss://ws.staging.blockchain.info/mercury-gateway/v1/ws'
        api_key = parser.get('DEFAULT', 'ApiKeyStaging')
        api_secret = parser.get('DEFAULT', 'ApiSecretStaging')
    else:
        pass
    return url, api_key, api_secret


def lookup_orderID(clOrdID, orders_store):
    return orders_store[clOrdID]['orderID']


def handle_symbols(res, symbols_store):
    if (res['channel'] == 'symbols' and res['event'] == 'snapshot'):
        for key, val in res['symbols'].items():
            symbols_store[key] = val
    elif (res['channel'] == 'symbols' and res['event'] == 'updated'):
        idx = res['base_currency'] + '-' + res['counter_currency']
        symbols_store[idx] = res
    return symbols_store


def handle_prices(res, prices_store):
    if (res['channel'] == 'ticker') and res['event'] != 'subscribed':
        prices_store[res['symbol']] = res['price_24h']
    return prices_store


def handle_tob(res):
    if (res['channel'] == 'l2' and res['event'] == 'updated'):
        mpx = 0
        if res['bids'][0]['px'] != [] and res['asks'][0]['px'] != []:
            mpx = (res['bids'][1]['px'] + res['bids'][1]['px']) / 2.0
        return mpx


def handle_balances(res, balances_store):
    if (res['channel'] == 'balances' and res['event'] == 'snapshot'):
        for b in res['balances']:
            balances_store[b['currency']] = b
    return balances_store


def handle_order_updates(res, orders_store):
    # Keeping track of order status
    if (res['channel'] == 'trading'):
        if res['event'] == 'snapshot':
            for o in res['orders']:
                orders_store[o['clOrdID']] = o
            return orders_store
        elif res['event'] == 'updated':
            if res['ordStatus']== 'open':
                orders_store[res['clOrdID']] = res
        else:
            Exception('Unknown event type. Exiting')
    return orders_store


# Market-orders
def place_market_order(sent_order, price_store, symbols_store):
    clOrdID = ''
    if (mode == "market_orders" and sent_order == False and symbols_store != {} and symbols_store[
        symbol] != {}):
        orderQty = (symbols_store[symbol]['min_order_size'] * 10 ** (
            -symbols_store[symbol]['min_order_size_scale'])) * order_size_multiplier
        clOrdID = client.create_new_market_order(side='buy', orderQty=orderQty, price=price_store[symbol])
        sent_order = True
    return clOrdID, sent_order


# Limit-orders
def place_limit_order(open_orders_buy, open_orders_sell, side, prices_store, symbols_store):
    clOrdID = ''
    if (mode == "limit_orders" and symbols_store != {} and prices_store != {}):
        print(symbols_store['BTC-USD'])
        try:
            if symbols_store['BTC-USD'] != None and prices_store['BTC-USD'] != None:
                orderQty = (symbols_store[symbol]['min_order_size'] * 10 ** (
                    -symbols_store[symbol]['min_order_size_scale'])) * order_size_multiplier
                clOrdID = client.create_new_limit_order(side=side, orderQty=orderQty, price=prices_store[symbol],
                                             symbol=symbols_store[symbol])
                if side == 'buy':
                    open_orders_buy +=1
                else:
                    open_orders_sell +=1
        except:
            print("Cannot place order for " + symbol)
    return clOrdID, open_orders_buy, open_orders_sell


def cancel_placed_order(clOrdID, orders_store):
    if (mode == "limit_orders"):
        try:
            client.cancel_order(lookup_orderID(clOrdID, orders_store))
        except:
            print('Order cannot be cancelled with clOrdID: ' + clOrdID + " and id: " + lookup_orderID(clOrdID, orders_store))
    return clOrdID


def parse_response(raw_res):
    res = json.loads(raw_res)
    print(res)
    time.sleep(0.2)
    return res


def cancel_all_open_orders(open_orders_buy, open_orders_sell):
    for order in orders_store:
        if order['side'] == 'buy' and order['ordStatus'] == 'open':
            clOrdID, sent_orders = cancel_placed_order(order['clOrdID'], orders_store)
            open_orders_buy -= 1
        elif order['side'] == 'sell' and order['ordStatus'] == 'open':
            clOrdID, sent_orders = cancel_placed_order(order['clOrdID'], orders_store)
            open_orders_sell -= 1
        else:
            print('Order side could not be identified')
    time.sleep(2)  # wait before placing new orders

def cancel_mass(client):
    client.mass_cancel()

if __name__ == '__main__':
    parser = configparser.SafeConfigParser()
    parser.read('config_params.ini')
    with codecs.open('../../config_params.ini', 'r', encoding='utf-8') as f:
        parser.read_file(f)
    mode = parser.get('DEFAULT', 'Mode')
    symbol = parser.get('DEFAULT', 'Symbol')
    env = parser.get('DEFAULT', 'Env')
    spread_multiplier = parser.getfloat('DEFAULT', 'SpreadMultiplier')
    order_size_multiplier = parser.getfloat('DEFAULT', 'OrderSizeMultiplier') # how many times the minimum size we want to quote
    update_frequency = parser.getfloat('DEFAULT', 'UpdateFrequency')
    max_orders = parser.getint('DEFAULT', 'MaxOrders')

    url, api_key, api_secret = derive_keys_from_config(env)
    client = BlockchainWebsocketClient(env, url, api_key, api_secret, symbol, spread_multiplier)

    symbols_store = {}
    prices_store = {}
    balances_store = {}
    orders_store = {}
    trades_store = {}
    tob_store = {}
    open_orders_buy = 0
    open_orders_sell = 0
    price_axes_buy = []
    price_axes_sell = []

    client.connect()
    # Subscribe to the following channels
    client.heartbeat()
    client.authenticate()
    client.trading()
    raw_res = client.get_ticker(symbol)
    if raw_res != None:
        res = parse_response(raw_res)

    raw_res = client.get_price(symbol)
    if raw_res != None:
        res = parse_response(raw_res)
        prices_store = handle_prices(res, prices_store)

    raw_res  = client.get_l2_ob(symbol)
    if raw_res != None:
        res = parse_response(raw_res)
        mpx = handle_tob(res)

    raw_res = client.get_trades(symbol)
    if raw_res != None:
      res = parse_response(raw_res)

    raw_res = client.get_balances()
    if raw_res != None:
        res = parse_response(raw_res)
        balances_store = handle_balances(res, balances_store)

    raw_res = client.get_symbols()
    if raw_res != None:
        res = parse_response(raw_res)
        symbols_store = handle_symbols(res, symbols_store)

    cancel_mass(client)

    while (1):  # monolithic, single-threaded and event-driven arch
        raw_res = client.ws.recv()
        if raw_res != None: # update any of the stores
            res = parse_response(raw_res)
            orders_store = handle_order_updates(res, orders_store)
            balances_store = handle_balances(res, balances_store)
            symbols_store = handle_symbols(res, symbols_store)
            mpx = handle_tob(res)
            prices_store = handle_prices(res, prices_store)

        if prices_store != {} and prices_store !={} and symbols_store != {} and balances_store != {} and (open_orders_buy < max_orders or open_orders_sell < max_orders):
            if open_orders_buy < max_orders:
                side = 'buy'
                clOrdID, open_orders_buy, open_orders_sell = place_limit_order(open_orders_buy, open_orders_sell, side, prices_store, symbols_store)
                raw_res = client.ws.recv() # make sure order is recorded on the orders_store
                if raw_res != None:  # update any of the stores
                    res = parse_response(raw_res)
                    orders_store = handle_order_updates(res, orders_store)
                price_axes_buy.append(prices_store[symbol])
                print('I placed buy orders at midprice ' + str(prices_store[symbol]))
            if open_orders_sell < max_orders:
                side = 'sell'
                clOrdID, open_orders_buy, open_orders_sell = place_limit_order(open_orders_buy, open_orders_sell, side, prices_store, symbols_store)
                raw_res = client.ws.recv() # make sure order is recorded on the orders_store
                if raw_res != None:  # update any of the stores
                    res = parse_response(raw_res)
                    orders_store = handle_order_updates(res, orders_store)
                price_axes_sell.append(prices_store[symbol])
                print('I placed sell orders at midprice ' + str(prices_store[symbol]))

        print(orders_store)

        if (open_orders_buy != 0 or open_orders_sell != 0) and orders_store != {} and (abs((prices_store[symbol] - max(price_axes_buy))/max(price_axes_buy)) > update_frequency or abs((prices_store[symbol] - min(price_axes_sell))/min(price_axes_sell)) > update_frequency):
            try: # cancel all orders
                cancel_all_open_orders(open_orders_buy, open_orders_sell)
            except:
                print('No orders to cancel')
        # client.ws.close()