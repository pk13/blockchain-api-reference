import time
import json
import asyncio
import pdb
import configparser
from blockchain_exchange_client import BlockchainWebsocketClient


def derive_keys_from_config(env):
    url = ''
    api_key = ''
    api_secret = ''
    if env == 'prod':
        url = 'wss://ws.prod.blockchain.info/mercury-gateway/v1/ws'
        api_key = config['ApiKeyProd']
        api_secret = config['ApiSecretProd']
    elif env == 'staging':
        url = 'wss://ws.staging.blockchain.info/mercury-gateway/v1/ws'
        api_key = config['ApiKeyStaging']
        api_secret = config['ApiSecretStaging']
    else:
        pass
    return url, api_key, api_secret


def lookup_orderID(clOrdId, orders_store):
    return orders_store[clOrdId]['orderID']


async def process_responses():
    def handle_symbols(res):
        if (res['channel'] == 'symbols' and res['event'] == 'snapshot'):
            for key, val in res['symbols'].items():
                symbols_store[key] = val
        elif (res['channel'] == 'symbols' and res['event'] == 'updated'):
            idx = res['base_currency'] + '-' + res['counter_currency']
            symbols_store[idx] = res
            # pdb.set_trace()

    def handle_prices(res, prices_store):
        if (res['channel'] == 'ticker'):
            prices_store[res['symbol']] = res['price_24h']
        return prices_store

    def handle_tob(res):
        if (res['channel'] == 'l2' and res['event'] == 'updated'):
            mpx = 0
            if res['bids'][0]['px'] != [] and res['asks'][0]['px'] != []:
                mpx = (res['bids'][1]['px'] + res['bids'][1]['px']) / 2.0
            print(res)
            return mpx

    def handle_balances(res):
        if (res['channel'] == 'balances' and res['event'] == 'snapshot'):
            for b in res['balances']:
                balances_store[b['currency']] = b

    def handle_order_updates(res):
        # Keeping track of order status
        if (res['channel'] == 'trading'):
            if res['event'] == 'snapshot':
                for o in res['orders']:
                    orders_store[o['clOrdId']] = o
                return orders_store
            elif res['event'] == 'updated':
                orders_store[res['clOrdId']] = res
            else:
                Exception('Unknown event type. Exiting')

    # Market-orders
    def place_market_order(res, sent_order, price_store, symbols_store):
        clOrdId = ''
        if (mode == "market_orders" and sent_order == False and symbols_store != {} and symbols_store[
            symbol] != {}):
            orderQty_multiplier = 2.0  # how many times more than the minimum size we want to quote
            orderQty = (symbols_store[symbol]['min_order_size'] * 10 ** (
                -symbols_store[symbol]['min_order_size_scale'])) * orderQty_multiplier
            clOrdId = client.create_new_market_order(side='buy', orderQty=orderQty, price=price_store[symbol])
            sent_order = True
        return clOrdId, sent_order

    # Limit-orders
    def place_limit_order(res, sent_order, prices_store, symbols_store):
        clOrdId = ''
        if (mode == "limit_orders" and sent_order == False and symbols_store != {} and prices_store != {}):
            # pdb.set_trace(
            print(symbols_store['BTC-USD'])
            try:
                if symbols_store['BTC-USD'] != None and prices_store['BTC-USD'] != None:
                    orderQty_multiplier = 2.0  # how many times more than the minimum size we want to quote
                    orderQty = (symbols_store[symbol]['min_order_size'] * 10 ** (
                        -symbols_store[symbol]['min_order_size_scale'])) * orderQty_multiplier
                    clOrdId = client.create_new_limit_order(side='buy', orderQty=orderQty, price=prices_store[symbol],
                                                 symbol=symbols_store[symbol])
                    sent_order = True
            except:
                print("Cannot find symbol " + symbol)
                pdb.set_trace()
        return clOrdId, sent_order

    def cancel_placed_order(res, clOrdId, sent_order, orders_store):
        if (mode == "limit_orders" and sent_order == True):
            time.sleep(5)
            try:
                client.cancel_order(lookup_orderID(clOrdId, orders_store))
                pdb.set_trace()
                exit()
            except:
                print('Order not accepted yet')
        return sent_order, clOrdId

    symbols_store = {}
    prices_store = {}
    balances_store = {}
    orders_store = {}
    tob_store = {}
    sent_order = False

    while (1):  # monolithic, single-threaded and event-driven arch
        result = client.ws.recv()
        res = json.loads(result)
        print(res)
        handle_symbols(res)
        handle_balances(res)
        await asyncio.sleep(0.2)
        handle_tob(res)
        prices_store = handle_prices(res, prices_store)
        orders_store = handle_order_updates(res)
        clOrdId, sent_order = place_market_order(res, sent_order, prices_store, symbols_store)
        if prices_store != None and symbols_store != None and balances_store != None:
            clOrdId, sent_order = place_limit_order(res, sent_order, prices_store, symbols_store)
        cancel_placed_order(res, clOrdId, sent_order, orders_store)
        await asyncio.sleep(0.1)
        # client.ws.close()


if __name__ == '__main__':
    config_all = configparser.ConfigParser()
    config_all.read('config_params')
    config = config_all['DEFAULT']

    mode = config['Mode']
    symbol = config['Symbol']
    env = config['Env']
    spread_multiplier = config['SpreadMultiplier']
    url, api_key, api_secret = derive_keys_from_config(env)
    client = BlockchainWebsocketClient(env, url, api_key, api_secret, symbol, spread_multiplier)
    client.connect()

    # Subscribe to the following channels
    client.heartbeat()
    client.authenticate()
    client.get_ticker(symbol)
    client.get_price(symbol)
    client.get_l2_ob(symbol)
    client.get_trades(symbol)
    client.get_balances()
    client.get_symbols()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_responses(), )
