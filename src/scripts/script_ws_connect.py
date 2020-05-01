import codecs
import io
import json
import asyncio
import configparser
import pdb

from src.client.blockchain_exchange_client import BlockchainWebsocketClient


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

    symbols_store = {}
    prices_store = {}
    balances_store = {}
    orders_store = {}
    tob_store = {}

    while (1):  # monolithic, single-threaded and event-driven arch
        result = client.ws.recv()
        res = json.loads(result)
        print(res)
        handle_symbols(res)
        handle_balances(res)
        handle_tob(res)
        prices_store = handle_prices(res, prices_store)
        orders_store = handle_order_updates(res)
        # client.ws.close()


if __name__ == '__main__':
    # buf = io.StringIO('config_params.ini')
    parser = configparser.SafeConfigParser()
    parser.read('config_params.ini')
    with codecs.open('../../config_params.ini', 'r', encoding='utf-8') as f:
        parser.read_file(f)
    mode = parser.get('DEFAULT', 'Mode')
    symbol = parser.get('DEFAULT', 'Symbol')
    env = parser.get('DEFAULT', 'Env')
    spread_multiplier = parser.getfloat('DEFAULT', 'SpreadMultiplier')
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
