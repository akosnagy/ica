import _thread
import json
import logging
import os
import sys
import time

import click
import jsonlines as jsl
import requests
import websocket

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s;%(levelname)s;%(message)s')

ALL_CHANNELS = ['book', 'ohlc', 'spread', 'ticker', 'trade', 'all']


class FileWriter:

    def __init__(self, path):
        self.writer: jsl.Writer = jsl.open(path, mode='a', flush=True)

    def write_message(self, data):
        self.writer.write(data)

    def close(self):
        self.writer.close()

    def __del__(self):
        self.close()


def pair_writer(url, pair, channel, depth, writer: FileWriter):
    def pair_open(ws):
        event = {"event": "subscribe",
                 "subscription":
                     {"name": channel},
                 "pair": [pair]}
        if channel == 'book':
            event['subscription']['depth'] = depth
        ws.send(json.dumps(event))

    def pair_message(ws, str_message):
        nonlocal pair, channel, writer
        message = json.loads(str_message)
        if isinstance(message, dict):
            event = message['event']
            if event == 'systemStatus':
                logging.info(f'{event}: {message["status"]}, pair: {pair}, channelName: {channel}')
            elif event == 'subscriptionStatus':
                logging.info(f'{event}: {message["status"]}, pair: {pair}, channelName: {channel}')
                if 'errorMessage' in message:
                    logging.error(message['errorMessage'])
            else:
                logging.debug(f'{event}: pair: {pair}, channelName: {channel}')
        elif isinstance(message, list):
            writer.write_message(message)

    def pair_error(ws, exception):
        nonlocal pair, channel
        logging.error(f'Exception for pair: {pair}, channelName: {channel} - {exception}')

    exc_caught = connect_and_run(url, pair_open, pair_message, pair_error)

    while exc_caught:
        wait = 5
        logging.warning(f'Reconnecting for: {pair}, {channel} in {wait} seconds')
        time.sleep(wait)
        exc_caught = connect_and_run(url, pair_open, pair_message, pair_error)


def connect_and_run(url, on_open, on_message, on_error):
    logging.debug(f'Connecting to {url}')
    ws = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error)
    logging.debug(f'Connected')
    return ws.run_forever()


@click.command()
@click.option('--kraken-api', '-a',
              default='https://api.kraken.com', show_default=True,
              help='The kraken API endpoint')
@click.option('--kraken-ws', '-w',
              default='wss://ws.kraken.com', show_default=True,
              help='The kraken WebSockets endpoint')
@click.option('--pairs', '-p', multiple=True,
              default=['BTC/USD', 'ETH/USD'], show_default=True,
              help='Pairs to collect order book messages for')
@click.option('--channels', '-c', multiple=True,
              type=click.Choice(ALL_CHANNELS, case_sensitive=False),
              default=['book'], show_default=True,
              help=f'Channels to subscribe to')
@click.option('--depth', '-d',
              default=1000, show_default=True,
              help='The depth of order book levels')
@click.option('--output-folder', '-o',
              type=click.Path(exists=True, file_okay=False, writable=True),
              default='../output', show_default=True,
              help='The the kraken WebSockets API endpoint')
def main(kraken_api, kraken_ws, pairs, channels, depth, output_folder):
    if 'all' in pairs:
        pairs = get_asset_pairs(kraken_api)

    if 'all' in channels:
        channels = ALL_CHANNELS

    for pair in pairs:
        for channel in channels:
            if channel == 'all':
                continue
            try:
                path = os.path.join(output_folder, f'{pair.replace("/", "_")}_{channel}.jsonl')
                writer = FileWriter(path)
                logging.info(f'{channel} messages for {pair} will be written into {path}')
                logging.info(f'Starting writer for: {pair}, {channel}')
                _thread.start_new_thread(pair_writer, (kraken_ws, pair, channel, depth, writer))
                # Sleep a bit in order to avoid "Handshake status 429 Too Many Requests"
                time.sleep(3)
            except Exception as e:
                logging.error(f'Cannot save messages for {pair}, {channel} due to:\n{e}')

    while True:
        time.sleep(5)
        logging.debug("Main thread: %d" % time.time())


def get_asset_pairs(base_url):
    url = f'{base_url}/0/public/AssetPairs'
    resp = requests.get(url)
    data = resp.json()
    pairs = []
    for asset in data['result'].values():
        if 'wsname' in asset.keys():
            pairs.append(asset['wsname'])
    return pairs


if __name__ == '__main__':
    main()
