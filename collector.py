# coding: utf-8
import pytz
import websocket
import time
import sys, os
import datetime
import sched
import time

from google.cloud import bigquery
import google.cloud.logging

# import tracemalloc
# tracemalloc.start()
# ... start your application ...

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

DRY_RUN = False
SUICIDE_FLAG = False
PROJECT_ID = "pandora-154702"

pnconfig = PNConfiguration()

pnconfig.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
# pnconfig.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNub(pnconfig)

import logging

logger = logging.getLogger('bitflyer-collector')
if not DRY_RUN:
    logging_client = google.cloud.logging.Client(PROJECT_ID)
    logger.addHandler(logging_client.get_default_handler())

logger.setLevel(logging.INFO)


class StreamDataProcessing():
    def __init__(self):
        pass

    def stream_data(self, rows):
        pass


class BQStreamInsersion(StreamDataProcessing):
    import threading
    project_id = PROJECT_ID
    dataset_id = "trading"

    last_client_updated = datetime.datetime(1970, 1, 1)
    interval = datetime.timedelta(0, 30, 0)  # 30秒ごとに再接続
    _lock = threading.Lock()

    def __init__(self, table_id):
        self.table_id = table_id
        BQStreamInsersion._refresh_bigquery_client_if_needed()
        self.counter = 0

        self.last_logged = datetime.datetime.now(pytz.timezone('UTC'))
        self.logging_interval = datetime.timedelta(0, 60, 0)
        self._update_table()

    @classmethod
    def _refresh_bigquery_client_if_needed(cls):
        def check_refresh_needed():
            now = datetime.datetime.now()
            with cls._lock:
                diff = now - cls.last_client_updated
                if diff > cls.interval:
                    cls.last_client_updated = now
                    return True
                else:
                    return False

        if check_refresh_needed():
            cls._refresh_bigquery_client()

    @classmethod
    def _refresh_bigquery_client(cls):
        logger.info("refreshing bigquery client")
        cls.bigquery_client = bigquery.Client(cls.project_id)
        cls.dataset_ref = cls.bigquery_client.dataset(cls.dataset_id)

    def preprocess_row(self, row):
        if not "timestamp" in row:
            row["timestamp"] = datetime.datetime.now(pytz.timezone('UTC'))

        if "bf_fx_execution_" in self.table_id:
            newrow = row
            newrow["timestamp"] = datetime.datetime.strptime(row["exec_date"][0:-2], "%Y-%m-%dT%H:%M:%S.%f")
            del newrow["id"]
            del newrow["sell_child_order_acceptance_id"]
            del newrow["buy_child_order_acceptance_id"]
            return newrow
        else:
            return row

    def _update_table(self):
        cls = BQStreamInsersion
        suffix = datetime.datetime.now(pytz.timezone('UTC')).strftime("%Y%m%d")
        table_name = "%s$%s" % (self.table_id, suffix)
        table_ref = cls.dataset_ref.table(table_name)
        self.table = cls.bigquery_client.get_table(table_ref)
        return table_name

    def _ensure_list_rows(self, rows):
        if isinstance(rows, list):
            return rows
        else:
            return [rows]

    def stream_data(self, rows):
        try:
            rows = self._ensure_list_rows(rows)

            cls = BQStreamInsersion
            self._refresh_bigquery_client_if_needed()

            self.counter += len(rows)

            now = datetime.datetime.now(pytz.timezone('UTC'))
            if self.last_logged + self.logging_interval < now:
                import gc
                gc.collect()
                table_name = self._update_table()
                logger.info("inserted %d rows to %s" % (self.counter, table_name))
                self.last_logged = now

            records = list(self.preprocess_row(r) for r in rows)

            if DRY_RUN:
                print(records)
            else:
                if len(records) > 0:
                    errors = cls.bigquery_client.create_rows(self.table, records)
                    if len(errors) > 0:
                        logger.error("errors", errors)

        except Exception as error:
            logger.exception(error)


class PubNubSubscriber(SubscribeCallback):
    def __init__(self, pubnub):
        self.pubnub = pubnub
        self.channel_to_insersion = {}
        pubnub.add_listener(self)

    def add_subscription(self, channel, table):
        logger.info("added subscription to %s using table %s" % (channel, table))
        self.channel_to_insersion[channel] = BQStreamInsersion(table)
        self.pubnub.subscribe().channels(channel).execute()

    def presence(self, pubnub, presence):
        pass  # handle incoming presence data

    def status(self, pubnub, status):
        if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
            logger.info("PNStatusCategory.PNUnexpectedDisconnectCategory")
            pass  # This event happens when radio / connectivity is lost

        elif status.category == PNStatusCategory.PNConnectedCategory:
            logger.info("PNStatusCategory.PNConnectedCategory")
            # Connect event. You can do stuff like publish, and know you'll get it.
            # Or just use the connected event to confirm you are subscribed for
            # UI / internal notifications, etc
            # pubnub.publish().channel("awesomeChannel").message("hello!!").async(my_publish_callback)
            pass

        elif status.category == PNStatusCategory.PNReconnectedCategory:
            logger.info("PNStatusCategory.PNReconnectedCategory")
            # Happens as part of our regular operation. This event happens when
            # radio / connectivity is lost, then regained.
            # elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
            #    pass
            # Handle message decryption error. Probably client configured to
            # encrypt messages and on live data feed it received plain text.

    def message(self, pubnub, message):
        if SUICIDE_FLAG:
            os._exit(0)
        insersion = self.channel_to_insersion.get(message.channel)
        insersion.stream_data(message.message)


class ApiPolling():
    def __init__(self, scheduler, path, table, duration):
        self.scheduler = scheduler
        self.insersion = BQStreamInsersion(table)

        self.path = path
        self.duration = duration

    def _ontick(self):
        import requests, json
        try:
          f = requests.get(self.path)
          data = json.loads(f.text)
          self.insersion.stream_data(self.preprocess_data(data))
        except Exception as error:
            logger.exception(error)

    def preprocess_data(self, data):
        return data

    def _callback(self):
        print("callback", self.path)
        self._ontick()
        self.scheduler.enter(self.duration, 1, self._callback)

    def run(self):
        print("enter", self.duration)
        self.scheduler.enter(self.duration, 1, self._callback)
        self._ontick()


class CCBoardApiPolling(ApiPolling):
    def __init__(self, scheduler, path, table, duration):
        super().__init__(scheduler, path, table, duration)

    def preprocess_data(self, data):
        print(data)
        asks = list({"price": float(row[0]), "size": float(row[1])} for row in data["asks"])
        bids = list({"price": float(row[0]), "size": float(row[1])} for row in data["bids"])

        best_ask = min(r["price"] for r in asks)
        best_bid = max(r["price"] for r in bids)
        mid_price = (best_ask + best_bid) / 2
        return {"ask": asks, "bid": bids, "mid_price": mid_price}


class CCTradesApiPolling(ApiPolling):
    def __init__(self, scheduler, path, table, duration):
        super().__init__(scheduler, path, table, duration)
        self.maxid = 0

    def preprocess_data(self, data):
        rows = list(
            {
                "price": row["rate"],
                "size": float(row["amount"]),
                "side": row["order_type"],
                "timestamp": datetime.datetime.strptime(row["created_at"][:-5], "%Y-%m-%dT%H:%M:%S")
            }
            for row in data["data"] if row["id"] > self.maxid
        )
        self.maxid = max(row["id"] for row in data["data"])
        return rows


scheduler = sched.scheduler(time.time, time.sleep)
bqSubscription = PubNubSubscriber(pubnub)
bqSubscription.add_subscription('lightning_board_FX_BTC_JPY', 'bf_fx_board_diff_btc_jpy')
bqSubscription.add_subscription('lightning_ticker_FX_BTC_JPY', 'bf_fx_ticker_btc_jpy')
bqSubscription.add_subscription('lightning_executions_FX_BTC_JPY', 'bf_fx_execution_btc_jpy')

bfApiPolling = ApiPolling(scheduler, "http://api.bitflyer.jp/v1/getboard", 'bf_fx_board_snapshot_btc_jpy', 15)
bfApiPolling.run()

time.sleep(1)
ccApiPolling = CCBoardApiPolling(scheduler, "https://coincheck.com/api/order_books", 'cc_board_snapshot_btc_jpy', 15)
ccApiPolling.run()

time.sleep(1)
ccTradesApiPolling = CCTradesApiPolling(scheduler, "https://coincheck.com/api/trades?pair=btc_jpy", 'cc_execution_btc_jpy', 5)
ccTradesApiPolling.run()


# メモリリーク対策のため30分ごとに自殺する
def suicide():
    SUICIDE_FLAG = True
    os._exit(0)

scheduler.enter(1800, 1, suicide)
scheduler.run()
