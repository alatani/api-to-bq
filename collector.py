#coding: utf-8
import pytz
import websocket
import time
import sys, os
import datetime
from google.cloud import bigquery, logging


import tracemalloc
tracemalloc.start()
# ... start your application ...

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
 
pnconfig = PNConfiguration()
 
pnconfig.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
#pnconfig.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNub(pnconfig)

PROJECT_ID="pandora-154702"
logging_client = logging.Client(PROJECT_ID)
import logging
logger = logging.getLogger('bitflyer-collector')
logger.addHandler(logging_client.get_default_handler())
logger.setLevel(logging.INFO)


SUICIDE_FLAG=False
DRY_RUN=False

class StreamDataProcessing():
    def __init__(self):
        pass

    def stream_data(self, rows):
        pass

class BQStreamInsersion(StreamDataProcessing):
    import threading
    project_id = PROJECT_ID
    dataset_id = "trading"

    last_client_updated = datetime.datetime(1970,1,1)
    interval = datetime.timedelta(0, 30, 0) #30秒ごとに再接続
    _lock = threading.Lock()

    def __init__(self, table_id):
        self.table_id = table_id
        BQStreamInsersion._refresh_bigquery_client_if_needed()
        self.counter = 0

        self.last_logged =  datetime.datetime.now(pytz.timezone('UTC'))
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

        if "_execution_" in self.table_id:
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

            records =  (self.preprocess_row(r) for r in rows)
            if DRY_RUN:
                print(list(records))
            else:
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
            #pubnub.publish().channel("awesomeChannel").message("hello!!").async(my_publish_callback)
            pass

        elif status.category == PNStatusCategory.PNReconnectedCategory:
            logger.info("PNStatusCategory.PNReconnectedCategory")
            # Happens as part of our regular operation. This event happens when
            # radio / connectivity is lost, then regained.
        #elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
        #    pass
            # Handle message decryption error. Probably client configured to
            # encrypt messages and on live data feed it received plain text.
 
    def message(self, pubnub, message):
        if SUICIDE_FLAG:
            os._exit(0)
        insersion = self.channel_to_insersion.get(message.channel)
        insersion.stream_data(message.message)
 

class ApiPolling():
    def __init__(self, path, table, duration):
        import sched
        import time
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.insersion = BQStreamInsersion(table)

        self.path = path
        self.duration = duration

    def _ontick(self):
        import requests, json
        f = requests.get("http://api.bitflyer.jp/v1/getboard")
        self.insersion.stream_data(json.loads(f.text))

    def _callback(self, duration):
        self._ontick()
        self.scheduler.enter(duration, 1, self._callback, (duration,))

    def run(self):
        self._ontick()
        self.scheduler.enter(self.duration, 1, self._callback, (self.duration,))
        self.scheduler.run(False)
 
bqSubscription = PubNubSubscriber(pubnub)
bqSubscription.add_subscription('lightning_board_FX_BTC_JPY', 'bf_fx_board_diff_btc_jpy')
bqSubscription.add_subscription('lightning_ticker_FX_BTC_JPY', 'bf_fx_ticker_btc_jpy')
bqSubscription.add_subscription('lightning_executions_FX_BTC_JPY', 'bf_fx_execution_btc_jpy')

apiPolling = ApiPolling("http://api.bitflyer.jp/v1/getboard", 'bf_fx_board_snapshot_btc_jpy', 15)
apiPolling.run()


time.sleep(1800)
SUICIDE_FLAG=True
os._exit(0)
