#coding: utf-8
import websocket
import time
import sys
import datetime
from google.cloud import bigquery

import json


from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
 
pnconfig = PNConfiguration()
 
pnconfig.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
#pnconfig.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNub(pnconfig)


class BQStreamInsersion():
    import threading
    project_id = "pandora-154702"
    dataset_id = "trading"

    last_client_updated = datetime.datetime(1970,1,1)
    interval = datetime.timedelta(0, 60, 0) #60 seconds
    _lock = threading.Lock()

    def __init__(self, table_id):
        self.table_id = table_id
        BQStreamInsersion._refresh_bigquery_client_if_needed()


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
        print("refreshing bigquery client")
        cls.bigquery_client = bigquery.Client(cls.project_id)
        cls.dataset_ref = cls.bigquery_client.dataset(cls.dataset_id)


    def stream_data(self, rows):
        import pytz
        cls = BQStreamInsersion
        self._refresh_bigquery_client_if_needed()

        suffix = datetime.datetime.now(pytz.timezone('UTC')).strftime("%Y%m%d")
        table_ref = cls.dataset_ref.table("%s$%s" % (self.table_id, suffix))
        table = cls.bigquery_client.get_table(table_ref)

        print(rows)
        errors = cls.bigquery_client.create_rows(table, rows)
        print("errors", errors)



class MySubscribeCallback(SubscribeCallback):
    def __init__(self, pubnub):
        self.pubnub = pubnub
        self.channel_to_insersion = {}

    def add_subscription(self, channel, table):
        print("added subscription to %s using table %s" % (channel, table))
        self.channel_to_insersion[channel] = BQStreamInsersion(table)
        self.pubnub.subscribe().channels(channel).execute()

    def presence(self, pubnub, presence):
        pass  # handle incoming presence data
 
    def status(self, pubnub, status):
        pass
        #if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
        #    pass  # This event happens when radio / connectivity is lost
 
        #elif status.category == PNStatusCategory.PNConnectedCategory:
        #    # Connect event. You can do stuff like publish, and know you'll get it.
        #    # Or just use the connected event to confirm you are subscribed for
        #    # UI / internal notifications, etc
        #    #pubnub.publish().channel("awesomeChannel").message("hello!!").async(my_publish_callback)
        #    pass

        #elif status.category == PNStatusCategory.PNReconnectedCategory:
        #    pass
        #    # Happens as part of our regular operation. This event happens when
        #    # radio / connectivity is lost, then regained.
        #elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
        #    pass
            # Handle message decryption error. Probably client configured to
            # encrypt messages and on live data feed it received plain text.
 
    def message(self, pubnub, message):
        #print(datetime.datetime.now(), message.message[0]['exec_date']) # execution
        #print(datetime.datetime.now(), message.message['timestamp'])
        #print(message.channel, message.message)
        insersion = self.channel_to_insersion.get(message.channel)
        if insersion:
            rows = message.message
            if isinstance(rows, list):
                insersion.stream_data(rows)
            else:
                insersion.stream_data([rows])
 
 
bqSubscription = MySubscribeCallback(pubnub)
pubnub.add_listener(bqSubscription)

#pubnub.subscribe().channels('lightning_board_snapshot_FX_BTC_JPY').execute()
#bqSubscription.add_subscription('lightning_board_FX_BTC_JPY', 'bf_fx_board_diff_btc_jpy')
#bqSubscription.add_subscription('lightning_ticker_FX_BTC_JPY', 'bf_fx_ticker_btc_jpy')
bqSubscription.add_subscription('lightning_executions_FX_BTC_JPY', 'bf_fx_execution_btc_jpy')

#pubnub.subscribe().channels('lightning_executions_FX_BTC_JPY').execute()
#pubnub.subscribe().channels('lightning_board_FX_BTC_JPY').execute()
#pubnub.subscribe().channels('lightning_ticker_FX_BTC_JPY').execute()


print("added_listener")


# Instantiates a client
#bigquery_client = bigquery.Client()
#bigquery_client.tabledata()
#dataset = bigquery_client.dataset('trading')

#rows = [
#    {
#        "timesatamp": datetime.datetime.now(),
#        "mid_price": 123456.2,
#        "bids": [{"price":30.12, "size":10002.31}, {"price":30.15, "size":20002.31}],
#        "asks": []
#    },
#    {
#        "timesatamp": datetime.datetime.now(),
#        "mid_price": 123456.2,
#        "bids": [{"price":30.12, "size":10002.31}, {"price":30.15, "size":20002.31}],
#        "asks": [{"price":0.92, "size":3130.31}, {"price":40.15, "size":4905.55},{"price":1, "size":0}]
#    }
#]

#bq = BQStreamInsersion("test_board")
#bq.stream_data(rows)

print("subscribed")


