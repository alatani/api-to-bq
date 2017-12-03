#coding: utf-8
import pytz
import websocket
import time
import sys
import datetime
import json


from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
 
pnconfig = PNConfiguration()
 
pnconfig.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
#pnconfig.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNub(pnconfig)


class PubNubSubscriber(SubscribeCallback):
    def __init__(self, pubnub):
        self.pubnub = pubnub
        self.channel_to_insersion = {}
        pubnub.add_listener(self)

    def add_subscription(self, channel):
        self.pubnub.subscribe().channels(channel).execute()

    def presence(self, pubnub, presence):
        pass  # handle incoming presence data
 
    def status(self, pubnub, status):
        pass
 
    def message(self, pubnub, message):
        print(message.message)
 

bqSubscription = PubNubSubscriber(pubnub)
#bqSubscription.add_subscription('lightning_board_FX_BTC_JPY')
bqSubscription.add_subscription('lightning_ticker_ETH_BTC')
#bqSubscription.add_subscription('lightning_executions_FX_BTC_JPY')
#bqSubscription.add_subscription('lightning_board_snapshot_FX_BTC_JPY')
