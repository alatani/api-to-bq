#coding: utf-8
import websocket
import time
import sys
import datetime

import json

import sched, time

class PeriodicDataCollectionScheduler():
    def __init__(self, interval):
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self.priority = 10
        self.interval = interval

    def run(self):
        self.scheduler.enterabs(self.interval, self.priority, self.ontick())

    def ontick(self):
        self.fetch()
        self.scheduler.enterabs(self.interval, self.priority, self.ontick())

    def fetch(self): 
        print("fetched")



class CoincheckWebSocketReader():
    endpoint = "wss://ws-api.coincheck.com/"
    def __init__(self):
        #websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
                CoincheckWebSocketReader.endpoint,
                on_message = self.on_message, 
                on_error = self.on_error, 
                on_close = self.on_close
         )
        self.ws.on_open = self.on_open

    def run(self):
        self.ws.run_forever()
        print("### run ###")
        pass

    def on_message(self, ws, message):
        #print("### message ###")
        now = datetime.datetime.now()
        print(str(now), message)
    
    def on_error(self, ws, error):
        print(error)
        sys.exit()
    
    def on_close(self, ws):
        print("### closed ###")
    
    def on_open(self, ws):
        print("### open ###")
        ws.send(json.dumps({"type":"subscribe", "channel":"btc_jpy-trades"}))
        #ws.send(json.dumps({"type":"subscribe", "channel":"btc_jpy-orderbook"}))

if __name__=="__main__":
    s = PeriodicDataCollectionScheduler(50000000)
    s.run()

    #btc_trades = CoincheckWebSocketReader().run()

    #socket = new WebSocket("wss://ws-api.coincheck.com/")
