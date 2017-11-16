#coding: utf-8
import websocket
import time
import sys


import json

[41317125,"btc_jpy","785200.0","0.005","buy"]
["btc_jpy",{"bids":[["785180.0","0"],["785149.0","0"],["784095.0","0"],["784094.0","0"],["784020.0","0"],["783813.0","0"],["783106.0","0"],["785181.0","0.17094015"],["785175.0","0.17094146"],["784966.0","0.6121"],["784819.0","0.61837588"],["784340.0","0.45"],["784140.0","0.45"],["784097.0","0.5377"],["783163.0","2.5394"]],"asks":[["785200.0","0.28660384"],["785600.0","0.45"],["788676.0","0.05"]]}]


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
        print(len(message), message)
    
    def on_error(self, ws, error):
        print(error)
        sys.exit()
    
    def on_close(self, ws):
        print("### closed ###")
    
    def on_open(self, ws):
        print("### open ###")
        ws.send(json.dumps({"type":"subscribe", "channel":"btc_jpy-trades"}))
        ws.send(json.dumps({"type":"subscribe", "channel":"btc_jpy-orderbook"}))

if __name__=="__main__":
    btc_trades = CoincheckWebSocketReader().run()

    #socket = new WebSocket("wss://ws-api.coincheck.com/")
