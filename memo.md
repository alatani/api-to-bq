# memo

## schemas
```
           tableId              Type    Labels
------------------------------ ------- --------
 bf_fx_board_diff_btc_jpy       TABLE
 bf_fx_board_snapshot_btc_jpy   TABLE
 bf_fx_execution_btc_jpy        TABLE
 bf_fx_ticker_btc_jpy           TABLE
 cc_board_snapshot_btc_jpy      TABLE
 cc_execution_btc_jpy           TABLE
```

### bf_fx_board_diff_btc_jpy

```
[{'mid_price': 1056543.0, 'bids': [], 'asks': [{'price': 1090140.0, 'size': 0.00100124}, {'price': 1056656.0, 'size': 0.0728}], 'timestamp': datetime.datetime(2018, 2, 15, 2, 7, 1, 157832, tzinfo=<UTC>)}]
```

Table pandora-154702:trading.bf_fx_board_diff_btc_jpy

   Last modified              Schema             Total Rows   Total Bytes   Expiration   Labels
 ----------------- ---------------------------- ------------ ------------- ------------ --------
  24 Dec 23:42:54   |- timestamp: timestamp      11972152     548758384
                    |- mid_price: float
                    +- bids: record (repeated)
                    |  |- price: float
                    |  |- size: float
                    +- asks: record (repeated)
                    |  |- price: float
                    |  |- size: float

### bf_fx_board_snapshot_btc_jpy
Table pandora-154702:trading.bf_fx_board_snapshot_btc_jpy

   Last modified                Schema               Total Rows   Total Bytes   Expiration   Labels
 ----------------- -------------------------------- ------------ ------------- ------------ --------
  25 Dec 00:07:34   |- mid_price: float (required)   89868        7178133928
                    +- bids: record (repeated)
                    |  |- price: float
                    |  |- size: float
                    +- asks: record (repeated)
                    |  |- price: float
                    |  |- size: float
                    |- timestamp: timestamp

### bf_fx_execution_btc_jpy
Table pandora-154702:trading.bf_fx_execution_btc_jpy

   Last modified                  Schema                 Total Rows   Total Bytes   Expiration   Labels
 ----------------- ------------------------------------ ------------ ------------- ------------ --------
  24 Dec 23:56:15   |- timestamp: timestamp (required)   5880907      173435419                      
                    |- price: float (required)                                                       
                    |- size: float (required)                                                        
                    |- side: string (required)                                                       

### bf_fx_ticker_btc_jpy

```
[{'product_code': 'FX_BTC_JPY', 'timestamp': '2018-02-15T02:07:57.5244865Z', 'tick_id': 12372572, 'best_bid': 1058454.0, 'best_ask': 1058750.0, 'best_bid_size': 3.94, 'best_ask_size': 1.2121, 'total_bid_depth': 7414.74236746, 'total_ask_depth': 7304.74018488, 'ltp': 1058454.0, 'volume': 204553.47145209, 'volume_by_product': 204553.47145209}]
[{'product_code': 'FX_BTC_JPY', 'timestamp': '2018-02-15T02:07:57.5244865Z', 'tick_id': 12372573, 'best_bid': 1058454.0, 'best_ask': 1058750.0, 'best_bid_size': 3.94, 'best_ask_size': 1.2121, 'total_bid_depth': 7415.24236793, 'total_ask_depth': 7304.74018488, 'ltp': 1058454.0, 'volume': 204553.47145209, 'volume_by_product': 204553.47145209}]
[{'product_code': 'FX_BTC_JPY', 'timestamp': '2018-02-15T02:07:57.5557344Z', 'tick_id': 12372575, 'best_bid': 1058454.0, 'best_ask': 1058750.0, 'best_bid_size': 3.94, 'best_ask_size': 1.2121, 'total_bid_depth': 7416.26236793, 'total_ask_depth': 7304.74018488, 'ltp': 1058454.0, 'volume': 204553.47045209, 'volume_by_product': 204553.47045209}]
```

Table pandora-154702:trading.bf_fx_ticker_btc_jpy

   Last modified                    Schema                   Total Rows   Total Bytes   Expiration   Labels
 ----------------- ---------------------------------------- ------------ ------------- ------------ --------
  24 Dec 23:53:44   |- timestamp: timestamp (required)       11882202     950576160                  
                    |- best_ask: float (required)                                                    
                    |- best_bid: float (required)                                                    
                    |- best_ask_size: float (required)                                               
                    |- best_bid_size: float (required)                                               
                    |- total_ask_depth: float (required)                                             
                    |- total_bid_depth: float (required)                                             
                    |- tick_id: integer (required)                                                   
                    |- volume_by_product: float (required)                                           
                    |- volume: float (required)                                                      

### cc_board_snapshot_btc_jpy


Table pandora-154702:trading.cc_board_snapshot_btc_jpy

   Last modified              Schema             Total Rows   Total Bytes   Expiration   Labels
 ----------------- ---------------------------- ------------ ------------- ------------ --------
  24 Dec 23:33:32   |- mid_price: float          88365        1565952
                    +- bids: record (repeated)
                    |  |- price: float
                    |  |- size: float
                    +- asks: record (repeated)
                    |  |- price: float
                    |  |- size: float
                    |- timestamp: timestamp

### cc_execution_btc_jpy

[{'side': 'BUY', 'price': 1060833.0, 'size': 0.001, 'exec_date': '2018-02-15T02:17:25.009891Z', 'timestamp': datetime.datetime(2018, 2, 15, 2, 17, 25, 9890)}]
[{'side': 'SELL', 'price': 1060688.0, 'size': 0.18, 'exec_date': '2018-02-15T02:17:25.1192641Z', 'timestamp': datetime.datetime(2018, 2, 15, 2, 17, 25, 119264)}]


Table pandora-154702:trading.cc_execution_btc_jpy

   Last modified            Schema            Total Rows   Total Bytes   Expiration   Labels
 ----------------- ------------------------- ------------ ------------- ------------ --------
  24 Dec 23:43:11   |- timestamp: timestamp   6390716      188553939
                    |- price: float
                    |- size: float
                    |- side: string
