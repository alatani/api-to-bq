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
Table pandora-154702:trading.cc_execution_btc_jpy

   Last modified            Schema            Total Rows   Total Bytes   Expiration   Labels
 ----------------- ------------------------- ------------ ------------- ------------ --------
  24 Dec 23:43:11   |- timestamp: timestamp   6390716      188553939
                    |- price: float
                    |- size: float
                    |- side: string
