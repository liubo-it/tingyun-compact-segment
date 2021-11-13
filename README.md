### Segment 大小与行数统计
````
    表名                          聚合后的行数               sgement的大小 
    
APP_UX_DATA_MIN		         maxRowsPerSegment:1500000	     670MB
APP_UX_DATA_HOUR             maxRowsPerSegment:1400000       730MB
APP_DEVICE_DATA_MIN          maxRowsPerSegment:1400000       660MB
APP_DEVICE_DATA_HOUR         maxRowsPerSegment:1400000       630MB
APP_EXCEPTION_DATA_MIN       maxRowsPerSegment:1500000       xxxMB
APP_EXCEPTION_DATA_HOUR      maxRowsPerSegment:1500000       xxxMB
APP_NETWORK_DATA_MIN         maxRowsPerSegment:1500000       xxxMB
APP_NETWORK_DATA_HOUR        maxRowsPerSegment:1500000       xxxMB
````
###说明   
 -  由于`数据量`变化以及`表结构`变更需要定期维护此表格。
 - `xxx`代表当前dataSource数据量小,聚合后行数可以为较大任意值,进行聚合。

