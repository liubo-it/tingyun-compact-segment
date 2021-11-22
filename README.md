### Segment File Size
 - `300MB`-`700MB`
 
### Segment 大小与行数统计
````
    表名                             聚合后的行数               sgement的大小 
    
APP_UX_DATA_MIN		            maxRowsPerSegment:1500000	    670MB
APP_UX_DATA_HOUR                maxRowsPerSegment:1400000       730MB
APP_DEVICE_DATA_MIN             maxRowsPerSegment:7000000       644MB
APP_DEVICE_DATA_HOUR            maxRowsPerSegment:4500000       620MB
APP_EXCEPTION_DATA_MIN          maxRowsPerSegment:1500000       xxxMB
APP_EXCEPTION_DATA_HOUR         maxRowsPerSegment:1500000       xxxMB
APP_NETWORK_DATA_OVERVIEW_MIN   maxRowsPerSegment:14000000      450MB
APP_NETWORK_DATA_OVERVIEW_HOUR  maxRowsPerSegment:17000000      640MB
APP_NETWORK_DATA_MIN            maxRowsPerSegment:10000000      450MB
APP_NETWORK_DATA_HOUR           maxRowsPerSegment:9000000       400MB
````
### 说明   
 -  由于`数据量`动态变化以及`表结构`不定期更改需要定期维护此表格。
 - `xxx`代表当前dataSource数据量小,聚合后行数可以为较大任意值,进行聚合。

