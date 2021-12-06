DROP TABLE  IF EXISTS `DRUID_COMPACT_SEGMENTS_INFO`;
CREATE TABLE IF NOT EXISTS  `DRUID_COMPACT_SEGMENTS_INFO` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `timestamp` datetime  NOT NULL COMMENT '创建task时间',
  `dataSource` varchar(255) NOT NULL COMMENT '表名',
  `taskId`     varchar(255)  NOT NULL COMMENT '生成的taskID',
  `startTime`  varchar(255)   NOT NULL COMMENT 'interval开始时间',
  `endTime`    varchar(255)   NOT NULL COMMENT 'interval结束时间',
  `checkTime`  datetime  NOT NULL COMMENT '校验时间',
  PRIMARY KEY (`id`),
  KEY `idx_druid_compact_segments_info_0` (`dataSource`,`timestamp`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='Druid压缩Segment Id表';
