CREATE TABLE `data_lifecycle_table` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ip` varchar(20) NOT NULL DEFAULT '0' COMMENT '执行清理操作的机器ip',
  `last_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '上次删除的记录对应的id',
  `last_update_time` timestamp COMMENT '上次删除的记录对应的更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;