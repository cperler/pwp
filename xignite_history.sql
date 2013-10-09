CREATE TABLE `pwp_pwp_xignite_stocks_history` (
  `date` varchar(11) NOT NULL DEFAULT '0' COMMENT 'date',
  `stock_id` int(11) NOT NULL DEFAULT '0' COMMENT 'the stock id',
  `last_close` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT 'the close price',
  UNIQUE KEY `date` (`date`,`stock_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='This is the historical info table for stocks';
