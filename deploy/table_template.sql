-- follow is state of the state machine, so called snapshot.
CREATE TABLE `snap_order_example` (
	`id`                BIGINT UNSIGNED NOT NULL PRIMARY KEY,
	`t`                 TINYINT UNSIGNED NOT NULL,
	`side`              TINYINT UNSIGNED NOT NULL,
	`create_time`       BIGINT NOT NULL,
	`update_time`       BIGINT NOT NULL,
	`user_id`           INT UNSIGNED NOT NULL,
	`price`             TEXT NOT NULL,
	`amount`            TEXT NOT NULL,
	`taker_fee_rate`    TEXT NOT NULL,
	`maker_fee_rate`    TEXT NOT NULL,
	`left`              TEXT NOT NULL,
	`deal_stock`        TEXT NOT NULL,
	`deal_money`        TEXT NOT NULL,
	`deal_fee`          TEXT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `snap` (
	`id`                INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
	`time`              BIGINT NOT NULL,
	`oper_id`       	BIGINT UNSIGNED NOT NULL,
	`order_id`      	BIGINT UNSIGNED NOT NULL,
	`deals_id`      	BIGINT UNSIGNED NOT NULL,
	`message_id`    	BIGINT UNSIGNED NOT NULL DEFAULT '0',
	`input_offset`      BIGINT NOT NULL DEFAULT '-1'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

