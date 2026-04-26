CREATE TABLE `quote_deal_ticks` (
	`id` int AUTO_INCREMENT NOT NULL,
	`ingested_at` int NOT NULL,
	`kafka_topic` varchar(191) NOT NULL,
	`market` varchar(191) NOT NULL,
	`deal_id` int,
	`time` int,
	`side` int,
	`price` varchar(191) NOT NULL,
	`amount` varchar(191) NOT NULL,
	`raw_json` text NOT NULL,
	CONSTRAINT `quote_deal_ticks_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE INDEX `idx_quote_deal_ticks_time` ON `quote_deal_ticks` (`time`);
--> statement-breakpoint
CREATE TABLE `offer_orders` (
	`id` int NOT NULL,
	`created_at` int NOT NULL,
	`kind` varchar(64) NOT NULL,
	`extern_id` int NOT NULL,
	`user_id` int NOT NULL,
	`summary` varchar(512) NOT NULL,
	`status` varchar(32) NOT NULL,
	`error` varchar(1024),
	`payload_json` text NOT NULL,
	CONSTRAINT `offer_orders_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE INDEX `idx_offer_orders_time` ON `offer_orders` (`created_at`);
--> statement-breakpoint
CREATE TABLE `quote_consumer_state` (
	`id` int AUTO_INCREMENT NOT NULL,
	`kafka_topic` varchar(191) NOT NULL,
	`partition_id` int NOT NULL DEFAULT 0,
	`last_offset` bigint NOT NULL,
	`last_deal_id` int,
	`updated_at` int NOT NULL,
	CONSTRAINT `quote_consumer_state_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE UNIQUE INDEX `uq_quote_consumer_state_topic_part` ON `quote_consumer_state` (`kafka_topic`,`partition_id`);
--> statement-breakpoint
CREATE TABLE `settle_consumer_state` (
	`id` int AUTO_INCREMENT NOT NULL,
	`settle_group_id` int NOT NULL,
	`market` varchar(191) NOT NULL,
	`last_offset` bigint NOT NULL,
	`last_msgid` int,
	`updated_at` int NOT NULL,
	CONSTRAINT `settle_consumer_state_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE UNIQUE INDEX `uq_settle_consumer_state_group_mkt` ON `settle_consumer_state` (`settle_group_id`,`market`);
--> statement-breakpoint
CREATE TABLE `settle_messages` (
	`id` int AUTO_INCREMENT NOT NULL,
	`ingested_at` int NOT NULL,
	`kafka_topic` varchar(191) NOT NULL,
	`raw_json` text NOT NULL,
	CONSTRAINT `settle_messages_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE INDEX `idx_settle_messages_ingested` ON `settle_messages` (`ingested_at`);
--> statement-breakpoint
CREATE TABLE `offer_dispatch_state` (
	`id` int NOT NULL,
	`last_sent_order_id` int NOT NULL DEFAULT 0,
	`updated_at` int NOT NULL,
	CONSTRAINT `offer_dispatch_state_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
INSERT INTO `offer_dispatch_state` (`id`, `last_sent_order_id`, `updated_at`) VALUES (1, 0, 0);
