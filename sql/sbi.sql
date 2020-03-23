-- phpMyAdmin SQL Dump
-- version 4.6.6deb5
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Mar 23, 2020 at 03:36 PM
-- Server version: 10.1.29-MariaDB-6
-- PHP Version: 7.1.20-1+ubuntu18.04.1+deb.sury.org+1

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `sbi`
--

-- --------------------------------------------------------

--
-- Table structure for table `accounts`
--

CREATE TABLE `accounts` (
  `name` varchar(16) NOT NULL,
  `voting` tinyint(1) NOT NULL,
  `transfer` tinyint(1) NOT NULL DEFAULT '0',
  `upvote_reward_rshares` tinyint(1) NOT NULL DEFAULT '0',
  `transfer_memo_sender` tinyint(1) NOT NULL DEFAULT '0',
  `last_paid_post` datetime DEFAULT NULL,
  `last_paid_comment` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `blacklist`
--

CREATE TABLE `blacklist` (
  `id` int(11) NOT NULL,
  `tags` text NOT NULL,
  `apps` text NOT NULL,
  `body` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `configuration`
--

CREATE TABLE `configuration` (
  `id` enum('1') NOT NULL,
  `share_cycle_min` float NOT NULL,
  `sp_share_ratio` float NOT NULL,
  `rshares_per_cycle` bigint(20) DEFAULT '800000000',
  `del_rshares_per_cycle` bigint(20) NOT NULL DEFAULT '80000000',
  `comment_vote_divider` float DEFAULT NULL,
  `comment_vote_timeout_h` float DEFAULT NULL,
  `last_cycle` datetime DEFAULT NULL,
  `upvote_multiplier` float NOT NULL DEFAULT '1.05',
  `upvote_multiplier_adjusted` float NOT NULL DEFAULT '1',
  `last_paid_post` datetime DEFAULT NULL,
  `last_paid_comment` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `minimum_vote_threshold` bigint(20) NOT NULL DEFAULT '800000000',
  `last_delegation_check` datetime DEFAULT NULL,
  `comment_footer` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `member`
--

CREATE TABLE `member` (
  `account` varchar(50) NOT NULL,
  `note` text,
  `shares` int(11) NOT NULL,
  `bonus_shares` int(11) NOT NULL,
  `total_share_days` int(11) DEFAULT NULL,
  `avg_share_age` float DEFAULT NULL,
  `last_comment` datetime DEFAULT NULL,
  `last_post` datetime DEFAULT NULL,
  `original_enrollment` datetime DEFAULT NULL,
  `latest_enrollment` datetime DEFAULT NULL,
  `flags` text,
  `earned_rshares` bigint(22) DEFAULT NULL,
  `subscribed_rshares` bigint(20) NOT NULL DEFAULT '0',
  `curation_rshares` bigint(20) NOT NULL DEFAULT '0',
  `delegation_rshares` bigint(20) NOT NULL DEFAULT '0',
  `other_rshares` bigint(20) NOT NULL DEFAULT '0',
  `rewarded_rshares` bigint(22) DEFAULT NULL,
  `balance_rshares` bigint(22) DEFAULT NULL,
  `upvote_delay` float DEFAULT NULL,
  `comment_upvote` tinyint(1) DEFAULT NULL,
  `updated_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `first_cycle_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `steemcleaners` tinyint(1) DEFAULT NULL,
  `buildawhale` tinyint(1) DEFAULT NULL,
  `blacklisted` tinyint(1) DEFAULT NULL,
  `last_received_vote` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `member_backup`
--

CREATE TABLE `member_backup` (
  `account` varchar(50) NOT NULL,
  `note` text,
  `shares` int(11) NOT NULL,
  `bonus_shares` int(11) NOT NULL,
  `total_share_days` int(11) DEFAULT NULL,
  `avg_share_age` float DEFAULT NULL,
  `last_comment` datetime DEFAULT NULL,
  `last_post` datetime DEFAULT NULL,
  `original_enrollment` datetime DEFAULT NULL,
  `latest_enrollment` datetime DEFAULT NULL,
  `flags` text,
  `earned_rshares` bigint(22) DEFAULT NULL,
  `subscribed_rshares` bigint(20) NOT NULL DEFAULT '0',
  `curation_rshares` bigint(20) NOT NULL DEFAULT '0',
  `delegation_rshares` bigint(20) NOT NULL DEFAULT '0',
  `other_rshares` bigint(20) NOT NULL DEFAULT '0',
  `rewarded_rshares` bigint(22) DEFAULT NULL,
  `balance_rshares` bigint(22) DEFAULT NULL,
  `upvote_delay` float DEFAULT NULL,
  `comment_upvote` tinyint(1) DEFAULT NULL,
  `post_hist_28` int(11) DEFAULT NULL,
  `post_hist_7` int(11) DEFAULT NULL,
  `norm_post_hist_7` float DEFAULT NULL,
  `upvote_count_28` int(11) DEFAULT NULL,
  `upvote_weight_28` float DEFAULT NULL,
  `updated_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `first_cycle_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `steemcleaners` tinyint(1) DEFAULT NULL,
  `buildawhale` tinyint(1) DEFAULT NULL,
  `blacklisted` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `member_hist`
--

CREATE TABLE `member_hist` (
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `pending_refunds`
--

CREATE TABLE `pending_refunds` (
  `index` int(11) DEFAULT NULL,
  `sender` varchar(16) NOT NULL,
  `to` varchar(16) DEFAULT NULL,
  `memo` text,
  `encrypted` tinyint(1) DEFAULT '0',
  `referenced_accounts` text,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `steem_keys`
--

CREATE TABLE `steem_keys` (
  `SBI_account_PK` tinyint(4) NOT NULL,
  `account` varchar(50) NOT NULL,
  `key_type` varchar(64) NOT NULL,
  `wif` varchar(64) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `transaction_memo`
--

CREATE TABLE `transaction_memo` (
  `index` int(11) DEFAULT NULL,
  `sender` varchar(16) NOT NULL,
  `to` varchar(16) DEFAULT NULL,
  `memo` text,
  `encrypted` tinyint(1) DEFAULT '0',
  `referenced_accounts` text,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `transaction_out`
--

CREATE TABLE `transaction_out` (
  `index` int(11) DEFAULT NULL,
  `sender` varchar(16) NOT NULL,
  `to` varchar(16) DEFAULT NULL,
  `memo` text,
  `encrypted` tinyint(1) DEFAULT '0',
  `referenced_accounts` text,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `transfer_memos`
--

CREATE TABLE `transfer_memos` (
  `id` int(11) NOT NULL,
  `memo_type` varchar(50) NOT NULL,
  `enabled` tinyint(1) NOT NULL DEFAULT '0',
  `memo` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `trx`
--

CREATE TABLE `trx` (
  `index` int(11) NOT NULL,
  `source` varchar(50) NOT NULL,
  `memo` text,
  `account` varchar(50) DEFAULT NULL,
  `sponsor` varchar(50) DEFAULT NULL,
  `sponsee` text,
  `shares` int(11) DEFAULT NULL,
  `vests` decimal(15,6) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `status` varchar(50) NOT NULL,
  `share_type` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `trx_backup`
--

CREATE TABLE `trx_backup` (
  `index` int(11) NOT NULL,
  `source` varchar(50) NOT NULL,
  `memo` text,
  `account` varchar(50) DEFAULT NULL,
  `sponsor` varchar(50) DEFAULT NULL,
  `sponsee` text,
  `shares` int(11) DEFAULT NULL,
  `vests` decimal(15,6) DEFAULT NULL,
  `timestamp` datetime NOT NULL,
  `status` varchar(50) NOT NULL,
  `share_type` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `accounts`
--
ALTER TABLE `accounts`
  ADD PRIMARY KEY (`name`);

--
-- Indexes for table `blacklist`
--
ALTER TABLE `blacklist`
  ADD UNIQUE KEY `id` (`id`);

--
-- Indexes for table `configuration`
--
ALTER TABLE `configuration`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `member`
--
ALTER TABLE `member`
  ADD PRIMARY KEY (`account`),
  ADD KEY `ix_member_ca20728d35f00c3a` (`account`);

--
-- Indexes for table `member_backup`
--
ALTER TABLE `member_backup`
  ADD PRIMARY KEY (`account`),
  ADD KEY `ix_member_ca20728d35f00c3a` (`account`);

--
-- Indexes for table `member_hist`
--
ALTER TABLE `member_hist`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `pending_refunds`
--
ALTER TABLE `pending_refunds`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `steem_keys`
--
ALTER TABLE `steem_keys`
  ADD PRIMARY KEY (`SBI_account_PK`);

--
-- Indexes for table `transaction_memo`
--
ALTER TABLE `transaction_memo`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `transaction_out`
--
ALTER TABLE `transaction_out`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `transfer_memos`
--
ALTER TABLE `transfer_memos`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `trx`
--
ALTER TABLE `trx`
  ADD PRIMARY KEY (`index`,`source`);

--
-- Indexes for table `trx_backup`
--
ALTER TABLE `trx_backup`
  ADD PRIMARY KEY (`index`,`source`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `member_hist`
--
ALTER TABLE `member_hist`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `pending_refunds`
--
ALTER TABLE `pending_refunds`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
--
-- AUTO_INCREMENT for table `transaction_memo`
--
ALTER TABLE `transaction_memo`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=343136664;
--
-- AUTO_INCREMENT for table `transaction_out`
--
ALTER TABLE `transaction_out`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=6387232;
--
-- AUTO_INCREMENT for table `transfer_memos`
--
ALTER TABLE `transfer_memos`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=6;
