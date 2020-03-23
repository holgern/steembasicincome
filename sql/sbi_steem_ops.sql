-- phpMyAdmin SQL Dump
-- version 4.6.6deb5
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Mar 23, 2020 at 03:37 PM
-- Server version: 10.1.29-MariaDB-6
-- PHP Version: 7.1.20-1+ubuntu18.04.1+deb.sury.org+1

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

--
-- Database: `sbi_steem_ops`
--

-- --------------------------------------------------------

--
-- Table structure for table `curation_optimization`
--

CREATE TABLE `curation_optimization` (
  `authorperm` varchar(300) NOT NULL,
  `member` varchar(16) NOT NULL,
  `created` datetime NOT NULL,
  `best_time_delay` float NOT NULL,
  `best_curation_performance` float NOT NULL,
  `vote_rshares` bigint(20) NOT NULL,
  `updated` datetime NOT NULL,
  `vote_delay` float NOT NULL,
  `performance` float NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `member_hist`
--

CREATE TABLE `member_hist` (
  `block_num` int(11) NOT NULL,
  `block_id` varchar(40) NOT NULL,
  `trx_id` varchar(40) NOT NULL,
  `trx_num` int(11) NOT NULL,
  `op_num` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(30) NOT NULL,
  `author` varchar(16) DEFAULT NULL,
  `permlink` varchar(256) DEFAULT NULL,
  `parent_author` varchar(16) DEFAULT NULL,
  `parent_permlink` varchar(256) DEFAULT NULL,
  `voter` varchar(16) DEFAULT NULL,
  `weight` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `posts_comments`
--

CREATE TABLE `posts_comments` (
  `authorperm` varchar(300) NOT NULL,
  `author` varchar(16) NOT NULL,
  `created` datetime NOT NULL,
  `block` int(11) DEFAULT NULL,
  `voted` tinyint(1) NOT NULL DEFAULT '0',
  `rshares` bigint(20) NOT NULL DEFAULT '0',
  `main_post` tinyint(1) NOT NULL DEFAULT '0',
  `skip` tinyint(1) NOT NULL DEFAULT '0',
  `comment_to_old` tinyint(1) NOT NULL DEFAULT '0',
  `vote_delay` float NOT NULL DEFAULT '900',
  `voted_after` float DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi2_ops`
--

CREATE TABLE `sbi2_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi3_ops`
--

CREATE TABLE `sbi3_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi4_ops`
--

CREATE TABLE `sbi4_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi5_ops`
--

CREATE TABLE `sbi5_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi6_ops`
--

CREATE TABLE `sbi6_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi7_ops`
--

CREATE TABLE `sbi7_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi8_ops`
--

CREATE TABLE `sbi8_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi9_ops`
--

CREATE TABLE `sbi9_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi10_ops`
--

CREATE TABLE `sbi10_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `sbi_ops`
--

CREATE TABLE `sbi_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` bigint(19) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` bigint(19) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `steembasicincome_ops`
--

CREATE TABLE `steembasicincome_ops` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `type` varchar(50) NOT NULL,
  `op_dict` text NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `transfers`
--

CREATE TABLE `transfers` (
  `virtual_op` int(11) NOT NULL,
  `op_acc_index` int(11) NOT NULL,
  `op_acc_name` varchar(50) NOT NULL,
  `block` int(11) NOT NULL,
  `trx_in_block` int(11) NOT NULL,
  `op_in_trx` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `from` varchar(50) NOT NULL,
  `to` varchar(50) NOT NULL,
  `amount` decimal(15,6) DEFAULT NULL,
  `amount_symbol` varchar(5) DEFAULT NULL,
  `memo` varchar(2048) DEFAULT NULL,
  `op_type` varchar(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `curation_optimization`
--
ALTER TABLE `curation_optimization`
  ADD PRIMARY KEY (`member`,`created`),
  ADD KEY `ix_curation_optimization_1f3f6589887b9c0e` (`member`,`created`);

--
-- Indexes for table `member_hist`
--
ALTER TABLE `member_hist`
  ADD PRIMARY KEY (`block_num`,`trx_id`,`op_num`),
  ADD KEY `author` (`author`),
  ADD KEY `voter` (`voter`),
  ADD KEY `type` (`type`),
  ADD KEY `block_num` (`block_num`);

--
-- Indexes for table `posts_comments`
--
ALTER TABLE `posts_comments`
  ADD PRIMARY KEY (`author`,`created`),
  ADD KEY `created` (`created`),
  ADD KEY `author` (`author`),
  ADD KEY `ix_posts_comments_83abfc77eaacd310` (`author`,`created`);

--
-- Indexes for table `sbi2_ops`
--
ALTER TABLE `sbi2_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`),
  ADD KEY `op_acc_index_2` (`op_acc_index`,`type`);

--
-- Indexes for table `sbi3_ops`
--
ALTER TABLE `sbi3_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi4_ops`
--
ALTER TABLE `sbi4_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi5_ops`
--
ALTER TABLE `sbi5_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi6_ops`
--
ALTER TABLE `sbi6_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi7_ops`
--
ALTER TABLE `sbi7_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi8_ops`
--
ALTER TABLE `sbi8_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi9_ops`
--
ALTER TABLE `sbi9_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi10_ops`
--
ALTER TABLE `sbi10_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `block` (`block`),
  ADD KEY `op_acc_index` (`op_acc_index`);

--
-- Indexes for table `sbi_ops`
--
ALTER TABLE `sbi_ops`
  ADD PRIMARY KEY (`virtual_op`,`block`,`trx_in_block`,`op_in_trx`),
  ADD KEY `op_acc_index` (`op_acc_index`),
  ADD KEY `block` (`block`);

--
-- Indexes for table `steembasicincome_ops`
--
ALTER TABLE `steembasicincome_ops`
  ADD PRIMARY KEY (`op_acc_index`);

--
-- Indexes for table `transfers`
--
ALTER TABLE `transfers`
  ADD PRIMARY KEY (`op_acc_index`,`op_acc_name`);
