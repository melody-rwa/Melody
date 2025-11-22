create database melo_db default character set utf8mb4 collate utf8mb4_unicode_ci;

USE melo_db;

SET NAMES utf8mb4;


/*
 Navicat Premium Dump SQL

 Source Server         : qqq
 Source Server Type    : MySQL
 Source Server Version : 80040 (8.0.40)
 Source Host           : localhost:3306
 Source Schema         : melo_db

 Target Server Type    : MySQL
 Target Server Version : 80040 (8.0.40)
 File Encoding         : 65001

 Date: 21/11/2025 13:38:50
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for aux_bt
-- ----------------------------
DROP TABLE IF EXISTS `aux_bt`;
CREATE TABLE `aux_bt`  (
  `d` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `t` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `f` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `s` varchar(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `w` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `rt` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`d`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_artist_redeem
-- ----------------------------
DROP TABLE IF EXISTS `t_artist_redeem`;
CREATE TABLE `t_artist_redeem`  (
  `block_num` bigint NOT NULL,
  `song_id` int NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `meme_token` decimal(18, 8) NULL DEFAULT 0.00000000,
  `bnb_token` decimal(18, 8) NULL DEFAULT NULL,
  `lpUnits` decimal(18, 8) NULL DEFAULT NULL,
  PRIMARY KEY (`block_num`, `song_id`, `user_address`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_bnb
-- ----------------------------
DROP TABLE IF EXISTS `t_bnb`;
CREATE TABLE `t_bnb`  (
  `bnb_amount` decimal(10, 1) NOT NULL,
  PRIMARY KEY (`bnb_amount`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_claim
-- ----------------------------
DROP TABLE IF EXISTS `t_claim`;
CREATE TABLE `t_claim`  (
  `block_num` bigint NOT NULL,
  `song_id` int NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `bnb_token` decimal(18, 8) NULL DEFAULT 0.00000000,
  `meme_token` decimal(18, 8) NULL DEFAULT 0.00000000,
  PRIMARY KEY (`block_num`, `song_id`, `user_address`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_claim_artist
-- ----------------------------
DROP TABLE IF EXISTS `t_claim_artist`;
CREATE TABLE `t_claim_artist`  (
  `block_num` bigint NOT NULL,
  `song_id` int NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `meme_token` decimal(18, 8) NULL DEFAULT 0.00000000,
  `bnb_token` decimal(18, 8) NULL DEFAULT NULL,
  PRIMARY KEY (`block_num`, `song_id`, `user_address`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_email
-- ----------------------------
DROP TABLE IF EXISTS `t_email`;
CREATE TABLE `t_email`  (
  `user_code` char(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `user_email` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`user_code`) USING BTREE,
  INDEX `user_email`(`user_email` ASC) USING BTREE,
  INDEX `user_address`(`user_address` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for t_music
-- ----------------------------
DROP TABLE IF EXISTS `t_music`;
CREATE TABLE `t_music`  (
  `music_id` int UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'unique',
  `song_id` int NULL DEFAULT 0 COMMENT 'Artist wallet address',
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'Artist wallet address',
  `music_url` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'music URL',
  `music_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'Music name',
  `total_raise` decimal(10, 1) NULL DEFAULT NULL,
  `music_seconds` int NULL DEFAULT NULL COMMENT 'Music duration in seconds',
  `token_id` int NULL DEFAULT 0 COMMENT 'Each artist\'s token_id starts from 1',
  `token_logo` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'logo URL',
  `token_name` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'token name',
  `token_symbol` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'token symbol',
  `token_desc` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL COMMENT 'description',
  `website` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `twitter` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `telegram` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `start_time` int NULL DEFAULT 0 COMMENT 'on-chain time',
  `series_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'Artist Music Collection Agent Address',
  `memetoken_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL COMMENT 'memeToken address',
  `is_end` tinyint NULL DEFAULT 0 COMMENT 'Has it ended',
  `confirm_time` int NULL DEFAULT 0 COMMENT 'Confirm on chain that the timestamp already includes planned_dec',
  `song_pre_id` int NULL DEFAULT 0 COMMENT 'pre-release song_id',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`music_id`) USING BTREE,
  INDEX `start_time`(`start_time` ASC) USING BTREE,
  INDEX `song_id`(`song_id` ASC, `is_end` ASC) USING BTREE,
  INDEX `is_end`(`is_end` ASC) USING BTREE,
  INDEX `song_pre_id`(`song_pre_id` ASC) USING BTREE,
  INDEX `user_address`(`user_address` ASC, `start_time` ASC) USING BTREE,
  INDEX `confirm_time`(`confirm_time` DESC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 30 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_music_sub
-- ----------------------------
DROP TABLE IF EXISTS `t_music_sub`;
CREATE TABLE `t_music_sub`  (
  `sub_id` int UNSIGNED NOT NULL,
  `song_id` int NOT NULL,
  `block_num` bigint NOT NULL,
  `token_id` int NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `start_second` int NULL DEFAULT 0,
  `sub_seconds` int NOT NULL DEFAULT 0,
  `sub_type` tinyint NULL DEFAULT 0,
  `sub_amount` decimal(18, 8) NULL DEFAULT 0.00000000,
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `series_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `meme_token` int NULL DEFAULT 0,
  `tx_hash` char(66) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `hash_time` int NULL DEFAULT NULL,
  PRIMARY KEY (`block_num`, `sub_id`, `song_id`) USING BTREE,
  INDEX `song_id`(`user_address` ASC, `song_id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_nftcon
-- ----------------------------
DROP TABLE IF EXISTS `t_nftcon`;
CREATE TABLE `t_nftcon`  (
  `id` int UNSIGNED NOT NULL AUTO_INCREMENT,
  `block_num` bigint NULL DEFAULT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_id` int NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `block_num`(`block_num` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_nftearly
-- ----------------------------
DROP TABLE IF EXISTS `t_nftearly`;
CREATE TABLE `t_nftearly`  (
  `id` int UNSIGNED NOT NULL AUTO_INCREMENT,
  `block_num` bigint NULL DEFAULT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_id` int NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `block_num`(`block_num` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1461 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_nftog
-- ----------------------------
DROP TABLE IF EXISTS `t_nftog`;
CREATE TABLE `t_nftog`  (
  `id` int UNSIGNED NOT NULL AUTO_INCREMENT,
  `block_num` bigint NULL DEFAULT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_id` int NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `block_num`(`block_num` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_play_all
-- ----------------------------
DROP TABLE IF EXISTS `t_play_all`;
CREATE TABLE `t_play_all`  (
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `user_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `user_avatar` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `music_id` int UNSIGNED NOT NULL,
  `song_id` int NULL DEFAULT NULL,
  `music_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `music_url` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `music_seconds` int NULL DEFAULT NULL,
  `token_id` int NULL DEFAULT NULL,
  `token_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_symbol` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_logo` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `start_time` int NULL DEFAULT NULL,
  `create_time` datetime NULL DEFAULT NULL,
  `artist_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `artist_avatar` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`music_id`) USING BTREE,
  INDEX `play_time`(`create_time` DESC) USING BTREE,
  INDEX `idx_user_address`(`user_address` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_play_user
-- ----------------------------
DROP TABLE IF EXISTS `t_play_user`;
CREATE TABLE `t_play_user`  (
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `user_avatar` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `music_id` int UNSIGNED NOT NULL,
  `song_id` int NULL DEFAULT NULL,
  `music_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `music_url` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `music_seconds` int NULL DEFAULT NULL,
  `token_id` int NULL DEFAULT NULL,
  `token_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_symbol` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_logo` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `start_time` int NULL DEFAULT NULL,
  `create_time` datetime NULL DEFAULT NULL,
  `artist_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `artist_avatar` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`user_address`, `music_id`) USING BTREE,
  INDEX `play_time`(`create_time` DESC) USING BTREE,
  INDEX `idx_music_id`(`music_id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_slat
-- ----------------------------
DROP TABLE IF EXISTS `t_slat`;
CREATE TABLE `t_slat`  (
  `id` int UNSIGNED NOT NULL AUTO_INCREMENT,
  `slat` char(66) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `used` tinyint NULL DEFAULT 0 COMMENT '0 no use,1 pre use,2 use',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `slat`(`slat` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 151 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_user
-- ----------------------------
DROP TABLE IF EXISTS `t_user`;
CREATE TABLE `t_user`  (
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `user_avatar` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `user_desc` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `user_type` tinyint NULL DEFAULT 0,
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `user_link` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `twitter` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `facebook` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `tg` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `instgram` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `artist_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `artist_avatar` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `artist_desc` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  `artist_link` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`user_address`) USING BTREE,
  INDEX `create_time`(`create_time` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_user_redeem
-- ----------------------------
DROP TABLE IF EXISTS `t_user_redeem`;
CREATE TABLE `t_user_redeem`  (
  `block_num` bigint NOT NULL,
  `song_id` int NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `meme_token` decimal(18, 8) NULL DEFAULT 0.00000000,
  `bnb_token` decimal(18, 8) NULL DEFAULT NULL,
  `liqRemoved` decimal(18, 8) NULL DEFAULT NULL,
  PRIMARY KEY (`block_num`, `song_id`) USING BTREE,
  INDEX `song_id`(`song_id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_user_related
-- ----------------------------
DROP TABLE IF EXISTS `t_user_related`;
CREATE TABLE `t_user_related`  (
  `id` int UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `related_account` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for t_whitelist
-- ----------------------------
DROP TABLE IF EXISTS `t_whitelist`;
CREATE TABLE `t_whitelist`  (
  `token_id` int NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `block_num` bigint NOT NULL,
  `series_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `song_id` int NOT NULL,
  PRIMARY KEY (`block_num`, `song_id`, `user_address`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for w_artist
-- ----------------------------
DROP TABLE IF EXISTS `w_artist`;
CREATE TABLE `w_artist`  (
  `block_num` bigint NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`block_num`, `user_address`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for w_finalized
-- ----------------------------
DROP TABLE IF EXISTS `w_finalized`;
CREATE TABLE `w_finalized`  (
  `block_num` bigint NOT NULL,
  `token_id` int NOT NULL,
  `series_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `song_id` int NOT NULL,
  `liquidity` decimal(18, 8) NULL DEFAULT NULL,
  `lp_token_id` int NULL DEFAULT NULL,
  PRIMARY KEY (`block_num`, `song_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for w_musc_pre
-- ----------------------------
DROP TABLE IF EXISTS `w_musc_pre`;
CREATE TABLE `w_musc_pre`  (
  `block_num` bigint NULL DEFAULT NULL,
  `music_id` int UNSIGNED NOT NULL,
  `user_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `song_pre_id` int NULL DEFAULT NULL,
  `planned_sec` int NULL DEFAULT NULL,
  `confirm_time` int NULL DEFAULT NULL,
  PRIMARY KEY (`music_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Table structure for w_music
-- ----------------------------
DROP TABLE IF EXISTS `w_music`;
CREATE TABLE `w_music`  (
  `block_num` bigint NOT NULL,
  `music_id` int UNSIGNED NOT NULL,
  `series_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `memetoken_address` char(42) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `token_id` int NULL DEFAULT NULL,
  `start_time` int NULL DEFAULT NULL,
  `song_id` int NULL DEFAULT NULL,
  PRIMARY KEY (`block_num`, `music_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- View structure for v_music
-- ----------------------------
DROP VIEW IF EXISTS `v_music`;
CREATE ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `v_music` AS select `t_music`.`music_id` AS `music_id`,`t_music`.`user_address` AS `user_address`,`t_music`.`music_url` AS `music_url`,`t_music`.`music_name` AS `music_name`,`t_music`.`total_raise` AS `total_raise`,`t_music`.`music_seconds` AS `music_seconds`,`t_music`.`token_id` AS `token_id`,`t_music`.`token_logo` AS `token_logo`,`t_music`.`token_name` AS `token_name`,`t_music`.`token_symbol` AS `token_symbol`,`t_music`.`token_desc` AS `token_desc`,`t_music`.`website` AS `website`,`t_music`.`twitter` AS `twitter`,`t_music`.`confirm_time` AS `confirm_time`,`t_music`.`song_id` AS `song_id`,`t_user`.`user_avatar` AS `user_avatar`,`t_user`.`user_name` AS `user_name`,`t_user`.`artist_avatar` AS `artist_avatar`,`t_user`.`artist_desc` AS `artist_desc`,`t_user`.`artist_name` AS `artist_name`,ifnull(`a`.`sub_amount`,0) AS `total_sub_amount`,`t_music`.`telegram` AS `telegram`,`t_music`.`series_address` AS `series_address`,`t_music`.`memetoken_address` AS `memetoken_address`,`t_music`.`is_end` AS `is_end`,`t_user`.`artist_link` AS `artist_link`,`t_music`.`start_time` AS `start_time`,unix_timestamp(`t_music`.`create_time`) AS `create_time`,unix_timestamp() AS `now_time` from ((`t_music` join `t_user` on((`t_music`.`user_address` = `t_user`.`user_address`))) left join (select `t_music_sub`.`song_id` AS `song_id`,sum(`t_music_sub`.`sub_amount`) AS `sub_amount` from `t_music_sub` group by `t_music_sub`.`song_id`) `a` on((`t_music`.`song_id` = `a`.`song_id`))) where ((`t_music`.`start_time` > 0) or (`t_music`.`confirm_time` > 0));

-- ----------------------------
-- View structure for v_music_sub
-- ----------------------------
DROP VIEW IF EXISTS `v_music_sub`;
CREATE ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `v_music_sub` AS select `a`.`sub_id` AS `sub_id`,`a`.`song_id` AS `song_id`,`a`.`token_id` AS `token_id`,`a`.`user_address` AS `user_address`,`a`.`sub_amount` AS `sub_amount`,unix_timestamp(`a`.`create_time`) AS `create_time`,`b`.`music_id` AS `music_id`,`b`.`music_name` AS `music_name`,`b`.`music_url` AS `music_url`,`b`.`music_seconds` AS `music_seconds`,`b`.`token_name` AS `token_name`,`b`.`token_symbol` AS `token_symbol`,`b`.`token_logo` AS `token_logo`,`b`.`start_time` AS `start_time`,`b`.`user_name` AS `user_name`,`b`.`user_avatar` AS `user_avatar`,unix_timestamp() AS `now_time` from (`t_music_sub` `a` join (select `b1`.`song_id` AS `song_id`,`b1`.`music_id` AS `music_id`,`b1`.`music_name` AS `music_name`,`b1`.`music_url` AS `music_url`,`b1`.`music_seconds` AS `music_seconds`,`b1`.`token_name` AS `token_name`,`b1`.`token_symbol` AS `token_symbol`,`b1`.`token_logo` AS `token_logo`,`b1`.`start_time` AS `start_time`,`b2`.`user_name` AS `user_name`,`b2`.`user_avatar` AS `user_avatar` from (`t_music` `b1` join `t_user` `b2` on((`b1`.`user_address` = `b2`.`user_address`)))) `b` on((`a`.`song_id` = `b`.`song_id`)));

-- ----------------------------
-- View structure for v_play_all
-- ----------------------------
DROP VIEW IF EXISTS `v_play_all`;
CREATE ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `v_play_all` AS select `t_play_all`.`user_address` AS `user_address`,`t_play_all`.`user_name` AS `user_name`,`t_play_all`.`user_avatar` AS `user_avatar`,`t_play_all`.`music_id` AS `music_id`,`t_play_all`.`song_id` AS `song_id`,`t_play_all`.`music_name` AS `music_name`,`t_play_all`.`music_url` AS `music_url`,`t_play_all`.`music_seconds` AS `music_seconds`,`t_play_all`.`token_id` AS `token_id`,`t_play_all`.`token_name` AS `token_name`,`t_play_all`.`artist_name` AS `artist_name`,`t_play_all`.`artist_avatar` AS `artist_avatar`,`t_play_all`.`token_symbol` AS `token_symbol`,`t_play_all`.`token_logo` AS `token_logo`,`t_play_all`.`start_time` AS `start_time`,unix_timestamp(`t_play_all`.`create_time`) AS `create_time`,unix_timestamp() AS `now_time` from `t_play_all`;

-- ----------------------------
-- View structure for v_play_user
-- ----------------------------
DROP VIEW IF EXISTS `v_play_user`;
CREATE ALGORITHM = UNDEFINED SQL SECURITY DEFINER VIEW `v_play_user` AS select `t_play_user`.`user_address` AS `user_address`,`t_play_user`.`user_name` AS `user_name`,`t_play_user`.`user_avatar` AS `user_avatar`,`t_play_user`.`music_id` AS `music_id`,`t_play_user`.`song_id` AS `song_id`,`t_play_user`.`music_name` AS `music_name`,`t_play_user`.`music_url` AS `music_url`,`t_play_user`.`music_seconds` AS `music_seconds`,`t_play_user`.`token_id` AS `token_id`,`t_play_user`.`artist_name` AS `artist_name`,`t_play_user`.`artist_avatar` AS `artist_avatar`,`t_play_user`.`token_name` AS `token_name`,`t_play_user`.`token_symbol` AS `token_symbol`,`t_play_user`.`token_logo` AS `token_logo`,`t_play_user`.`start_time` AS `start_time`,unix_timestamp(`t_play_user`.`create_time`) AS `create_time`,unix_timestamp() AS `now_time` from `t_play_user`;

-- ----------------------------
-- Procedure structure for aa
-- ----------------------------
DROP PROCEDURE IF EXISTS `aa`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `aa`()
BEGIN
TRUNCATE TABLE t_artist_redeem;
TRUNCATE TABLE t_claim;
TRUNCATE TABLE t_claim_artist;
TRUNCATE TABLE t_music;
TRUNCATE TABLE t_music_sub;
TRUNCATE TABLE t_user;
TRUNCATE TABLE t_user_related;
TRUNCATE TABLE t_user_redeem;
TRUNCATE TABLE t_whitelist;
TRUNCATE TABLE w_artist;
TRUNCATE TABLE w_finalized;
TRUNCATE TABLE w_music;
TRUNCATE TABLE w_musc_pre;
    END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for get_and_use_slat
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_and_use_slat`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `get_and_use_slat`()
BEGIN
    DECLARE v_id INT;
    DECLARE v_slat CHAR(66);
    START TRANSACTION;
    SELECT id, slat INTO v_id, v_slat
    FROM t_slat
    WHERE used = 0
    LIMIT 1
    FOR UPDATE;
    UPDATE t_slat SET used = 1 WHERE id = v_id;
    COMMIT;
    SELECT v_id AS id, v_slat AS slat;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for get_page
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_page`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `get_page`(_daima VARCHAR(6000),_ps INT,_i INT,_s VARCHAR(6000),_a VARCHAR(4),_w NVARCHAR(6000))
BEGIN
declare _t varchar(20);
	SELECT t INTO _t FROM aux_bt WHERE d=_daima;
	
	IF _w='' THEN 
	SELECT w INTO _w FROM aux_bt WHERE d=_daima;
	END IF;
	
	SET _w=IF(_w='','',CONCAT(' where ',_w));
	
	SET @cqw=CONCAT('SELECT * FROM ',_t,_w,' order by ',_s,' ',_a,' LIMIT ',_ps,' OFFSET ',(_i-1)*_ps);
	PREPARE stmt1 FROM @cqw;
	EXECUTE stmt1 ;
		
	 SET @cqw=CONCAT('SELECT count(*) as mcount FROM ',_t,_w);
	 PREPARE stmt1 FROM @cqw;
         EXECUTE stmt1 ;
	END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for in_play
-- ----------------------------
DROP PROCEDURE IF EXISTS `in_play`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `in_play`(did char(42),musicId int)
BEGIN
	if exists(select 1 from t_music where music_id=musicId) then 
		INSERT INTO t_play_user(user_address,user_name,user_avatar,music_id,song_id,music_name,music_url,music_seconds,token_id,token_name,token_symbol,token_logo,start_time,create_time,artist_name,artist_avatar) 
		SELECT did,b.user_name,b.user_avatar,a.music_id,a.song_id,a.music_name,a.music_url,a.music_seconds,a.token_id,a.token_name,a.token_symbol,a.token_logo,a.start_time,NOW(),b.artist_name,b.artist_avatar FROM t_music a JOIN t_user b ON a.user_address=b.user_address WHERE a.music_id=musicId
		ON DUPLICATE KEY UPDATE create_time = NOW();
		INSERT INTO t_play_all(user_address,user_name,user_avatar,music_id,song_id,music_name,music_url,music_seconds,token_id,token_name,token_symbol,token_logo,start_time,create_time,artist_name,artist_avatar) 
		SELECT a.user_address,b.user_name,b.user_avatar,a.music_id,a.song_id,a.music_name,a.music_url,a.music_seconds,a.token_id,a.token_name,a.token_symbol,a.token_logo,a.start_time,now(),b.artist_name,b.artist_avatar FROM t_music a JOIN t_user b ON a.user_address=b.user_address WHERE a.music_id=musicId
		 ON DUPLICATE KEY UPDATE create_time = NOW();
	end if;
    END
;;
delimiter ;

-- ----------------------------
-- Event structure for day_event
-- ----------------------------
DROP EVENT IF EXISTS `day_event`;
delimiter ;;
CREATE EVENT `day_event`
ON SCHEDULE
EVERY '1' DAY STARTS '2025-09-29 00:00:00'
DO BEGIN
    --   confirm_time 
    DELETE FROM t_music WHERE  song_id=0 AND confirm_time=0 AND DATEDIFF(NOW(), create_time)>1;
END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table t_user
-- ----------------------------
DROP TRIGGER IF EXISTS `up_user`;
delimiter ;;
CREATE TRIGGER `up_user` AFTER UPDATE ON `t_user` FOR EACH ROW BEGIN
    
	update t_play_all set artist_name=new.artist_name,artist_avatar=new.artist_avatar where user_address=new.user_address;
	
    END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table w_artist
-- ----------------------------
DROP TRIGGER IF EXISTS `artist_insert`;
delimiter ;;
CREATE TRIGGER `artist_insert` AFTER INSERT ON `w_artist` FOR EACH ROW BEGIN
	update t_user set user_type=1 where user_address=new.user_address;
    END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table w_finalized
-- ----------------------------
DROP TRIGGER IF EXISTS `finalized_insert`;
delimiter ;;
CREATE TRIGGER `finalized_insert` AFTER INSERT ON `w_finalized` FOR EACH ROW BEGIN
	update t_music set is_end=1 where token_id=new.token_id;
	
    END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table w_musc_pre
-- ----------------------------
DROP TRIGGER IF EXISTS `muscpre_insert`;
delimiter ;;
CREATE TRIGGER `muscpre_insert` AFTER INSERT ON `w_musc_pre` FOR EACH ROW BEGIN
    
     update t_music set confirm_time=new.confirm_time,song_pre_id=new.song_pre_id where music_id=new.music_id;
    END
;;
delimiter ;

-- ----------------------------
-- Triggers structure for table w_music
-- ----------------------------
DROP TRIGGER IF EXISTS `music_insert`;
delimiter ;;
CREATE TRIGGER `music_insert` AFTER INSERT ON `w_music` FOR EACH ROW BEGIN
	update t_music set series_address=new.series_address,memetoken_address=new.memetoken_address,token_id=new.token_id,start_time=new.start_time,song_id=new.song_id where music_id=new.music_id;
    END
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
