CREATE DATABASE IF NOT EXISTS cdc_meta CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS cdc_meta.write_ownership (
  scope_namespace varchar(80) NOT NULL,
  uid bigint NOT NULL CHECK (uid >= 0),
  writer_side varchar(8) NOT NULL CHECK (writer_side IN ('MYSQL','PG')),
  epoch bigint NOT NULL CHECK (epoch > 0),
  state varchar(8) NOT NULL CHECK (state IN ('ACTIVE','DRAINING')),
  barrier_id varchar(80),
  PRIMARY KEY (scope_namespace, uid),
  CHECK ((state = 'ACTIVE' AND barrier_id IS NULL) OR (state = 'DRAINING' AND barrier_id IS NOT NULL))
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cdc_meta.tx_context (
  marker_id varchar(80) PRIMARY KEY,
  scope_namespace varchar(80) NOT NULL,
  root_tx_id varchar(128) NOT NULL,
  writer_side varchar(8) NOT NULL CHECK (writer_side IN ('MYSQL','PG')),
  apply_stage varchar(24) NOT NULL CHECK (apply_stage IN ('native','pg-sink','pgcat-forward','pgcat-reverse','mysql-sink')),
  uid bigint NOT NULL CHECK (uid >= 0),
  epoch bigint NOT NULL CHECK (epoch > 0),
  barrier_id varchar(80)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cdc_meta.applied_tx (
  channel varchar(96) NOT NULL,
  source_stream_id varchar(96) NOT NULL,
  source_tx_key char(64) NOT NULL,
  payload_hash char(64) NOT NULL,
  result varchar(16) NOT NULL,
  PRIMARY KEY (channel, source_stream_id, source_tx_key)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cdc_meta.apply_progress (
  channel varchar(96) PRIMARY KEY,
  source_stream_id varchar(96) NOT NULL,
  started boolean NOT NULL DEFAULT false,
  committed_position text
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cdc_meta.cutover_step (
  barrier_id varchar(80) NOT NULL,
  channel varchar(96) NOT NULL,
  scope_namespace varchar(80) NOT NULL,
  uid bigint NOT NULL,
  epoch bigint NOT NULL,
  source_position text NOT NULL,
  PRIMARY KEY (barrier_id, channel)
) ENGINE=InnoDB;

DROP PROCEDURE IF EXISTS cdc_meta.begin_mysql_write;
DELIMITER //
CREATE PROCEDURE cdc_meta.begin_mysql_write(
  IN p_scope varchar(80), IN p_uid bigint, IN p_epoch bigint, IN p_tx varchar(80))
SQL SECURITY INVOKER
BEGIN
  DECLARE accepted bigint DEFAULT NULL;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET accepted = NULL;
  SELECT epoch INTO accepted FROM cdc_meta.write_ownership
    WHERE scope_namespace=p_scope AND uid=p_uid AND writer_side='MYSQL'
      AND epoch=p_epoch AND state='ACTIVE' FOR SHARE;
  IF accepted IS NULL THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='MySQL write fenced';
  END IF;
  INSERT INTO cdc_meta.tx_context(marker_id,scope_namespace,root_tx_id,writer_side,apply_stage,uid,epoch)
    VALUES (p_tx,p_scope,p_tx,'MYSQL','native',p_uid,p_epoch);
END//
DELIMITER ;
