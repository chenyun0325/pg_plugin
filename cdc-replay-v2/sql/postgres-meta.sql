CREATE SCHEMA IF NOT EXISTS cdc_meta;
CREATE TABLE IF NOT EXISTS cdc_meta.write_ownership (
  scope_namespace varchar(80) NOT NULL,
  uid bigint NOT NULL CHECK (uid >= 0),
  writer_side varchar(8) NOT NULL CHECK (writer_side IN ('MYSQL','PG')),
  epoch bigint NOT NULL CHECK (epoch > 0),
  state varchar(8) NOT NULL CHECK (state IN ('ACTIVE','DRAINING')),
  barrier_id varchar(80),
  PRIMARY KEY (scope_namespace, uid),
  CHECK ((state = 'ACTIVE' AND barrier_id IS NULL) OR (state = 'DRAINING' AND barrier_id IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS cdc_meta.tx_context (
  marker_id varchar(80) PRIMARY KEY,
  scope_namespace varchar(80) NOT NULL,
  root_tx_id varchar(128) NOT NULL,
  writer_side varchar(8) NOT NULL CHECK (writer_side IN ('MYSQL','PG')),
  apply_stage varchar(24) NOT NULL CHECK (apply_stage IN ('native','pg-sink','pgcat-forward','pgcat-reverse','mysql-sink')),
  uid bigint NOT NULL CHECK (uid >= 0),
  epoch bigint NOT NULL CHECK (epoch > 0),
  barrier_id varchar(80)
);
CREATE TABLE IF NOT EXISTS cdc_meta.applied_tx (
  channel varchar(96) NOT NULL,
  source_stream_id varchar(96) NOT NULL,
  source_tx_key char(64) NOT NULL,
  payload_hash char(64) NOT NULL,
  result varchar(16) NOT NULL,
  PRIMARY KEY (channel, source_stream_id, source_tx_key)
);
CREATE TABLE IF NOT EXISTS cdc_meta.apply_progress (
  channel varchar(96) PRIMARY KEY,
  source_stream_id varchar(96) NOT NULL,
  started boolean NOT NULL DEFAULT false,
  committed_position text
);
CREATE TABLE IF NOT EXISTS cdc_meta.cutover_step (
  barrier_id varchar(80) NOT NULL,
  channel varchar(96) NOT NULL,
  scope_namespace varchar(80) NOT NULL,
  uid bigint NOT NULL,
  epoch bigint NOT NULL,
  source_position text NOT NULL,
  PRIMARY KEY (barrier_id, channel)
);

CREATE OR REPLACE FUNCTION cdc_meta.begin_pg_write(p_scope text, p_uid bigint, p_epoch bigint, p_tx text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM cdc_meta.write_ownership
    WHERE scope_namespace=p_scope AND uid=p_uid AND writer_side='PG'
      AND epoch=p_epoch AND state='ACTIVE' FOR SHARE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'PG write fenced for % UID % epoch %', p_scope, p_uid, p_epoch;
  END IF;
  INSERT INTO cdc_meta.tx_context(marker_id,scope_namespace,root_tx_id,writer_side,apply_stage,uid,epoch)
    VALUES (p_tx,p_scope,p_tx,'PG','native',p_uid,p_epoch);
END;
$$;
