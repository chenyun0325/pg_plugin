CREATE SCHEMA IF NOT EXISTS cdc_meta;
CREATE SCHEMA IF NOT EXISTS pgflo_meta;
CREATE TABLE IF NOT EXISTS cdc_meta.write_ownership(
 scope_namespace varchar(80) NOT NULL,uid bigint NOT NULL CHECK(uid>=0),
 writer_side varchar(8) NOT NULL CHECK(writer_side IN ('MYSQL','PG')),
 epoch bigint NOT NULL CHECK(epoch>0),state varchar(8) NOT NULL CHECK(state IN ('ACTIVE','DRAINING')),
 barrier_id varchar(80),PRIMARY KEY(scope_namespace,uid),
 CHECK((state='ACTIVE' AND barrier_id IS NULL) OR (state='DRAINING' AND barrier_id IS NOT NULL)));
CREATE TABLE IF NOT EXISTS cdc_meta.tx_context(
 marker_id varchar(80) PRIMARY KEY,scope_namespace varchar(80) NOT NULL,root_tx_id varchar(128) NOT NULL,
 writer_side varchar(8) NOT NULL,apply_stage varchar(24) NOT NULL,uid bigint NOT NULL,epoch bigint NOT NULL,barrier_id varchar(80));
CREATE TABLE IF NOT EXISTS cdc_meta.applied_tx(
 channel varchar(96) NOT NULL,source_stream_id varchar(96) NOT NULL,source_tx_key char(64) NOT NULL,
 payload_hash char(64) NOT NULL,result varchar(16) NOT NULL,PRIMARY KEY(channel,source_stream_id,source_tx_key));
CREATE TABLE IF NOT EXISTS cdc_meta.apply_progress(
 channel varchar(96) PRIMARY KEY,source_stream_id varchar(96) NOT NULL,started boolean NOT NULL DEFAULT false,committed_position text);
CREATE TABLE IF NOT EXISTS cdc_meta.cutover_step(
 barrier_id varchar(80) NOT NULL,channel varchar(96) NOT NULL,scope_namespace varchar(80) NOT NULL,
 uid bigint NOT NULL,epoch bigint NOT NULL,source_position text NOT NULL,PRIMARY KEY(barrier_id,channel));
CREATE TABLE IF NOT EXISTS pgflo_meta.replay_binding(
 channel text PRIMARY KEY,stream_id text NOT NULL,config_hash text NOT NULL);
CREATE TABLE IF NOT EXISTS pgflo_meta.replay_baseline(channel text PRIMARY KEY,slot text NOT NULL,start_lsn pg_lsn NOT NULL);
CREATE TABLE IF NOT EXISTS pgflo_meta.source_rows(
 channel text NOT NULL,source_table text NOT NULL,source_pk text NOT NULL,row_data jsonb NOT NULL,
 PRIMARY KEY(channel,source_table,source_pk));
CREATE TABLE IF NOT EXISTS pgflo_meta.cutover_history(
 scope_namespace text NOT NULL,uid bigint NOT NULL,old_epoch bigint NOT NULL,new_epoch bigint NOT NULL,
 old_writer text NOT NULL,new_writer text NOT NULL,barrier_id text NOT NULL,channels text[] NOT NULL,
 PRIMARY KEY(scope_namespace,uid,old_epoch));

CREATE OR REPLACE FUNCTION cdc_meta.begin_write(p_scope text,p_uid bigint,p_writer text,p_epoch bigint,p_tx text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM cdc_meta.write_ownership WHERE scope_namespace=p_scope AND uid=p_uid
 AND writer_side=p_writer AND epoch=p_epoch AND state='ACTIVE' FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'business write fenced uid=% writer=% epoch=%',p_uid,p_writer,p_epoch; END IF;
 INSERT INTO cdc_meta.tx_context VALUES(p_tx,p_scope,p_tx,p_writer,'native',p_uid,p_epoch,NULL);
END $$;

CREATE OR REPLACE FUNCTION cdc_meta.freeze_writer(p_scope text,p_uid bigint,p_writer text,p_epoch bigint,p_barrier text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE v cdc_meta.write_ownership%ROWTYPE;
BEGIN
 IF p_barrier IS NULL OR p_barrier='' THEN RAISE EXCEPTION 'barrier required'; END IF;
 SELECT * INTO v FROM cdc_meta.write_ownership WHERE scope_namespace=p_scope AND uid=p_uid FOR UPDATE;
 IF NOT FOUND OR v.writer_side<>p_writer OR v.epoch<>p_epoch THEN RAISE EXCEPTION 'freeze CAS failed'; END IF;
 IF v.state='DRAINING' AND v.barrier_id=p_barrier THEN RETURN; END IF;
 IF v.state<>'ACTIVE' THEN RAISE EXCEPTION 'different cutover already pending'; END IF;
 UPDATE cdc_meta.write_ownership SET state='DRAINING',barrier_id=p_barrier WHERE scope_namespace=p_scope AND uid=p_uid;
END $$;

CREATE OR REPLACE FUNCTION cdc_meta.emit_barrier(p_scope text,p_uid bigint,p_writer text,p_epoch bigint,p_barrier text,p_stage text DEFAULT 'native')
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 PERFORM 1 FROM cdc_meta.write_ownership WHERE scope_namespace=p_scope AND uid=p_uid AND writer_side=p_writer
 AND epoch=p_epoch AND state='DRAINING' AND barrier_id=p_barrier FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'barrier CAS failed'; END IF;
 IF EXISTS(SELECT FROM cdc_meta.tx_context WHERE marker_id=p_barrier AND
 (scope_namespace<>p_scope OR uid<>p_uid OR epoch<>p_epoch OR writer_side<>p_writer OR apply_stage<>p_stage OR barrier_id IS DISTINCT FROM p_barrier))
 THEN RAISE EXCEPTION 'barrier marker collision'; END IF;
 INSERT INTO cdc_meta.tx_context VALUES(p_barrier,p_scope,p_barrier,p_writer,p_stage,p_uid,p_epoch,p_barrier) ON CONFLICT DO NOTHING;
END $$;

CREATE OR REPLACE FUNCTION cdc_meta.activate_writer(p_scope text,p_uid bigint,p_writer text,p_epoch bigint,p_next text,p_next_epoch bigint,p_barrier text,p_channels text[])
RETURNS void LANGUAGE plpgsql AS $$
DECLARE v cdc_meta.write_ownership%ROWTYPE; ch text;
BEGIN
 SELECT * INTO v FROM cdc_meta.write_ownership WHERE scope_namespace=p_scope AND uid=p_uid FOR UPDATE;
 IF p_next NOT IN ('MYSQL','PG') OR p_next=p_writer OR p_next_epoch<>p_epoch+1 OR p_channels IS NULL OR cardinality(p_channels)=0
 THEN RAISE EXCEPTION 'invalid next writer/epoch or required channels'; END IF;
 IF v.writer_side=p_next AND v.epoch=p_next_epoch AND v.state='ACTIVE' AND EXISTS(
 SELECT FROM pgflo_meta.cutover_history WHERE scope_namespace=p_scope AND uid=p_uid AND old_epoch=p_epoch
 AND new_writer=p_next AND old_writer=p_writer AND barrier_id=p_barrier AND channels=p_channels) THEN RETURN; END IF;
 IF NOT FOUND OR v.writer_side<>p_writer OR v.epoch<>p_epoch OR v.state<>'DRAINING' OR v.barrier_id IS DISTINCT FROM p_barrier
 THEN RAISE EXCEPTION 'activation CAS failed'; END IF;
 FOREACH ch IN ARRAY p_channels LOOP
  IF NOT EXISTS(SELECT FROM cdc_meta.cutover_step WHERE barrier_id=p_barrier AND channel=ch AND scope_namespace=p_scope AND uid=p_uid AND epoch=p_epoch)
  THEN RAISE EXCEPTION 'barrier not reached: %',ch; END IF;
 END LOOP;
 INSERT INTO pgflo_meta.cutover_history VALUES(p_scope,p_uid,p_epoch,p_next_epoch,p_writer,p_next,p_barrier,p_channels);
 UPDATE cdc_meta.write_ownership SET writer_side=p_next,epoch=p_next_epoch,state='ACTIVE',barrier_id=NULL WHERE scope_namespace=p_scope AND uid=p_uid;
END $$;
