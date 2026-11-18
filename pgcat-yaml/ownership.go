package main

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"strconv"
)

// Optional target-side fence. Epoch must be carried by the captured event; it
// must never be inferred from the current owner or the consumer's wall clock.
type OwnershipConfig struct {
	Namespace  string `yaml:"namespace"`
	UIDField   string `yaml:"uid-field"`
	EpochField string `yaml:"epoch-field"`
	Writer     string `yaml:"writer"`
}

const ownershipDDL = `
CREATE TABLE IF NOT EXISTS _pgcat_yaml.write_ownership(
 namespace text NOT NULL,uid text NOT NULL,writer text NOT NULL,
 epoch bigint NOT NULL CHECK(epoch>0),state text NOT NULL CHECK(state IN ('active','draining')),
 PRIMARY KEY(namespace,uid));
CREATE OR REPLACE FUNCTION _pgcat_yaml.assert_writer(p_namespace text,p_uid text,p_writer text,p_epoch bigint)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE v _pgcat_yaml.write_ownership%ROWTYPE;
BEGIN
 SELECT * INTO v FROM _pgcat_yaml.write_ownership
 WHERE namespace=p_namespace AND uid=p_uid FOR SHARE;
 IF NOT FOUND OR p_epoch IS NULL OR p_epoch<=0 OR v.state<>'active'
    OR v.writer IS DISTINCT FROM p_writer OR v.epoch IS DISTINCT FROM p_epoch THEN
   RAISE EXCEPTION 'ownership fenced uid=% writer=% event_epoch=%',p_uid,p_writer,p_epoch;
 END IF;
END $$;
CREATE OR REPLACE FUNCTION _pgcat_yaml.freeze_writer(p_namespace text,p_uid text,p_epoch bigint)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 UPDATE _pgcat_yaml.write_ownership SET state='draining'
 WHERE namespace=p_namespace AND uid=p_uid AND epoch=p_epoch AND state='active';
 IF NOT FOUND THEN RAISE EXCEPTION 'ownership freeze CAS failed'; END IF;
END $$;
CREATE OR REPLACE FUNCTION _pgcat_yaml.activate_writer(p_namespace text,p_uid text,p_old_epoch bigint,p_writer text,p_epoch bigint,p_barriers jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE b record; v_lsn pg_lsn;
BEGIN
 PERFORM 1 FROM _pgcat_yaml.write_ownership WHERE namespace=p_namespace AND uid=p_uid
   AND epoch=p_old_epoch AND state='draining' FOR UPDATE;
 IF NOT FOUND OR p_epoch<=p_old_epoch OR p_writer='' THEN RAISE EXCEPTION 'ownership activation CAS failed'; END IF;
 IF jsonb_typeof(p_barriers)<>'object' OR p_barriers='{}'::jsonb OR p_barriers IS NULL THEN
   RAISE EXCEPTION 'nonempty local checkpoint barriers required'; END IF;
 FOR b IN SELECT * FROM jsonb_each_text(p_barriers) LOOP
   SELECT end_lsn INTO v_lsn FROM _pgcat_yaml.checkpoint WHERE job=b.key;
   IF NOT FOUND OR v_lsn<b.value::pg_lsn THEN RAISE EXCEPTION 'checkpoint barrier not reached for %',b.key; END IF;
 END LOOP;
 UPDATE _pgcat_yaml.write_ownership SET writer=p_writer,epoch=p_epoch,state='active'
 WHERE namespace=p_namespace AND uid=p_uid;
END $$;
`

func (r *runtime) guardOwnership(tx pgx.Tx, before, after Row) error {
	o := r.c.Ownership
	if o == nil {
		return nil
	}
	row := after
	if row == nil {
		row = before
	}
	if row == nil {
		return nil
	}
	uid, ok := row[o.UIDField]
	if !ok || uid == nil {
		return fmt.Errorf("ownership missing uid field %s", o.UIDField)
	}
	if before != nil && after != nil && fmt.Sprint(before[o.UIDField]) != fmt.Sprint(uid) {
		return fmt.Errorf("ownership uid changes require an explicit migration")
	}
	rawEpoch, ok := row[o.EpochField]
	if !ok || rawEpoch == nil {
		return fmt.Errorf("ownership event must carry %s", o.EpochField)
	}
	eventEpoch, e := strconv.ParseInt(fmt.Sprint(rawEpoch), 10, 64)
	if e != nil || eventEpoch <= 0 {
		return fmt.Errorf("invalid ownership event epoch")
	}
	var writer, state string
	var epoch int64
	// SHARE fences a concurrent freeze/activation until target DML+checkpoint commit.
	e = tx.QueryRow(r.ctx, `SELECT writer,epoch,state FROM _pgcat_yaml.write_ownership WHERE namespace=$1 AND uid=$2 FOR SHARE`, o.Namespace, fmt.Sprint(uid)).Scan(&writer, &epoch, &state)
	if e != nil {
		return fmt.Errorf("ownership uid=%v: %w", uid, e)
	}
	if state != "active" || writer != o.Writer || epoch != eventEpoch {
		return fmt.Errorf("ownership fenced uid=%v writer=%s event_epoch=%d active=%s/%d/%s", uid, o.Writer, eventEpoch, writer, epoch, state)
	}
	return nil
}
