CREATE SCHEMA legacy;
CREATE TABLE legacy.orders_0(id bigint PRIMARY KEY,uid bigint NOT NULL,customer_name text,document jsonb,barcode text);
CREATE TABLE legacy.orders_1(LIKE legacy.orders_0 INCLUDING ALL);
INSERT INTO legacy.orders_1 VALUES(9007199254740993,0,'Alice',
 '{"stock":{"reserved":4,"available":30},"unmanaged":{"code":9007199254740993}}','keep-barcode');
