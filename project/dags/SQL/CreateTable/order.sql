CREATE TABLE SRCSCHEMA_ID.order(
order_id VARCHAR(10),
account_id VARCHAR(10),
bank_to VARCHAR(2),
account_to INTEGER,
amount decimal(10,2),
k_symbol VARCHAR(100),
order_hk CHAR(32),
processing_valid_from DATE,
processing_valid_to DATE,
systemtime_start timestamp,
systemtime_end timestamp,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
order_hk CHAR(32)
,
PRIMARY KEY(order_hk,systemtime)
);

CREATE TABLE SRCSCHEMA_ID.order_hist (like SRCSCHEMA_ID.order including all);

CREATE TRIGGER versioning_trigger_order BEFORE INSERT OR UPDATE OR DELETE ON SRCSCHEMA_ID.order FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'SRCSCHEMA_ID.order_hist', true);

