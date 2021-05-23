CREATE TABLE SRCSCHEMA_ID.trans(
run_id VARCHAR(100),
trans_id VARCHAR(10),
account_id VARCHAR(10),
trans_type VARCHAR(100),
operation VARCHAR(100),
amount VARCHAR(100),
balance VARCHAR(100),
k_symbol VARCHAR(100),
bank VARCHAR(2),
account VARCHAR(255),
year INTEGER,
month INTEGER,
day INTEGER,
fulldate VARCHAR(100),
fulltime VARCHAR(10),
fulldatewithtime VARCHAR(100),
trans_hk CHAR(32),
processing_valid_from DATE,
processing_valid_to DATE,
systemtime_start timestamp,
systemtime_end timestamp,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
trans_hk CHAR(32)
,
PRIMARY KEY(trans_hk,systemtime)
);

CREATE TABLE SRCSCHEMA_ID.trans_hist (like SRCSCHEMA_ID.trans including all);

CREATE TRIGGER versioning_trigger_trans BEFORE INSERT OR UPDATE OR DELETE ON SRCSCHEMA_ID.trans FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'SRCSCHEMA_ID.trans_hist', true);

