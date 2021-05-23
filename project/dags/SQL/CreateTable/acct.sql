CREATE TABLE SRCSCHEMA_ID.acct(
account_id VARCHAR(11),
district_id INTEGER,
frequency VARCHAR(100),
parseddate DATE,
year INTEGER,
month INTEGER,
day INTEGER,
acct_hk CHAR(32),
processing_valid_from DATE,
processing_valid_to DATE,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
acct_hk CHAR(32)
,
PRIMARY KEY(acct_hk,systemtime)
);

CREATE TABLE SRCSCHEMA_ID.acct_hist (like SRCSCHEMA_ID.acct including all);

CREATE TRIGGER versioning_trigger_acct BEFORE INSERT OR UPDATE OR DELETE ON SRCSCHEMA_ID.acct FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'SRCSCHEMA_ID.acct_hist', true);

