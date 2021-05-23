CREATE TABLE SRCSCHEMA_ID.loan(
loan_id VARCHAR(10),
account_id VARCHAR(10),
amount INTEGER,
duration INTEGER,
payments INTEGER,
status VARCHAR(1),
year INTEGER,
month INTEGER,
day INTEGER,
fulldate DATE,
location INTEGER,
purpose VARCHAR(255),
loan_hk CHAR(32),
processing_valid_from DATE,
processing_valid_to DATE,
systemtime_start timestamp,
systemtime_end timestamp,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
loan_hk CHAR(32)
,
PRIMARY KEY(loan_hk,systemtime)
);

CREATE TABLE SRCSCHEMA_ID.loan_hist (like SRCSCHEMA_ID.loan including all);

CREATE TRIGGER versioning_trigger_loan BEFORE INSERT OR UPDATE OR DELETE ON SRCSCHEMA_ID.loan FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'SRCSCHEMA_ID.loan_hist', true);

