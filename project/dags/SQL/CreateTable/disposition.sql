CREATE TABLE SRC.disposition(
disp_id VARCHAR(10),
client_id VARCHAR(10),
account_id VARCHAR(10),
user_type VARCHAR(6),
disposition_hk CHAR(32),
processing_valid_from DATE,
processing_valid_to DATE,
systemtime_start timestamp,
systemtime_end timestamp,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
disposition_hk CHAR(32)
,
PRIMARY KEY(disposition_hk,systemtime)
);

CREATE TABLE SRC.disposition_hist (like SRC.disposition including all);

CREATE TRIGGER versioning_trigger_disposition BEFORE INSERT OR UPDATE OR DELETE ON SRC.disposition FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'SRC.disposition_hist', true);

