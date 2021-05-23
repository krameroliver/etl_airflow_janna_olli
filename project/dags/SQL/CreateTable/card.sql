CREATE TABLE SRCSCHEMA_ID.card(
card_id VARCHAR(10),
disp_id VARCHAR(10),
card_type VARCHAR(100),
year INTEGER,
month INTEGER,
day INTEGER,
fulldate DATE,
card_hk CHAR(32),
processing_valid_from DATE,
processing_valid_to DATE,
systemtime_start timestamp,
systemtime_end timestamp,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
card_hk CHAR(32)
,
PRIMARY KEY(card_hk,systemtime)
);

CREATE TABLE SRCSCHEMA_ID.card_hist (like SRCSCHEMA_ID.card including all);

CREATE TRIGGER versioning_trigger_card BEFORE INSERT OR UPDATE OR DELETE ON SRCSCHEMA_ID.card FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'SRCSCHEMA_ID.card_hist', true);

