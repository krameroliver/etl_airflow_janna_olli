CREATE TABLE bizSCHEMA_ID.s_time(
time_hk CHAR(32),
processingday DATE,
isbusinessday INTEGER,
isendofmonth INTEGER,
isendofquarter INTEGER,
isendofhalfyear INTEGER,
isendofyear INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
PRIMARY KEY(time_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.s_time_hist (like bizSCHEMA_ID.s_time including all);

CREATE TRIGGER versioning_trigger_s_time BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.s_time FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.s_time_hist', true);

