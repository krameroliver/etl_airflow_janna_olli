CREATE TABLE bizSCHEMA_ID.h_time(
processingday DATE,
loadingtime timestamp,
time_hk CHAR(32),
processing_date DATE,
record_source varchar(255),
time_hk CHAR(32)
,
PRIMARY KEY(time_hk,systemtime)

CREATE TABLE bizSCHEMA_ID.h_time_hist (like bizSCHEMA_ID.h_time including all);

CREATE TRIGGER versioning_trigger_h_time BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.h_time FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.h_time_hist', true);

