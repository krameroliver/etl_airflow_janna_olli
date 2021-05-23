CREATE TABLE bizSCHEMA_ID.h_darlehen(
kontonummer VARCHAR(10),
darlehen_hk CHAR(32),
processing_date DATE,
record_source varchar(255),
darlehen_hk CHAR(32)
,
PRIMARY KEY(darlehen_hk,systemtime)

CREATE TABLE bizSCHEMA_ID.h_darlehen_hist (like bizSCHEMA_ID.h_darlehen including all);

CREATE TRIGGER versioning_trigger_h_darlehen BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.h_darlehen FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.h_darlehen_hist', true);

