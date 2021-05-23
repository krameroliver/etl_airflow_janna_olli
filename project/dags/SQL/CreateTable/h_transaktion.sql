CREATE TABLE bizSCHEMA_ID.h_transaktion(
transaktions_id VARCHAR(20),
transaktion_hk CHAR(32),
processing_date DATE,
record_source varchar(255),
transaktion_hk CHAR(32)
,
PRIMARY KEY(transaktion_hk,systemtime)

CREATE TABLE bizSCHEMA_ID.h_transaktion_hist (like bizSCHEMA_ID.h_transaktion including all);

CREATE TRIGGER versioning_trigger_h_transaktion BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.h_transaktion FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.h_transaktion_hist', true);

