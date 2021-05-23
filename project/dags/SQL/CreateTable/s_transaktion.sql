CREATE TABLE bizSCHEMA_ID.s_transaktion(
transaktions_id VARCHAR(20),
ausfuehrungsdatum DATE,
betrag decimal(20,10),
buchungsart VARCHAR(50),
loeschung INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
transaktion_hk CHAR(32)
,
PRIMARY KEY(transaktion_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.s_transaktion_hist (like bizSCHEMA_ID.s_transaktion including all);

CREATE TRIGGER versioning_trigger_s_transaktion BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.s_transaktion FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.s_transaktion_hist', true);

