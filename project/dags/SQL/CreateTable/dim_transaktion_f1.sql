CREATE TABLE rpt_bi.dim_transaktion_f1(
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
);

CREATE TABLE rpt_bi.dim_transaktion_f1_hist (like rpt_bi.dim_transaktion_f1 including all);

CREATE TRIGGER versioning_trigger_dim_transaktion_f1 BEFORE INSERT OR UPDATE OR DELETE ON rpt_bi.dim_transaktion_f1 FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'rpt_bi.dim_transaktion_f1_hist', true);

