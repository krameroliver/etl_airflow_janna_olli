CREATE TABLE rpt_bi.dim_kreditkarte_f1(
kartennummer VARCHAR(20),
beginndatum DATE,
kartentyp INTEGER,
loeschung INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
kreditkarte_hk CHAR(32)
);

CREATE TABLE rpt_bi.dim_kreditkarte_f1_hist (like rpt_bi.dim_kreditkarte_f1 including all);

CREATE TRIGGER versioning_trigger_dim_kreditkarte_f1 BEFORE INSERT OR UPDATE OR DELETE ON rpt_bi.dim_kreditkarte_f1 FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'rpt_bi.dim_kreditkarte_f1_hist', true);

