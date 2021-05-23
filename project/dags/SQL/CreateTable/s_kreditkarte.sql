CREATE TABLE bizSCHEMA_ID.s_kreditkarte(
kartennummer VARCHAR(20),
beginndatum DATE,
kartentyp INTEGER,
loeschung INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
kreditkarte_hk CHAR(32)
,
PRIMARY KEY(kreditkarte_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.s_kreditkarte_hist (like bizSCHEMA_ID.s_kreditkarte including all);

CREATE TRIGGER versioning_trigger_s_kreditkarte BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.s_kreditkarte FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.s_kreditkarte_hist', true);

