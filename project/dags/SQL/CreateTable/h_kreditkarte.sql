CREATE TABLE bizSCHEMA_ID.h_kreditkarte(
kartennummer VARCHAR(20),
kreditkarte_hk CHAR(32),
processing_date DATE,
record_source varchar(255),
kreditkarte_hk CHAR(32)
,
PRIMARY KEY(kreditkarte_hk,systemtime)

CREATE TABLE bizSCHEMA_ID.h_kreditkarte_hist (like bizSCHEMA_ID.h_kreditkarte including all);

CREATE TRIGGER versioning_trigger_h_kreditkarte BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.h_kreditkarte FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.h_kreditkarte_hist', true);

