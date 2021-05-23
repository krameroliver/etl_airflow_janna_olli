CREATE TABLE bizSCHEMA_ID.h_geschaeftspartner(
kundennummer VARCHAR(18),
geschaeftspartner_hk CHAR(32),
processing_date DATE,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,systemtime)

CREATE TABLE bizSCHEMA_ID.h_geschaeftspartner_hist (like bizSCHEMA_ID.h_geschaeftspartner including all);

CREATE TRIGGER versioning_trigger_h_geschaeftspartner BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.h_geschaeftspartner FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.h_geschaeftspartner_hist', true);

