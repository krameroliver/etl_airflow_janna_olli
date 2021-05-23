CREATE TABLE bizSCHEMA_ID.s_geschaeftspartner_postalische_addresse(
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.s_geschaeftspartner_postalische_addresse_hist (like bizSCHEMA_ID.s_geschaeftspartner_postalische_addresse including all);

CREATE TRIGGER versioning_trigger_s_geschaeftspartner_postalische_addresse BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.s_geschaeftspartner_postalische_addresse FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.s_geschaeftspartner_postalische_addresse_hist', true);

