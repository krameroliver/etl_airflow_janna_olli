CREATE TABLE bizSCHEMA_ID.m_geschaeftspartner_digitale_addresse(
kontakttyp INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.m_geschaeftspartner_digitale_addresse_hist (like bizSCHEMA_ID.m_geschaeftspartner_digitale_addresse including all);

CREATE TRIGGER versioning_trigger_m_geschaeftspartner_digitale_addresse BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.m_geschaeftspartner_digitale_addresse FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.m_geschaeftspartner_digitale_addresse_hist', true);

