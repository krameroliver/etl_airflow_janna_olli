CREATE TABLE bizSCHEMA_ID.s_geschaeftspartner(
kundennummer VARCHAR(18),
anrede VARCHAR(20),
vorname VARCHAR(24),
nachname VARCHAR(24),
geburtsdatum DATE,
sterbedatum DATE,
geschlecht INTEGER,
sozialversicherungsnummer VARCHAR(24),
kreditkartenanzahl INTEGER,
loeschung INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.s_geschaeftspartner_hist (like bizSCHEMA_ID.s_geschaeftspartner including all);

CREATE TRIGGER versioning_trigger_s_geschaeftspartner BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.s_geschaeftspartner FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.s_geschaeftspartner_hist', true);

