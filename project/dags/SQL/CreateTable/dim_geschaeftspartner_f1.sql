CREATE TABLE rpt_bi.dim_geschaeftspartner_f1(
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
kontakttyp INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
);

CREATE TABLE rpt_bi.dim_geschaeftspartner_f1_hist (like rpt_bi.dim_geschaeftspartner_f1 including all);

CREATE TRIGGER versioning_trigger_dim_geschaeftspartner_f1 BEFORE INSERT OR UPDATE OR DELETE ON rpt_bi.dim_geschaeftspartner_f1 FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'rpt_bi.dim_geschaeftspartner_f1_hist', true);

