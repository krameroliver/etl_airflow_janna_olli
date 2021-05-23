CREATE TABLE bizSCHEMA_ID.s_darlehen(
kontonummer VARCHAR(10),
nominal decimal(10,2),
startdatum DATE,
enddatum DATE,
status INTEGER,
tilgung decimal(10,2),
verwendungszweck INTEGER,
futurecashflow INTEGER,
loeschung INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
darlehen_hk CHAR(32)
,
PRIMARY KEY(darlehen_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.s_darlehen_hist (like bizSCHEMA_ID.s_darlehen including all);

CREATE TRIGGER versioning_trigger_s_darlehen BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.s_darlehen FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.s_darlehen_hist', true);

