CREATE TABLE biz_python.h_darlehen(
kontonummer VARCHAR(10),
darlehen_hk CHAR(32),
PRIMARY KEY(darlehen_hk));



CREATE TABLE biz_python.h_geschaeftspartner(
kundennummer VARCHAR(18),
geschaeftspartner_hk CHAR(32),
PRIMARY KEY(geschaeftspartner_hk));




CREATE TABLE biz_python.h_kreditkarte(
kartennummer VARCHAR(20),
kreditkarte_hk CHAR(32)
,
PRIMARY KEY(kreditkarte_hk));


CREATE TABLE biz_python.h_time(
processingday DATE,
loadingtime timestamp,
time_hk CHAR(32)
,
PRIMARY KEY(time_hk));



CREATE TABLE biz_python.h_transaktion(
transaktions_id VARCHAR(20),
transaktion_hk CHAR(32)
,
PRIMARY KEY(transaktion_hk));


CREATE TABLE biz_python.m_geschaeftspartner_digitale_addresse(
kontakttyp INTEGER,
kontaktinfo VARCHAR(255),
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,kontakttyp,processing_date_end)
);

CREATE TABLE biz_python.m_geschaeftspartner_digitale_addresse_hist (like biz_python.m_geschaeftspartner_digitale_addresse including all);

CREATE TRIGGER versioning_trigger_m_geschaeftspartner_digitale_addresse BEFORE INSERT OR UPDATE OR DELETE ON biz_python.m_geschaeftspartner_digitale_addresse FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.m_geschaeftspartner_digitale_addresse_hist', true);




CREATE TABLE biz_python.s_darlehen(
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
PRIMARY KEY(darlehen_hk,processing_date_end)
);

CREATE TABLE biz_python.s_darlehen_hist (like biz_python.s_darlehen including all);

CREATE TRIGGER versioning_trigger_s_darlehen BEFORE INSERT OR UPDATE OR DELETE ON biz_python.s_darlehen FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.s_darlehen_hist', true);



CREATE TABLE biz_python.s_geschaeftspartner_postalische_addresse(
addresse1 VARCHAR(100),
addresse2 VARCHAR(100), 
stadt VARCHAR(100),
bundesland VARCHAR(100),
postleitzahl VARCHAR(100),
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,processing_date_end)
);

CREATE TABLE biz_python.s_geschaeftspartner_postalische_addresse_hist (like biz_python.s_geschaeftspartner_postalische_addresse including all);

CREATE TRIGGER versioning_trigger_s_geschaeftspartner_postalische_addresse BEFORE INSERT OR UPDATE OR DELETE ON biz_python.s_geschaeftspartner_postalische_addresse FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.s_geschaeftspartner_postalische_addresse_hist', true);



CREATE TABLE biz_python.s_geschaeftspartner(
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
PRIMARY KEY(geschaeftspartner_hk,processing_date_end)
);

CREATE TABLE biz_python.s_geschaeftspartner_hist (like biz_python.s_geschaeftspartner including all);

CREATE TRIGGER versioning_trigger_s_geschaeftspartner BEFORE INSERT OR UPDATE OR DELETE ON biz_python.s_geschaeftspartner FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.s_geschaeftspartner_hist', true);



CREATE TABLE biz_python.s_kreditkarte(
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
PRIMARY KEY(kreditkarte_hk,processing_date_end)
);

CREATE TABLE biz_python.s_kreditkarte_hist (like biz_python.s_kreditkarte including all);

CREATE TRIGGER versioning_trigger_s_kreditkarte BEFORE INSERT OR UPDATE OR DELETE ON biz_python.s_kreditkarte FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.s_kreditkarte_hist', true);



CREATE TABLE biz_python.s_time(
time_hk CHAR(32),
processingday DATE,
isbusinessday INTEGER,
isendofmonth INTEGER,
isendofquarter INTEGER,
isendofhalfyear INTEGER,
isendofyear INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
PRIMARY KEY(time_hk,processing_date_end)
);

CREATE TABLE biz_python.s_time_hist (like biz_python.s_time including all);

CREATE TRIGGER versioning_trigger_s_time BEFORE INSERT OR UPDATE OR DELETE ON biz_python.s_time FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.s_time_hist', true);



CREATE TABLE biz_python.s_transaktion(
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
,
PRIMARY KEY(transaktion_hk,processing_date_end)
);

CREATE TABLE biz_python.s_transaktion_hist (like biz_python.s_transaktion including all);

CREATE TRIGGER versioning_trigger_s_transaktion BEFORE INSERT OR UPDATE OR DELETE ON biz_python.s_transaktion FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.s_transaktion_hist', true);



-------------------------------------- LINKS ---------------------------------


CREATE TABLE biz_python.r_link_cc_trans(
link_cc_trans_hk CHAR(32),
kreditkarte_hk CHAR(32),
transaktion_hk CHAR(32),
PRIMARY KEY(link_cc_trans_hk));

CREATE TABLE biz_python.r_s_link_cc_trans(
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
link_cc_trans_hk CHAR(32),
PRIMARY KEY(link_cc_trans_hk,processing_date_end)
);

CREATE TABLE biz_python.r_s_link_cc_trans_hist (like biz_python.r_s_link_cc_trans including all);

CREATE TRIGGER versioning_trigger_r_s_link_cc_trans BEFORE INSERT OR UPDATE OR DELETE ON biz_python.r_s_link_cc_trans FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.r_s_link_cc_trans_hist', true);




CREATE TABLE biz_python.r_link_darlehen_trans(
link_darlehen_trans_hk CHAR(32),
darlehen_hk CHAR(32),
transaktion_hk CHAR(32),
PRIMARY KEY(link_darlehen_trans_hk));


CREATE TABLE biz_python.r_s_link_darlehen_trans(
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
link_darlehen_trans_hk CHAR(32)
,
PRIMARY KEY(link_darlehen_trans_hk,processing_date_start)
);

CREATE TABLE biz_python.r_s_link_darlehen_trans_hist (like biz_python.r_s_link_darlehen_trans including all);

CREATE TRIGGER versioning_trigger_r_s_link_darlehen_trans BEFORE INSERT OR UPDATE OR DELETE ON biz_python.r_s_link_darlehen_trans FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.r_s_link_darlehen_trans_hist', true);




CREATE TABLE biz_python.r_link_gp_cc(
link_gp_cc_hk CHAR(32),
kreditkarte_hk CHAR(32),
geschaeftspartner_hk CHAR(32),
PRIMARY KEY(link_gp_cc_hk));



CREATE TABLE biz_python.r_s_link_gp_cc(
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
link_gp_cc_hk CHAR(32),
PRIMARY KEY(link_gp_cc_hk,processing_date_start)
);

CREATE TABLE biz_python.r_s_link_gp_cc_hist (like biz_python.r_s_link_gp_cc including all);

CREATE TRIGGER versioning_trigger_r_s_link_gp_cc BEFORE INSERT OR UPDATE OR DELETE ON biz_python.r_s_link_gp_cc FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.r_s_link_gp_cc_hist', true);


CREATE TABLE biz_python.r_link_gp_darlehen(
link_gp_darlehen_hk CHAR(32),
darlehen_hk CHAR(32),
geschaeftspartner_hk CHAR(32),
PRIMARY KEY(link_gp_darlehen_hk));



CREATE TABLE biz_python.r_s_link_gp_darlehen(
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
link_gp_darlehen_hk CHAR(32)
,
PRIMARY KEY(link_gp_darlehen_hk,processing_date_start)
);

CREATE TABLE biz_python.r_s_link_gp_darlehen_hist (like biz_python.r_s_link_gp_darlehen including all);

CREATE TRIGGER versioning_trigger_r_s_link_gp_darlehen BEFORE INSERT OR UPDATE OR DELETE ON biz_python.r_s_link_gp_darlehen FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'biz_python.r_s_link_gp_darlehen_hist', true);



