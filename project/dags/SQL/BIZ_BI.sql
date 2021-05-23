CREATE TABLE biz.r_link_cc_trans(
link_cc_trans_hk CHAR(32),
kreditkarte_hk CHAR(32),
transaktion_hk CHAR(32),
PRIMARY KEY(link_cc_trans_hk),
CONSTRAINT r_link_cc_trans__trans UNIQUE (transaktion_hk),
CONSTRAINT r_link_cc_trans_cc UNIQUE (kreditkarte_hk)
);



CREATE TABLE biz.r_link_darlehen_trans(
link_darlehen_trans_hk CHAR(32),
darlehen_hk CHAR(32),
transaktion_hk CHAR(32),
PRIMARY KEY(link_darlehen_trans_hk),
CONSTRAINT r_link_darlehen_trans__trans UNIQUE (transaktion_hk),
CONSTRAINT r_link_darlehen_trans__darlehen UNIQUE (darlehen_hk)
);


CREATE TABLE biz.r_link_gp_cc(
link_gp_cc_hk CHAR(32),
kreditkarte_hk CHAR(32),
geschaeftspartner_hk CHAR(32),
PRIMARY KEY(link_gp_cc_hk),
CONSTRAINT r_link_gp_cc__gp UNIQUE (geschaeftspartner_hk),
CONSTRAINT r_link_gp_cc__cc UNIQUE (kreditkarte_hk)
);


CREATE TABLE biz.r_link_gp_darlehen(
link_gp_darlehen_hk CHAR(32),
darlehen_hk CHAR(32),
geschaeftspartner_hk CHAR(32),
PRIMARY KEY(link_gp_darlehen_hk),
CONSTRAINT r_link_gp_darlehen__darlehen UNIQUE (darlehen_hk),
CONSTRAINT r_link_gp_darlehen__gp UNIQUE (geschaeftspartner_hk)
);

-------------------------------------------------------------------


CREATE TABLE biz.darlehen(
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
systemtime_create timestamp,
systemtime_modifiy timestamp,
record_source varchar(255),
diff_hk CHAR(32),
darlehen_hk CHAR(32),
PRIMARY KEY(darlehen_hk,processing_date_end,record_source)
);

ALTER TABLE biz.darlehen ADD CONSTRAINT darlehen_fk__r_link_darlehen_trans FOREIGN KEY (darlehen_hk) REFERENCES biz.r_link_darlehen_trans(darlehen_hk);
ALTER TABLE biz.darlehen ADD CONSTRAINT darlehen_fk__r_link_gp_darlehen FOREIGN KEY (darlehen_hk) REFERENCES biz.r_link_gp_darlehen(darlehen_hk);
--CREATE TABLE biz.darlehen_hist (like biz.darlehen including all);



CREATE TABLE biz.geschaeftspartner_digitale_addresse(
kontakttyp INTEGER,
kontaktinfo VarCHAR(255),
geschaeftspartner_hk CHAR(32),
processing_date_start DATE,
processing_date_end   DATE,
systemtime_create timestamp,
systemtime_modifiy timestamp,
record_source varchar(255),
diff_hk CHAR(32),
PRIMARY KEY(geschaeftspartner_hk,kontakttyp,processing_date_end,record_source));

ALTER TABLE biz.geschaeftspartner_digitale_addresse ADD CONSTRAINT geschaeftspartner_digitale_addresse_fk__r_link_gp_cc FOREIGN KEY (geschaeftspartner_hk) REFERENCES biz.r_link_gp_cc(geschaeftspartner_hk);
ALTER TABLE biz.geschaeftspartner_digitale_addresse ADD CONSTRAINT geschaeftspartner_digitale_addresse_fk__r_link_gp_darlehen FOREIGN KEY (geschaeftspartner_hk) REFERENCES biz.r_link_gp_darlehen(geschaeftspartner_hk);
--CREATE TABLE biz.geschaeftspartner_digitale_addresse_hist (like biz.geschaeftspartner_digitale_addresse including all);




CREATE TABLE biz.geschaeftspartner_postalische_addresse(
processing_date_start DATE,
processing_date_end   DATE,
systemtime_create timestamp,
systemtime_modifiy timestamp,
record_source varchar(255),
diff_hk CHAR(32),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,processing_date_end,record_source));

ALTER TABLE biz.geschaeftspartner_postalische_addresse ADD CONSTRAINT geschaeftspartner_postalische_addresse_fk__r_link_gp_cc FOREIGN KEY (geschaeftspartner_hk) REFERENCES biz.r_link_gp_cc(geschaeftspartner_hk);
ALTER TABLE biz.geschaeftspartner_postalische_addresse ADD CONSTRAINT geschaeftspartner_postalische_addresse_fk__r_link_gp_darlehen FOREIGN KEY (geschaeftspartner_hk) REFERENCES biz.r_link_gp_darlehen(geschaeftspartner_hk);
--CREATE TABLE biz.geschaeftspartner_postalische_addresse_hist (like biz.geschaeftspartner_postalische_addresse including all);






CREATE TABLE biz.geschaeftspartner(
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
systemtime_create timestamp,
systemtime_modifiy timestamp,
record_source varchar(255),
diff_hk CHAR(32),
geschaeftspartner_hk CHAR(32));

ALTER TABLE biz.geschaeftspartner ADD CONSTRAINT geschaeftspartner_fk__r_link_gp_cc FOREIGN KEY (geschaeftspartner_hk) REFERENCES biz.r_link_gp_cc(geschaeftspartner_hk);
ALTER TABLE biz.geschaeftspartner ADD CONSTRAINT geschaeftspartner_fk__r_link_gp_darlehen FOREIGN KEY (geschaeftspartner_hk) REFERENCES biz.r_link_gp_darlehen(geschaeftspartner_hk);
--CREATE TABLE biz.geschaeftspartner_hist (like biz.geschaeftspartner including all);




CREATE TABLE biz.kreditkarte(
kartennummer VARCHAR(20),
beginndatum DATE,
kartentyp INTEGER,
loeschung INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime_create timestamp,
systemtime_modifiy timestamp,
record_source varchar(255),
diff_hk CHAR(32),
kreditkarte_hk CHAR(32),
PRIMARY KEY(kreditkarte_hk,processing_date_end,record_source)
);

ALTER TABLE biz.kreditkarte ADD CONSTRAINT kreditkarte_fk__r_link_gp_cc FOREIGN KEY (kreditkarte_hk) REFERENCES biz.r_link_gp_cc(kreditkarte_hk);
ALTER TABLE biz.kreditkarte ADD CONSTRAINT kreditkarte_fk__r_link_cc_trans FOREIGN KEY (kreditkarte_hk) REFERENCES biz.r_link_cc_trans(kreditkarte_hk);
--CREATE TABLE biz.kreditkarte_hist (like biz_python.kreditkarte including all);



CREATE TABLE biz.time(
time_hk CHAR(32),
processingday DATE,
isbusinessday INTEGER,
isendofmonth INTEGER,
isendofquarter INTEGER,
isendofhalfyear INTEGER,
isendofyear INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime_create timestamp,
systemtime_modifiy timestamp,
record_source varchar(255),
diff_hk CHAR(32),
PRIMARY KEY(time_hk,processing_date_end,record_source)
);

--CREATE TABLE biz.time_hist (like biz.time including all);




CREATE TABLE biz.transaktion(
transaktions_id VARCHAR(20),
ausfuehrungsdatum DATE,
betrag decimal(20,10),
buchungsart VARCHAR(50),
loeschung INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime_create timestamp,
systemtime_modifiy timestamp,
record_source varchar(255),
diff_hk CHAR(32),
transaktion_hk CHAR(32)
,
PRIMARY KEY(transaktion_hk,processing_date_end,record_source)
);
ALTER TABLE biz.transaktion ADD CONSTRAINT transaktion_fk__r_link_cc_trans FOREIGN KEY (transaktion_hk) REFERENCES biz.r_link_cc_trans(transaktion_hk);
ALTER TABLE biz.transaktion ADD CONSTRAINT transaktion_fk__r_link_darlehen_trans FOREIGN KEY (transaktion_hk) REFERENCES biz.r_link_darlehen_trans(transaktion_hk);

--CREATE TABLE biz.transaktion_hist (like biz.transaktion including all);


