CREATE TABLE biz.l_gp_cc(
gp_cc_hk CHAR(32),
kreditkarte_hk CHAR(32),
geschaeftspartner_hk CHAR(32),
PRIMARY KEY(link_gp_cc_hk)
);

CREATE TABLE biz.l_m_gp_cc(
gp_cc_hk CHAR(32),
kreditkarte_hk CHAR(32),
geschaeftspartner_hk CHAR(32),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(gp_cc_hk,processing_date_end)
)WITH SYSTEM VERSIONING;

---------------------------------



CREATE TABLE biz.l_cc_konto(
cc_konto_hk CHAR(32),
kreditkarte_hk CHAR(32),
konto_hk CHAR(32),
PRIMARY KEY(cc_trans_hk)
);

CREATE TABLE biz.l_m_cc_konto(
cc_konto_hk CHAR(32),
rolle INT,
kreditkarte_hk CHAR(32) INVISIBLE,
konto_hk CHAR(32) INVISIBLE,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(cc_konto_hk,processing_date_end)
)WITH SYSTEM VERSIONING;


----------------------------------------

CREATE TABLE biz.h_konto(
kontonummer VARCHAR(18),
konto_hk CHAR(32),
PRIMARY KEY(konto_hk));

CREATE TABLE biz.s_konto(
kontonummer VARCHAR(18),
frequenz INTEGER,
wertstellungstag DATE,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
geschaeftspartner_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(geschaeftspartner_hk,kontakttyp,processing_date_end)
)WITH SYSTEM VERSIONING;

----------------------------------------
CREATE TABLE biz.h_geschaeftspartner(
kundennummer VARCHAR(18),
geschaeftspartner_hk CHAR(32),
PRIMARY KEY(geschaeftspartner_hk));

CREATE TABLE biz.m_geschaeftspartner_digitale_addresse(
kundennummer VARCHAR(18),
kontakttyp INTEGER,
kontaktinfo VARCHAR(255),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
geschaeftspartner_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(geschaeftspartner_hk,kontakttyp,processing_date_end)
)WITH SYSTEM VERSIONING;

CREATE TABLE biz.s_geschaeftspartner_postalische_addresse(
kundennummer VARCHAR(18),
addresse1 VARCHAR(100),
addresse2 VARCHAR(100), 
stadt VARCHAR(100),
bundesland VARCHAR(100),
postleitzahl VARCHAR(100),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
geschaeftspartner_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(geschaeftspartner_hk,processing_date_end)
)WITH SYSTEM VERSIONING;


CREATE TABLE biz.s_geschaeftspartner(
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
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
geschaeftspartner_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(geschaeftspartner_hk,processing_date_end)
)WITH SYSTEM VERSIONING;



CREATE TABLE biz.s_darlehen(
kontonummer VARCHAR(10),
nominal decimal(10,2),
startdatum DATE,
enddatum DATE,
status INTEGER,
tilgung decimal(10,2),
verwendungszweck INTEGER,
futurecashflow INTEGER,
loeschung INTEGER,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
diff_hk CHAR(32),
darlehen_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(darlehen_hk,processing_date_end)
)WITH SYSTEM VERSIONING;


CREATE TABLE biz.h_darlehen(
kontonummer VARCHAR(10),
darlehen_hk CHAR(32),
PRIMARY KEY(darlehen_hk)
);

CREATE TABLE biz.s_kreditkarte(
kartennummer VARCHAR(20),
beginndatum DATE,
kartentyp INTEGER,
loeschung INTEGER,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
diff_hk CHAR(32),
kreditkarte_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(kreditkarte_hk,processing_date_end)
)WITH SYSTEM VERSIONING;


CREATE TABLE biz.h_kreditkarte(
kartennummer VARCHAR(10),
kreditkarte_hk CHAR(32),
PRIMARY KEY(kreditkarte_hk)
);


CREATE TABLE biz.s_transaktion(
transaktions_id VARCHAR(20),
ausfuehrungsdatum DATE,
betrag decimal(20,10),
buchungsart VARCHAR(50),
loeschung INTEGER,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
diff_hk CHAR(32),
transaktion_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(transaktion_hk,processing_date_end)
)WITH SYSTEM VERSIONING;


CREATE TABLE biz.h_transaktion(
transaktions_id VARCHAR(10),
transaktion_hk CHAR(32),
PRIMARY KEY(transaktion_hk)
);