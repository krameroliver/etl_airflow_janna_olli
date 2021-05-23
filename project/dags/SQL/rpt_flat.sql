CREATE TABLE rpt_python.dim_darlehen_f1 (
	"index" int8 NULL,
	kontonummer text NULL,
	nominal float8 NULL,
	startdatum timestamp NULL,
	enddatum timestamp NULL,
	status int8 NULL,
	tilgung float8 NULL,
	verwendungszweck int8 NULL,
	futurecashflow int8 NULL,
	loeschung int8 NULL,
	record_source text NULL,
	darlehen_hk text NULL,
	processing_date text NULL
);


CREATE TABLE rpt_python.dim_geschaeftspartner_adresse_digital_f2 (
	kontakttyp int8 NULL,
	kontaktinfo text NULL,
	geschaeftspartner_hk text NULL,
	record_source text NULL,
	processing_date text NULL
);

 CREATE TABLE rpt_python.dim_geschaeftspartner_adresse_postalisch_f2 (
	addresse1 text NULL,
	addresse2 text NULL,
	stadt text NULL,
	bundesland text NULL,
	postleitzahl int8 NULL,
	geschaeftspartner_hk text NULL,
	record_source text NULL,
	processing_date text NULL
);



CREATE TABLE rpt_python.dim_geschaeftspartner_f1 (
	kundennummer varchar(18) NULL,
	anrede varchar(20) NULL,
	vorname varchar(24) NULL,
	nachname varchar(24) NULL,
	geburtsdatum date NULL,
	sterbedatum date NULL,
	geschlecht int4 NULL,
	sozialversicherungsnummer varchar(24) NULL,
	kreditkartenanzahl int4 NULL,
	loeschung int4 NULL,
	kontakttyp int4 NULL,
	record_source text NULL,
	darlehen_hk text NULL,
	processing_date text NULL,
	geschaeftspartner_hk bpchar(32) NULL
);


-- rpt_python.dim_kreditkarte_f1 definition

-- Drop table

-- DROP TABLE rpt_python.dim_kreditkarte_f1;

CREATE TABLE rpt_python.dim_kreditkarte_f1 (
	kartennummer varchar(20) NULL,
	beginndatum date NULL,
	kartentyp int4 NULL,
	loeschung int4 NULL,
	record_source text NULL,
	processing_date text NULL,
	kreditkarte_hk bpchar(32) NULL
);


-- rpt_python.dim_time_f1 definition

-- Drop table

-- DROP TABLE rpt_python.dim_time_f1;

CREATE TABLE rpt_python.dim_time_f1 (
	time_hk bpchar(32) NULL,
	processingday date NULL,
	isbusinessday int4 NULL,
	isendofmonth int4 NULL,
	isendofquarter int4 NULL,
	isendofhalfyear int4 NULL,
	isendofyear int4 NULL,
	record_source text NULL,
	processing_date text NULL
);


-- rpt_python.dim_transaktion_f1 definition

-- Drop table

-- DROP TABLE rpt_python.dim_transaktion_f1;

CREATE TABLE rpt_python.dim_transaktion_f1 (
	transaktions_id varchar(20) NULL,
	ausfuehrungsdatum date NULL,
	betrag numeric(20,10) NULL,
	buchungsart varchar(50) NULL,
	loeschung int4 NULL,
	record_source text NULL,
	processing_date text NULL,
	transaktion_hk bpchar(32) NULL
);


-- rpt_python.fact_darlehen_f0 definition

-- Drop table

-- DROP TABLE rpt_python.fact_darlehen_f0;

CREATE TABLE rpt_python.fact_darlehen_f0 (
	processing_date date NULL,
	fk_time bpchar(32) NULL,
	fk_darlehen bpchar(32) NULL,
	fk_geschaeftspartner bpchar(32) NULL,
	nominalbetrag numeric(20,10) NULL,
	tilgungsbetrag numeric(20,10) NULL,
	tilgungsanteil numeric(20,10) NULL,
	tilgungssumme_ausstehend numeric(20,10) NULL,
	record_source varchar NULL
);


-- rpt_python.fact_ergebnis_f0 definition

-- Drop table

-- DROP TABLE rpt_python.fact_ergebnis_f0;

CREATE TABLE rpt_python.fact_ergebnis_f0 (
	fk_time text NULL,
	fk_darlehen text NULL,
	fk_kreditkarte text NULL,
	fk_geschaeftspartner text NULL,
	fk_transaktion text NULL,
	betrag float8 NULL,
	kennzahl text NULL,
	record_source text NULL,
	processing_date text NULL
);


-- rpt_python.fact_transaktion_f0 definition

-- Drop table

-- DROP TABLE rpt_python.fact_transaktion_f0;

CREATE TABLE rpt_python.fact_transaktion_f0 (
	fk_darlehen text NULL,
	fk_kreditkarte text NULL,
	fk_geschaeftspartner text NULL,
	fk_transaktion text NULL,
	fk_time text NULL,
	transaktionsbetrag float8 NULL,
	record_source text NULL,
	processing_date text NULL
);
