CREATE TABLE RPT.darlehen(
prdocessing_date DATE,
fk_time CHAR(32),
fk_darlehen CHAR(32),
fk_geschaeftspartner CHAR(32),
nominalbetrag NUMERIC(20 ,10),
tilgungsbetrag NUMERIC(20 ,10),
tilgungsanteil NUMERIC(20 ,10),
tilgungssumme_ausstehend NUMERIC(20 ,10));
