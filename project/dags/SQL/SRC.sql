CREATE TABLE src.acct(
account_id VARCHAR(11),
district_id INTEGER,
frequency VARCHAR(100),
parseddate DATE,
year INTEGER,
month INTEGER,
day INTEGER,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
acct_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PRIMARY KEY(acct_hk, processing_date_end)
);
commit;

CREATE TABLE src.acct_hist (like src.acct including all);
CREATE TRIGGER versioning_trigger_acct BEFORE INSERT OR UPDATE OR DELETE ON src.acct FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'src.acct_hist', true);
--###################################################################



CREATE TABLE src.card(
card_id VARCHAR(10),
disp_id VARCHAR(10),
card_type VARCHAR(100),
year INTEGER,
month INTEGER,
day INTEGER,
fulldate DATE,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
card_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1)
,
PRIMARY KEY(card_hk, processing_date_end)
);
commit;

CREATE TABLE src.card_hist (like src.card including all);

CREATE TRIGGER versioning_trigger_card BEFORE INSERT OR UPDATE OR DELETE ON src.card FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'src.card_hist', true);



--#################################################
CREATE TABLE src.client(
client_id VARCHAR(10 ),
sex VARCHAR(6 ),
fulldate DATE,
day integer,
month integer,
year integer,
age integer,
social VARCHAR(15 ),
first VARCHAR(255 ),
middle VARCHAR(255 ),
last VARCHAR(255 ),
phone VARCHAR(100 ),
email VARCHAR(100 ),
address_1 VARCHAR(100 ),
address_2 VARCHAR(100 ),
city VARCHAR(100 ),
state VARCHAR(100 ),
zipcode VARCHAR(100 ),
district_id integer,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
client_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PRIMARY KEY(client_hk, processing_date_end)
);
COMMIT;
CREATE TABLE src.client_hist (like src.client including all);
CREATE TRIGGER versioning_trigger_client BEFORE INSERT OR UPDATE OR DELETE ON src.client FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'src.client_hist', true);

COMMIT;


--############################################


CREATE TABLE src.disposition(
disp_id VARCHAR(10 ),
client_id VARCHAR(10 ),
account_id VARCHAR(10 ),
user_type VARCHAR(6 ),
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
disposition_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PRIMARY KEY(disposition_hk, processing_date_end)
);
COMMIT;
CREATE TABLE src.disposition_hist (like src.disposition including all);
CREATE TRIGGER versioning_trigger_disposition BEFORE INSERT OR UPDATE OR DELETE ON src.disposition FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'src.disposition_hist', true);
COMMIT;


--############################################

CREATE TABLE src.loan(
loan_id VARCHAR(10 ),
account_id VARCHAR(10 ),
amount integer,
duration integer,
payments integer,
status VARCHAR(1 ),
year integer,
month integer,
day integer,
fulldate DATE,
location integer,
purpose VARCHAR(255 ),
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
loan_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PRIMARY KEY(loan_hk, processing_date_end)
);
COMMIT;
CREATE TABLE src.loan_hist (like src.loan including all);
CREATE TRIGGER versioning_trigger_loan BEFORE INSERT OR UPDATE OR DELETE ON src.loan FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'src.loan_hist', true);
COMMIT;


--###################################################

CREATE TABLE src.order(
order_id VARCHAR(10 ),
account_id VARCHAR(10 ),
bank_to VARCHAR(2 ),
account_to integer,
amount NUMERIC(20 ,10),
k_symbol VARCHAR(100 ),
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
order_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PRIMARY KEY(order_hk, processing_date_end)
);
COMMIT;
CREATE TABLE src.order_hist (like src.order including all);
CREATE TRIGGER versioning_trigger_order BEFORE INSERT OR UPDATE OR DELETE ON src.order FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'src.order_hist', true);
COMMIT;

--###################################################
CREATE TABLE src.trans(
run_id integer,
trans_id VARCHAR(100 ),
account_id VARCHAR(100 ),
trans_type VARCHAR(100 ),
operation VARCHAR(100 ),
amount varchar(100),
balance VARCHAR(100 ),
k_symbol VARCHAR(100 ),
bank VARCHAR(200 ),
account varchar(100),
year integer,
month integer,
day integer,
fulldate VARCHAR(100),
fulltime VARCHAR(100 ),
fulldatewithtime varchar(100),
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
trans_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PRIMARY KEY(trans_hk, processing_date_end)
);
COMMIT;
CREATE TABLE src.trans_hist (like src.trans including all);
CREATE TRIGGER versioning_trigger_trans BEFORE INSERT OR UPDATE OR DELETE ON src.trans FOR EACH ROW EXECUTE PROCEDURE versioning('systemtime', 'src.trans_hist', true);
COMMIT;

