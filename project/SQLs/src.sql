CREATE or replace TABLE acct(
account_id VARCHAR(11),
district_id INTEGER,
frequency VARCHAR(100),
parseddate DATE,
year INTEGER,
month INTEGER,
day INTEGER,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
acct_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(acct_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE or replace TABLE acct_hist(
account_id VARCHAR(11),
district_id INTEGER,
frequency VARCHAR(100),
parseddate DATE,
year INTEGER,
month INTEGER,
day INTEGER,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
acct_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(acct_hk)
)WITH SYSTEM VERSIONING;
--delim
create or replace TABLE district(
district_id INTEGER,
city VARCHAR(255),
state_name VARCHAR(255),
state_abbrev CHAR(2),
region VARCHAR(255),
division VARCHAR(255),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
district_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(district_hk)
)WITH SYSTEM VERSIONING;
--delim
create or replace TABLE district_hist(
district_id INTEGER,
city VARCHAR(255),
state_name VARCHAR(255),
state_abbrev CHAR(2),
region VARCHAR(255),
division VARCHAR(255),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
district_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(district_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE card(
card_id VARCHAR(10),
disp_id VARCHAR(10),
card_type VARCHAR(100),
year INTEGER,
month INTEGER,
day INTEGER,
fulldate DATE,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
card_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(card_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE card_hist(
card_id VARCHAR(10),
disp_id VARCHAR(10),
card_type VARCHAR(100),
year INTEGER,
month INTEGER,
day INTEGER,
fulldate DATE,
processing_date_start DATE,
processing_date_end   DATE DEFAULT NOW(),
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
card_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(card_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE client(
client_id VARCHAR(10 ),
sex VARCHAR(6 ),
fulldate DATE,
`day` integer,
`month` integer,
`year` integer,
age integer,
social VARCHAR(15 ),
`first` VARCHAR(255 ),
middle VARCHAR(255 ),
`last` VARCHAR(255 ),
phone VARCHAR(100 ),
email VARCHAR(100 ),
address_1 VARCHAR(100 ),
address_2 VARCHAR(100 ),
city VARCHAR(100 ),
state VARCHAR(100 ),
zipcode VARCHAR(100 ),
district_id integer,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
client_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(client_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE client_hist(
client_id VARCHAR(10 ),
sex VARCHAR(6 ),
fulldate DATE,
`day` integer,
`month` integer,
`year` integer,
age integer,
social VARCHAR(15 ),
`first` VARCHAR(255 ),
middle VARCHAR(255 ),
`last` VARCHAR(255 ),
phone VARCHAR(100 ),
email VARCHAR(100 ),
address_1 VARCHAR(100 ),
address_2 VARCHAR(100 ),
city VARCHAR(100 ),
state VARCHAR(100 ),
zipcode VARCHAR(100 ),
district_id integer,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
client_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(client_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE disposition(
disp_id VARCHAR(10 ),
client_id VARCHAR(10 ),
account_id VARCHAR(10 ),
user_type VARCHAR(6 ),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
disposition_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(disposition_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE disposition_hist(
disp_id VARCHAR(10 ),
client_id VARCHAR(10 ),
account_id VARCHAR(10 ),
user_type VARCHAR(6 ),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
disposition_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(disposition_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE loan(
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
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
loan_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(loan_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE loan_hist(
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
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
loan_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(loan_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE `order`(
order_id VARCHAR(10 ),
account_id VARCHAR(10 ),
bank_to VARCHAR(2 ),
account_to integer,
amount NUMERIC(20 ,10),
k_symbol VARCHAR(100 ),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
order_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(order_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE `order_hist`(
order_id VARCHAR(10 ),
account_id VARCHAR(10 ),
bank_to VARCHAR(2 ),
account_to integer,
amount NUMERIC(20 ,10),
k_symbol VARCHAR(100 ),
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
order_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(order_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE trans(
run_id bigint,
trans_id text,
account_id text,
trans_type text,
operation text,
amount NUMERIC(20,10),
balance NUMERIC(20,10),
k_symbol text,
bank text,
account text,
year bigint,
month bigint,
day bigint,
fulldate DATE,
fulltime text,
fulldatewithtime TIMESTAMP(6),
`date` DATE,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
trans_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(trans_hk)
)WITH SYSTEM VERSIONING;
--delim
CREATE TABLE trans_hist(
run_id bigint,
trans_id text,
account_id text,
trans_type text,
operation text,
amount NUMERIC(20,10),
balance NUMERIC(20,10),
k_symbol text,
bank text,
account text,
year bigint,
month bigint,
day bigint,
fulldate DATE,
fulltime text,
fulldatewithtime TIMESTAMP(6),
`date` DATE,
processing_date_start DATE DEFAULT NOW(),
processing_date_end   DATE DEFAULT '2262-04-11',
createte_at TIMESTAMP(6) AS ROW START INVISIBLE,
modified_at TIMESTAMP(6) AS ROW END INVISIBLE,
record_source varchar(255),
trans_hk CHAR(32),
diff_hk CHAR(32),
mod_flg CHAR(1),
PERIOD FOR business_time(processing_date_start, processing_date_end),
PERIOD FOR system_time(createte_at, modified_at),
PRIMARY KEY(trans_hk)
)WITH SYSTEM VERSIONING;