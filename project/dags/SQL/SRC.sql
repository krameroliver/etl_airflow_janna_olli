CREATE TABLE acct(
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
PRIMARY KEY(acct_hk,processing_date_end)
)WITH SYSTEM VERSIONING;



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
PRIMARY KEY(card_hk,processing_date_end)
)WITH SYSTEM VERSIONING;


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
PRIMARY KEY(client_hk,processing_date_end)
)WITH SYSTEM VERSIONING;





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
PRIMARY KEY(disposition_hk,processing_date_end)
)WITH SYSTEM VERSIONING;



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
PRIMARY KEY(loan_hk,processing_date_end)
)WITH SYSTEM VERSIONING;



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
PRIMARY KEY(order_hk,processing_date_end)
)WITH SYSTEM VERSIONING;

CREATE TABLE district(
district_id integer,
city varchar(255),
state_name  varchar(255),
state_abbrev  varchar(255),
region  varchar(255),
division varchar(255),
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
PRIMARY KEY(district_hk,processing_date_end)
)

CREATE TABLE trans(
run_id varchar(100),
trans_id VARCHAR(100 ),
account_id VARCHAR(100 ),
trans_type VARCHAR(100 ),
operation VARCHAR(100 ),
amount varchar(100),
balance VARCHAR(100 ),
k_symbol VARCHAR(100 ),
bank VARCHAR(200 ),
account varchar(100),
year varchar(100),
month varchar(100),
day varchar(100),
fulldate VARCHAR(100),
fulltime VARCHAR(100 ),
fulldatewithtime varchar(100),
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
PRIMARY KEY(trans_hk,processing_date_end)
)WITH SYSTEM VERSIONING;