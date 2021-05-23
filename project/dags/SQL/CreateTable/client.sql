CREATE TABLE SRCSCHEMA_ID.client(
client_id VARCHAR(10),
sex VARCHAR(6),
fulldate DATE,
day INTEGER,
month INTEGER,
year INTEGER,
age INTEGER,
social VARCHAR(15),
first VARCHAR(255),
middle VARCHAR(255),
last VARCHAR(255),
phone VARCHAR(100),
email VARCHAR(100),
address_1 VARCHAR(100),
address_2 VARCHAR(100),
city VARCHAR(100),
state VARCHAR(100),
zipcode VARCHAR(100),
district_id INTEGER,
client_hk CHAR(32),
processing_valid_from DATE,
processing_valid_to DATE,
systemtime_start timestamp,
systemtime_end timestamp,
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
client_hk CHAR(32)
,
PRIMARY KEY(client_hk,systemtime)
);

CREATE TABLE SRCSCHEMA_ID.client_hist (like SRCSCHEMA_ID.client including all);

CREATE TRIGGER versioning_trigger_client BEFORE INSERT OR UPDATE OR DELETE ON SRCSCHEMA_ID.client FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'SRCSCHEMA_ID.client_hist', true);

