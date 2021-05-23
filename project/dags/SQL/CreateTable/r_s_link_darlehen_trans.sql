CREATE TABLE bizSCHEMA_ID.r_s_link_darlehen_trans(
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
transaktion_hk CHAR(32)
,
PRIMARY KEY(transaktion_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.r_s_link_darlehen_trans_hist (like bizSCHEMA_ID.r_s_link_darlehen_trans including all);

CREATE TRIGGER versioning_trigger_r_s_link_darlehen_trans BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.r_s_link_darlehen_trans FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.r_s_link_darlehen_trans_hist', true);

