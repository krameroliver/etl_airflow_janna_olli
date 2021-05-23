CREATE TABLE bizSCHEMA_ID.r_link_darlehen_trans(
link_darlehen_trans_hk CHAR(32),
darlehen_hk CHAR(32),
transaktion_hk CHAR(32),
processing_date DATE,
record_source varchar(255),
transaktion_hk CHAR(32)
,
PRIMARY KEY(transaktion_hk,systemtime)

CREATE TABLE bizSCHEMA_ID.r_link_darlehen_trans_hist (like bizSCHEMA_ID.r_link_darlehen_trans including all);

CREATE TRIGGER versioning_trigger_r_link_darlehen_trans BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.r_link_darlehen_trans FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.r_link_darlehen_trans_hist', true);

