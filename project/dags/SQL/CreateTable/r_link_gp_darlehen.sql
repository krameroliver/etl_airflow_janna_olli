CREATE TABLE bizSCHEMA_ID.r_link_gp_darlehen(
link_gp_darlehen_hk CHAR(32),
darlehen_hk CHAR(32),
geschaeftspartner_hk CHAR(32),
processing_date DATE,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,systemtime)

CREATE TABLE bizSCHEMA_ID.r_link_gp_darlehen_hist (like bizSCHEMA_ID.r_link_gp_darlehen including all);

CREATE TRIGGER versioning_trigger_r_link_gp_darlehen BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.r_link_gp_darlehen FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.r_link_gp_darlehen_hist', true);

