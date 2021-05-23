CREATE TABLE bizSCHEMA_ID.r_s_link_gp_darlehen(
processing_date_start DATE,
processing_date_end   DATE,
systemtime tstzrange,
record_source varchar(255),
geschaeftspartner_hk CHAR(32)
,
PRIMARY KEY(geschaeftspartner_hk,systemtime)
);

CREATE TABLE bizSCHEMA_ID.r_s_link_gp_darlehen_hist (like bizSCHEMA_ID.r_s_link_gp_darlehen including all);

CREATE TRIGGER versioning_trigger_r_s_link_gp_darlehen BEFORE INSERT OR UPDATE OR DELETE ON bizSCHEMA_ID.r_s_link_gp_darlehen FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'bizSCHEMA_ID.r_s_link_gp_darlehen_hist', true);

