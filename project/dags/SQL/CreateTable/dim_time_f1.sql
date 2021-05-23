CREATE TABLE rpt.dim_time_f1(
processingday DATE,
processing_date DATE,
record_source varchar(255),
time_hk CHAR(32)
);

CREATE TABLE rpt.dim_time_f1_hist (like rpt.dim_time_f1 including all);

CREATE TRIGGER versioning_trigger_dim_time_f1 BEFORE INSERT OR UPDATE OR DELETE ON rpt.dim_time_f1 FOR EACH ROW EXECUTE PROCEDURE versioning('effectiv_timerange', 'rpt.dim_time_f1_hist', true);

