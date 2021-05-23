CREATE TABLE tech."trigger" (
	triggername varchar NOT NULL,
	trigger_id int8 NOT NULL,
	job_id varchar NOT NULL,
	job_name varchar NOT NULL,
	activitytime timestamptz(0) NOT NULL,
	processingdate date NOT NULL,
	activitydate date NOT NULL,
	CONSTRAINT trigger_pk PRIMARY KEY (trigger_id, job_id, activitytime, processingdate)
);