USE src;



DELIMITER $$
$$
CREATE OR REPLACE TRIGGER `delete_acct`
BEFORE DELETE ON acct
FOR EACH ROW
BEGIN
	INSERT INTO acct_hist
	(account_id, district_id, frequency, parseddate, `year`, `month`, `day`, processing_date_start, processing_date_end, record_source, acct_hk, diff_hk, mod_flg)
	VALUES(old.account_id, old.district_id, old.frequency, old.`parseddate`, old.`year`, old.`month`, old.`day`, old.processing_date_start, current_date(),old.record_source,old.acct_hk,old.diff_hk,'D');
END$$

$$
CREATE OR REPLACE TRIGGER `delete_district`
BEFORE DELETE ON district
FOR EACH ROW
BEGIN
	INSERT INTO district_hist
	(district_id, city, state_name, state_abbrev, region, division, processing_date_start, processing_date_end, record_source, district_hk, diff_hk, mod_flg)
	VALUES(old.district_id, old.city, old.state_name, old.state_abbrev, old.region, old.division, old.processing_date_start, current_date(),old.record_source,old.district_hk,old.diff_hk,'D');
END$$





$$
CREATE TRIGGER `delete_card`
BEFORE DELETE ON card
FOR EACH ROW
BEGIN
	INSERT INTO card_hist
	(card_id, disp_id, card_type, `year`, `month`, `day`, fulldate, processing_date_start, processing_date_end, record_source, card_hk, diff_hk, mod_flg)
	VALUES(old.card_id, old.disp_id, old.card_type, old.`year`, old.`month`, old.`day`, old.fulldate, old.processing_date_start, current_date(),old.record_source,old.card_hk,old.diff_hk,'D');
END
$$



$$
CREATE TRIGGER `delete_client`
BEFORE DELETE ON client
FOR EACH ROW
BEGIN
	INSERT INTO client_hist
	(client_id, sex, fulldate, `day`, `month`, `year`, age, social, `first`, middle, `last`, phone, email, address_1, address_2, city, state, zipcode, district_id, processing_date_start, processing_date_end, record_source, client_hk, diff_hk, mod_flg)
VALUES(old.client_id, old.sex, old.fulldate, old.`day`, old.`month`, old.`year`, old.age, old.social, old.`first`, old.middle, old.`last`, old.phone, old.email, old.address_1, old.address_2, old.city, old.state, old.zipcode, old.district_id, old.processing_date_start, current_date(), old.record_source, old.client_hk, old.diff_hk, 'D');

END
$$


$$
CREATE TRIGGER `delete_disposition`
BEFORE DELETE ON disposition
FOR EACH ROW
BEGIN
	INSERT INTO disposition_hist
	(disp_id, client_id, account_id, user_type, processing_date_start, processing_date_end, record_source, disposition_hk, diff_hk, mod_flg)
VALUES(old.disp_id, old.client_id, old.account_id, old.user_type, old.processing_date_start, current_date(), old.record_source, old.disposition_hk, old.diff_hk, 'D');

END
$$


$$
CREATE TRIGGER `delete_loan`
BEFORE DELETE ON loan
FOR EACH ROW
BEGIN
	INSERT INTO loan_hist
	(loan_id, account_id, amount, duration, payments, status, `year`, `month`, `day`, fulldate, location, purpose, processing_date_start, processing_date_end, record_source, loan_hk, diff_hk, mod_flg)
VALUES(old.loan_id, old.account_id, old.amount, old.duration, old.payments, old.status, old.`year`, old.`month`, old.`day`, old.fulldate, old.location, old.purpose, old.processing_date_start, current_date(), old.record_source, old.loan_hk,old. diff_hk, 'D');

END
$$


$$
CREATE TRIGGER `delete_order`
BEFORE DELETE ON `order`
FOR EACH ROW
BEGIN
	INSERT INTO order_hist
	(order_id, account_id, bank_to, account_to, amount, k_symbol, processing_date_start, processing_date_end, record_source, order_hk, diff_hk, mod_flg)
VALUES(old.order_id, old.account_id, old.bank_to, old.account_to, old.amount, old.k_symbol, old.processing_date_start, current_date(), old.record_source, old.order_hk, old.diff_hk, 'D');

END
$$


$$
CREATE TRIGGER `delete_trans`
BEFORE DELETE ON trans
FOR EACH ROW
BEGIN
	INSERT INTO trans_hist
	(run_id, trans_id, account_id, trans_type, operation, amount, balance, k_symbol, bank, account, `year`, `month`, `day`, fulldate, fulltime, fulldatewithtime, processing_date_start, processing_date_end, record_source, trans_hk, diff_hk, mod_flg)
VALUES(old.run_id, old.trans_id, old.account_id, old.trans_type, old.operation, old.amount, old.balance, old.k_symbol, old.bank, old.account, old.`year`, old.`month`, old.`day`, old.fulldate, old.fulltime, old.fulldatewithtime, old.processing_date_start, current_date(), old.record_source, old.trans_hk, old.diff_hk, 'D');

END
$$

DELIMITER ;
