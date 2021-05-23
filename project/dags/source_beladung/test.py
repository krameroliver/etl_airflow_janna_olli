# import packages
from source_beladung.beladung import LoadSource



l = LoadSource(file_nm='../Daten/2018-12-31/acct.csv', table_nm='acct', separator=',', process_date='2018-12-31')
d = l.read_csv()
d = l.add_technical_col(d)
l.write_to_db(data=d)

#
# l = LoadSource(file_nm='../Daten/2018-12-31/card.csv', table_nm='card', separator=',', process_date='2018-12-31')
# d = l.read_csv()
# d = l.add_technical_col(d)
# l.write_to_db(data=d)
#
# l = LoadSource(file_nm='../Daten/2018-12-31/client.csv', table_nm='client', separator=',', process_date='2018-12-31')
# d = l.read_csv()
# d = l.add_technical_col(d)
# l.write_to_db(data=d)
#
#
# l = LoadSource(file_nm='../Daten/2018-12-31/disposition.csv', table_nm='disposition', separator=',', process_date='2018-12-31')
# d = l.read_csv()
# d = l.add_technical_col(d)
# l.write_to_db(data=d)
#
# l = LoadSource(file_nm='../Daten/2018-12-31/loan.csv', table_nm='loan', separator=',', process_date='2018-12-31')
# d = l.read_csv()
# d = l.add_technical_col(d)
# l.write_to_db(data=d)
#
# l = LoadSource(file_nm='../Daten/2018-12-31/order.csv', table_nm='order', separator=',', process_date='2018-12-31')
# d = l.read_csv()
# d = l.add_technical_col(d)
# l.write_to_db(data=d)
#
# l = LoadSource(file_nm='../Daten/2018-12-31/trans.csv', table_nm='trans', separator=';', process_date='2018-12-31')
# d = l.read_csv()
# d = l.add_technical_col(d)
# l.write_to_db(data=d)
#
#
#
