import pandas as pd 
customers_info = pd.read_csv('data/customers-data.csv')
contact_info = pd.read_csv('data/contact-data.csv')
business_info = pd.read_csv('data/business-data.csv')

customer360 = customers_info.merge(contact_info,on='id')

monthly_ds = pd.merge(customer360,business_info)

monthly_ds.to_csv('data/data.csv',index=False)