import urllib3
urllib3.disable_warnings()

# setting up the conf file, for the getting started purpose we set the right configuration file for the month
import os
os.environ["CONF_FILE"] = "conf.ini"

# init the library using the conf file
from kensu.utils.kensu_provider import KensuProvider
kensu = KensuProvider().initKensu()

# small data preparation example to demonstrate the results of using kensu-py
import kensu.pandas as pd 
customers_info = pd.read_csv('data/customers-data.csv')
contact_info = pd.read_csv('data/contact-data.csv')
business_info = pd.read_csv('data/business-data.csv')

customer360 = customers_info.merge(contact_info,on='id')

monthly_ds = pd.merge(customer360,business_info)

monthly_ds.to_csv('data/data.csv',index=False)
