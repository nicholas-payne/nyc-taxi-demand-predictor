import pandas as pd
from bs4 import BeautifulSoup
import requests
from tkinter.filedialog import askdirectory


def import_holidays():

    ''' 
    Scrapes all public holidays for New York from public-holidays.us and creates a dataframe with all the holidays. This will be used to label the taxi with holday status.
    '''
    
    years = range(2014,2015)

    headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
    }

    df_dates = pd.DataFrame()

    while True:        
        folder = askdirectory()
        
        print(f'Files will be downloaded to {folder}')
        
        approval = input('y/n? ')
        
        if approval == 'y':
            print('Commencing download')
            break

        else:
           print(f'{folder} is an invalid file path')

    for year in years:
        holiday_url = f'https://www.public-holidays.us/US_EN_{year}_New%20York'
        req = requests.get(holiday_url,headers)
        soup = BeautifulSoup(req.content,'html.parser')
        table = soup.find("table").prettify()

        table = soup.find_all('table')
    
        df_html = pd.read_html(str(table),index_col=0,parse_dates=[1])[0]
        df_dates = pd.concat([df_dates,df_html])

    csv_filepath = folder + '/' + f'holidays.csv'
    df_dates.to_csv(csv_filepath)
    
if __name__ == '__main__':
    import_holidays()
