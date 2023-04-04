# Store all of the necessary functions to automate your process from acquiring the data to returning a cleaned dataframe with no missing values in your wrangle.py file. Name your final function wrangle_zillow.


import env
import pandas as pd
from sklearn.model_selection import train_test_split



def aquire_zillow_data():
    import os 
    import env
   

    query='\
    select bedroomcnt, bathroomcnt, calculatedfinishedsquarefeet, taxvaluedollarcnt, yearbuilt, taxamount,fips\
    from properties_2017\
    left join propertylandusetype using(propertylandusetypeid)\
    where propertylandusedesc = "Single Family Residential"'
    
    if os.path.exists('zillow.csv') == True:
        return pd.read_csv('zillow.csv')
    else:
        df = pd.read_sql(query, get_connection())
        df.to_csv('zillow.csv')
        return pd.read_csv('zillow.csv')

    
def get_connection(user=env.user,password=env.password,host=env.host,database=env.database):
    return f'mysql+pymysql://{user}:{password}@{host}/{database}'   


def prepare(df):
    df = df.drop(columns='Unnamed: 0')
    df = df.dropna()
    cols_name = ['bedrooms', 'bathrooms', 'calculated_finished_squarefeet',
           'tax_valuedollar_cnt', 'year_built', 'tax_amount', 'fips']
    df.set_axis(cols_name, axis=1,inplace=True)
    remove_outliers(df)
    return df

def split_data(df):
    train, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train, test_size=.3, random_state=123)
    return train, validate, test

def quantile_scaler(train):
    q_scaler = QuantileTransformer(output_distribution='normal')
    q_scaler.fit(train)
    # q_scaler.fit_transform(train)
    train_scaled = q_scaler.transform(train)
    validate_scaled = q_scaler.transform(validate)
    test_scaled = q_scaler.transform(test)
    
    

def remove_outliers(df):
    
    col_list = list(df.columns)
    for col in col_list:

        q1, q3 = df[col].quantile([.25, .75])  # get quartiles

        iqr = q3 - q1   # calculate interquartile range

        upper_bound = q3 + 1.5 * iqr   # get upper bound
        lower_bound = q1 - 1.5 * iqr   # get lower bound

        # return dataframe without outliers

        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]

    return df