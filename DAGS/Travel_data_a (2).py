
import pandas as pd
import numpy as np






def airflowfunc():

    file_path="visa_number_in_japan.csv"
    visa=pd.read_csv(file_path)
    visa.head(10)
    visa.info()
    visa=visa[visa['Country']!= 'total'].reset_index(drop=True)
    visa.head(10)
    if visa.duplicated().sum() == 0 :
        print('duplicates removed')
    else:
        print('need to remove duplicates')

    visa.isnull().sum().sort_values(ascending=False)
    visa.columns.tolist()
    visa.drop(labels=['Specified_short term', 'Employment_Steng (new)', 'Employment_Nursing care', 'Employment_Country'], axis=1, inplace=True)
    visa.columns.tolist()
    visa.head(10)
    ### now replace all the missing values with 0
    visa=visa[visa['Country']!= 'others'].reset_index(drop=True)
    visa['Country'].value_counts().head(40)
    visa.duplicated().sum()
    visa.isnull().sum()
    visa.fillna(0,inplace=True)

    visa.to_csv("s3://michelledata/visa.csv", index = False)
