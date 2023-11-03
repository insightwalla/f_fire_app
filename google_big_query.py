from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class GoogleBigQuery:
    def __init__(self):
        self.credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

    def connect(self):
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        return self.client
    
    def query(self, query = str, as_dataframe = True):
        with self.connect() as client:
            query_job = self.client.query(query)
            results = query_job.result()
            if as_dataframe:
                results = results.to_dataframe()
            return results

def lambda_for_duration(x):
    start_hour = x['Hour_Start_Check']
    start_minute = x['Minute_Start_Check']
    close_hour = x['Hour_Close_Check']
    close_minute = x['Minute_Close_Check']
    if close_hour < start_hour:
        close_hour = close_hour + 24
    duration = (close_hour - start_hour)*60 + (close_minute - start_minute)
    return duration

def lambda_for_table_number(x):
    try:
        table = int(x['TableDescription'])
    except:
        try:
            table = x['TableDescription'].split(' ')[1]
            table = int(table)
        except:
            table = None
    return table


class TransformationGoogleBigQuery:
    def __init__(self, df, plot = True):
        self.plot = plot
        self.df = df
        #self.df = self.transform()

    def cleaning(self):
        # take off 0 net sales
        df = self.df
        df = df[df['NetSales'] != 0]
        # take off 0 covers
        df = df[df['GuestCount'] != 0]
        self.df = df

    def transformation0(self):
        df = self.df
        # trasform the First Order Time to a datetime format, they represent the minutes after midnight divided by 60
        df['Hour_Start_Check'] = df['FirstOrderTime'] // 60
        df['Minute_Start_Check'] = df['FirstOrderTime'] % 60
        df['Hour_Close_Check'] = df['ActualCloseTime'] // 60
        df['Minute_Close_Check'] = df['ActualCloseTime'] % 60
        # if the close time is before the start time, add 24 hours to the close time
        
        df['Duration'] = df.apply(lambda_for_duration, axis=1)
        # keep only duration > 5 minutes and < 360 minutes
        df = df[(df['Duration'] > 5) & (df['Duration'] < 360)]

        df['Hour_Close_Check'] = df.apply(lambda x: x['Hour_Close_Check'] + 24 if x['Hour_Close_Check'] < x['Hour_Start_Check'] else x['Hour_Close_Check'], axis=1)

        # Zero-fill hour and minute columns
        #df['Hour_Start_Check'] = df['Hour_Start_Check'].astype(str).str.zfill(2)
        #df['Minute_Start_Check'] = df['Minute_Start_Check'].astype(str).str.zfill(2)
        #df['Hour_Close_Check'] = df['Hour_Close_Check'].astype(str).str.zfill(2)
        #df['Minute_Close_Check'] = df['Minute_Close_Check'].astype(str).str.zfill(2)
        # df.loc[:, 'Hour_Start_Check'] = df['Hour_Start_Check'].astype(str).str.zfill(2)  
        # df.loc[:, 'Minute_Start_Check'] = df['Minute_Start_Check'].astype(str).str.zfill(2)
        # df.loc[:, 'Hour_Close_Check'] = df['Hour_Close_Check'].astype(str).str.zfill(2)
        # df.loc[:, 'Minute_Close_Check'] = df['Minute_Close_Check'].astype(str).str.zfill(2)
        df.loc[:, 'Hour_Start_Check'] = df.loc[:, 'Hour_Start_Check'].astype(str).str.zfill(2)
        df.loc[:, 'Minute_Start_Check'] = df.loc[:, 'Minute_Start_Check'].astype(str).str.zfill(2)
        df.loc[:, 'Hour_Close_Check'] = df.loc[:, 'Hour_Close_Check'].astype(str).str.zfill(2)
        df.loc[:, 'Minute_Close_Check'] = df.loc[:, 'Minute_Close_Check'].astype(str).str.zfill(2)
        
        # modify hour start check if < 7 
        df.loc[df['Hour_Start_Check'].astype(int) < 4, 'Hour_Start_Check'] = (df['Hour_Start_Check'].astype(int) + 24).astype(str).str.zfill(2)
        self.df = df

    def transformation1(self):
        # get min hour and max hour
        df = self.df
        # transform to int
        df['Hour_Start_Check'] = df['Hour_Start_Check'].astype(int)
        df['Hour_Close_Check'] = df['Hour_Close_Check'].astype(int)
        df['Minute_Start_Check'] = df['Minute_Start_Check'].astype(int)
        df['Minute_Close_Check'] = df['Minute_Close_Check'].astype(int) 

        hours = range(df['Hour_Start_Check'].min(), df['Hour_Close_Check'].max()+1)
        
        # create a list of minutes
        minutes = range(0, 60, 15)
        # create a list of hours and minutes
        self.hours_minutes = [str(hour).zfill(2) + ':' + str(minute).zfill(2) for hour in hours for minute in minutes]

        df['Minute_Start_Check_rounded'] = df['Minute_Start_Check'].apply(lambda x: 
                                                                    0 if x < 10 else \
                                                                    15 if x < 20 else \
                                                                    30 if x <= 37 else \
                                                                    45 if 38 < x < 60 else \
                                                                    0)
        df['Minute_Close_Check_rounded'] = df['Minute_Close_Check'].apply(lambda x:
                                                                    0 if x < 10 else \
                                                                    15 if x < 20 else \
                                                                    30 if x <= 37 else \
                                                                    45 if 38 < x < 60 else \
                                                                    0)
        
        # now create all the columns with the hours and minutes

        df.loc[:, self.hours_minutes] = 0
        # now fill the columns with the hours and minutes
        for index, row in df.iterrows():
            guest_count = row['GuestCount']
            start_hour = str(row['Hour_Start_Check']).zfill(2)
            start_minute = str(row['Minute_Start_Check_rounded']).zfill(2)
            close_hour = str(row['Hour_Close_Check']).zfill(2)
            close_minute = str(row['Minute_Close_Check_rounded']).zfill(2)
            # if close hour is before start hour, add 24 hours to close hour
            if close_hour < start_hour:
                close_hour = str(int(close_hour) + 24).zfill(2)
            # now create the string
            start_hour_minute = start_hour + ':' + start_minute
            close_hour_minute = close_hour + ':' + close_minute
            # now fill the columns
            df.loc[index, start_hour_minute:close_hour_minute] = guest_count
        
        df['TableDescription'] = df.apply(lambda_for_table_number, axis=1)
        # filter out the None
        df = df[df['TableDescription'].notnull()]
        # get all columns that contains :
        columns_to_modify = [column for column in df.columns if ':' in column]
        # now take only the first item before : and if >= 24, subtract 24
        columns_right_name = [f"{str(column.split(':')[0])}:{column.split(':')[1]}"
                            if int(column.split(':')[0]) < 24 else
                            f"{str(int(column.split(':')[0]) - 24).zfill(2)}:{column.split(':')[1]}"
                            for column in columns_to_modify]
         
        # rename the columns
        df = df.rename(columns=dict(zip(columns_to_modify, columns_right_name)))
        self.hours_minutes = columns_right_name
        self.df = df

    def visualization(self):
        percentage_ = True
        df = self.df
        # for each of the days sum the covers
        unique_dates = df['DateOfBusiness'].unique()
        # sort the dates
        unique_dates = sorted(unique_dates)


        for date in unique_dates:
            # filter
            df_date = df[df['DateOfBusiness'] == date]
            # keep only hour columns and sum
            df_for_viz = df_date
            #expander = st.expander(f'View {date.strftime("%d/%m/%Y")} data')
            #expander.write(df_for_viz)
            df_date = df_date[self.hours_minutes].sum(axis=0)

            # 
            df_date_table = df[df['DateOfBusiness'] == date]
            number_of_unique_tables = df_date_table['TableDescription'].nunique()
            df_date_table = df_date_table.groupby('TableDescription')[self.hours_minutes].sum()
            df_date_table = df_date_table.applymap(lambda x: 1 if x > 0 else 0)
            df_date_table = pd.DataFrame(df_date_table.sum(axis=0))
            # transform to percentage of total tables knowing that we have n table
            if percentage_:
                df_date_table = df_date_table.apply(lambda x: x/number_of_unique_tables*100)
                df_date_table = df_date_table.round(2)

            fig = make_subplots(
                # add second yaxis
                specs=[[{"secondary_y": True}]],
            )
            fig.add_trace(go.Bar(x=df_date.index, y=df_date.values), secondary_y=False)
            fig.add_trace(go.Bar(x=df_date_table.index, y=df_date_table.values.flatten(), opacity=0.5), secondary_y=False)
            # the second trace is a percentage of the total tables
            # add names for the traces
            fig.data[0].name = 'Guest Inside the Restaurant'
            fig.data[1].name = 'Table occupancy %'
            # need to make a grouped bar with the second y axis
            fig.update_layout(barmode='group')

            fig.update_layout(
                title=f'{date.strftime("%A")} -{ date.strftime("%d/%m/%Y")}',
                xaxis_title="Time",
                yaxis_title="Guest count",
                legend_title="Legend Title",
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="RebeccaPurple"
                )
            )
            # set size very small
            fig.update_layout(
                autosize=False,
                width=800,
                height=300,
            )
            return fig

    def transform(self):
        self.cleaning()
        self.transformation0()
        self.transformation1()
        if self.plot == True:
            fig = self.visualization()
            return fig, self.df
        else:
            return self.df
    
if __name__ == "__main__":
    googleconnection = GoogleBigQuery(key_path = "key.json")
    query = 'SELECT * FROM `sql_server_on_rds.Dishoom_dbo_dpvHstCheckSummary` LIMIT 100'
    df = googleconnection.query(query = query, as_dataframe = True)
    st.write(df)
