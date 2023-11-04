'''
This are the helper functions that are used in the app:
- lambda_for_month    -> get the month from the date
- lambda_for_week     -> get the week from the date
- lambda_for_day_name -> get the day name from the date
- lambda_for_day_part -> get the day part from the time
- _get_day_part       -> get the day part from the hour

- rescoring           -> rescoring the values of the columns that are related to the ratings

- clean_food          -> clean the food column
- clean_drinks        -> clean the drinks column
- clean_label         -> clean the labels column

- save to database    -> save the dataframe to the database
'''
import base64
import random
random.seed(42)

import streamlit as st
import numpy as np
import pandas as pd


from parameters import *
empty= ['', 'nan', ' ', np.nan]
# Helper functions



def lambda_for_month(x: pd.Series):
   '''
   This function is used to get the month from the date,
   if the date is empty, it will return "Not Specified"

   It favors the Reservation: Date over the Date Submitted
   
   ---

   params:
      x: the row of the dataframe

   return:
      the month of the date

   '''
   if x['Reservation: Date'] in empty and x['Date Submitted'] not in empty:
       return str(pd.to_datetime(x['Date Submitted']).month)
   elif x['Reservation: Date'] not in empty and x['Date Submitted'] in empty:
         return str(pd.to_datetime(x['Reservation: Date']).month)
   elif x['Reservation: Date'] not in empty and x['Date Submitted'] not in empty:
         return str(pd.to_datetime(x['Reservation: Date']).month)
   else:
      return "Not Specified"

def lambda_for_week(x: pd.Series):
   '''
   This function is used to get the week from the date,
   if the date is empty, it will return "Not Specified"

   It favors the Reservation: Date over the Date Submitted

   ---

   params:
      x: the row of the dataframe

   return:
      the week of the date

   '''
   if x['Reservation: Date'] in empty and x['Date Submitted'] not in empty:
       return str(pd.to_datetime(x['Date Submitted']).week)
   elif x['Reservation: Date'] not in empty and x['Date Submitted'] in empty:
         return str(pd.to_datetime(x['Reservation: Date']).week)
   elif x['Reservation: Date'] not in empty and x['Date Submitted'] not in empty:
         return str(pd.to_datetime(x['Reservation: Date']).week)
   else:
      return 'Not Specified'
   
def lambda_for_day_name(x: pd.Series):
   '''
   This function is used to get the day name from the date,
   if the date is empty, it will return "Not Specified"

   It favors the Reservation: Date over the Date Submitted

   ---

   params:
      x: the row of the dataframe

   return:
      the day name of the date

   '''
   if x['Reservation: Date'] in empty and x['Date Submitted'] not in empty:
      return str(pd.to_datetime(x['Date Submitted']).day_name())
   elif x['Reservation: Date'] not in empty and x['Date Submitted'] in empty:
      return str(pd.to_datetime(x['Reservation: Date']).day_name())
   elif x['Reservation: Date'] not in empty and x['Date Submitted'] not in empty:
      return str(pd.to_datetime(x['Reservation: Date']).day_name())
   else:
      return 'Not Specified'   
   
def lambda_for_day_part(x: pd.Series):
   '''
   This function is used to get the day part from the time,
   if the time is empty, it will return "Not Specified"  

   ---

   params:
      x: the row of the dataframe

   return:
      the day part of the time (Breakfast, Lunch, Dinner, Late Night)

   '''
   if x['Reservation: Time'] == "":
      return ""
   # if time is not ""
   else:
      return _get_day_part(str(pd.to_datetime(x['Reservation: Time']).hour))

def _get_day_part(hour: str):
    '''
    This function takes the hour as a `str` and returns the day part

    ---
      Parameters:
         hour: str
            the hour of the day

      Returns:
         the day part (Breakfast, Lunch, Dinner, Late Night)
    '''
    if hour == 'Not Specified' or hour == '' or hour == 'nan':
       return 'Not Specified'
    elif 7 <= int(hour) < 12:
        time_part = 'Breakfast'
    elif 12 <= int(hour) < 16:
        time_part = 'Lunch'
    elif 16 <= int(hour) < 20:
        time_part = 'Dinner'
    else:
       time_part = 'Late Night'
    return time_part

def get_day_part(hour: str):
    '''
    This function takes the hour as a `str` and returns the day part

    ---
      Parameters:
         hour: str
            the hour of the day

      Returns:
         the day part (Breakfast, Lunch, Dinner, Late Night)
    '''
    if hour == 'Not Specified' or hour == '' or hour == 'nan':
       return 'Not Specified'
    elif 7 <= int(hour) < 12:
        time_part = 'Breakfast'
    elif 12 <= int(hour) < 16:
        time_part = 'Lunch'
    elif 16 <= int(hour) < 20:
        time_part = 'Dinner'
    else:
       time_part = 'Late Night'
    return time_part

def rescoring(df):
   '''
   This function takes the dataframe and rescoring the values of the columns that are related to the ratings

   ---
      Parameters:
         df: dataframe
            the dataframe that we want to apply the rescoring
   '''
   #st.write(df)
   value_map = {
               5: 10,
                 4: 8,
                   3: 5,
                     2: 1,
                       1: 1
               }
   columns_to_rescore = ['Feedback: Food Rating', 'Feedback: Drink Rating', 'Feedback: Service Rating', 'Feedback: Ambience Rating', 'Overall Rating']
   df.loc[:, columns_to_rescore] = df[columns_to_rescore].replace('', 0)   # now transform the values into flaot
   df.loc[:, columns_to_rescore] = df[columns_to_rescore].astype(float)  
   #df.loc[:, columns_to_rescore] = df[columns_to_rescore].replace(value_map)
   return df
   
def rescoring_empty(df, new = False):
   '''
   This function takes the dataframe and rescoring the values of the columns that are related to the ratings

   ---
      Parameters:
         df: dataframe
            the dataframe that we want to apply the rescoring

         new: bool
            if True, it means that the dataframe is the new one, so the columns are different

   '''
   #st.write(df)
   if not new:
      columns_to_rescore = ['Feedback: Food Rating', 'Feedback: Drink Rating', 'Feedback: Service Rating', 'Feedback: Ambience Rating', 'Overall Rating']
   else:
      columns_to_rescore = ['Feedback_Food_Rating', 'Feedback_Drink_Rating', 'Feedback_Service_Rating', 'Feedback_Ambience_Rating', 'Overall_Rating']
   
   value_map = {
               5: 10,
                 4: 8,
                   3: 5,
                     2: 1,
                       1: 1
               }
   df.loc[:, columns_to_rescore] = df[columns_to_rescore].replace('', 0)   # now transform the values into flaot
   df.loc[:, columns_to_rescore] = df[columns_to_rescore].astype(float)  
   df.loc[:, columns_to_rescore] = df[columns_to_rescore].replace(value_map)
   return df

def lambda_for_menu_item(x):
   menu_item_string = str(x['menu_item'])
   food_string = str(x['food'])

   # case 1: menu_item is not empty
   if menu_item_string:
      if not food_string:
            return menu_item_string
      else:
            menu_list = [m.strip() for m in menu_item_string.split(' - ') if m.strip()]
            food_list = [f.strip() for f in food_string.split(' - ') if f.strip()]

            if set(menu_list) == set(food_list):
               return menu_item_string

   # case 2: menu_item is empty
   if food_string:
      return food_string + ' - '

   # default case: both menu_item and food are empty
   return ''


   #       

def lambda_for_drink_item(x):
   drink_string = str(x['drink'])
   drink_item_string = str(x['drink_item'])

   if not drink_string:
      return drink_item_string
   elif not drink_item_string:
      return drink_string + ' - '
   
   drink_list = [d.strip() for d in drink_string.split(' - ') if d.strip()]
   drink_item_list = [d.strip() for d in drink_item_string.split(' - ') if d.strip()]

   if set(drink_list) != set(drink_item_list):
      return drink_string + ' - '
   else:
      return drink_item_string
   
# CLEANING FUNCTIONS
def clean_food(food: str):
   '''
   This function takes a string and returns a list of the food items that are contained in the string

   ---

   params:
      food: str

   return:
      food: list

   Example:

   >>> food = 'Pizza - Pasta - Salad'
   >>> clean_food(food)
   ['Pizza', 'Pasta', 'Salad']
   '''
   # divide the food at the space
   food = food.split('-')
   # take off spaces
   food = [f.strip() for f in food if f != '']
   # check if are contained in the list
   food = [f for f in food if f in menu_items_lookup]
   return food

def clean_drinks(drink: str):
   '''
   This function takes a string and returns a list of the drink items that are contained in the string
   
   ---

   params:
      drink: str

   return:
      drink: list

   Example:

   >>> drink = 'Coke - Sprite - Fanta'
   >>> clean_drinks(drink)
   ['Coke', 'Sprite', 'Fanta']

   '''

   # divide the food at the space
   drink = drink.split('-')
   # take off spaces
   drink = [f.strip() for f in drink if f != '']
   # check if are contained in the list
   drink = [f for f in drink if f in drink_items_lookup]
   return drink

def clean_label(labels_: str):
   #st.write('This is the labels_: {}'.format(labels_))
   # we need to transform this to an interpretable format list
   # split take off [ and ] and split at ,
   if labels_ != "" and labels_ != "nan":
      labels_ = labels_.split('-')
      # replace  ' with nothing
      labels_ = [l.replace("'", "") for l in labels_] 
      # strip of spaces
      labels_ = [l.strip() for l in labels_ if l != '']
      return labels_
   else:
      return []

# SAVING TO DATABASE
def save_to_db(df, cols, name = 'pages/reviews.db'):
   db = Database_Manager(name)
   db.delete_all()
   df_values = df[cols].values
   db.insert_multiple(df_values)

# FILTERING
def filter_only_food_related_reviews(df):
   '''
   1. Take off the positive reviews
   2. Take off the reviews that don't have food keywords
   3. Set the index to start from 1
   '''
   df = df[df['sentiment'] != 'POSITIVE']
   food_df = df[df['keywords'].str.contains('|'.join(food_keywords))| (df['menu_item'] != '') | (df['drink_item'] != '')]
   food_df.index = range(1, len(food_df) + 1)
   return food_df

def filter_only_ambience_related_reviews(df):
   '''
   1. Take off the positive reviews
   2. Take off the reviews that don't have ambience keywords
   3. Set the index to start from 1
   '''
   ambience_df = df[df['sentiment'] != 'POSITIVE']
   ambience_df = ambience_df[ambience_df['keywords'].str.contains('|'.join(key_words_related_to_ambience))]
   ambience_df.index = range(1, len(ambience_df) + 1)
   # get the negative with no keywords
   ambience_df_no_keywords = df[(df['sentiment'] != 'POSITIVE') & (df['keywords'] == '') & (df['menu_item'] == '') \
                                & (df['drink_item'] == '') & (df['details'] != '')]
   ambience_df = pd.concat([ambience_df, ambience_df_no_keywords])
   ambience_df.index = range(1, len(ambience_df) + 1)
   return ambience_df

def filter_only_service_related_reviews(df):
   '''
   1. Take off the positive reviews
   2. Take off the reviews that don't have service keywords
   3. Set the index to start from 1
   '''
   df = df[df['sentiment'] != 'POSITIVE']
   service_df = df[df['keywords'].str.contains('|'.join(service_keywords))]
   service_df.index  = range(1, len(service_df) + 1)
   return service_df

# Examining the data
def get_worst_reviews(df):
    # keep only negative reviews
    df = df[df['sentiment'] == 'NEGATIVE']
    df['num_labels'] = df['label'].apply(lambda x: len(x.split('-')))
    df['num_keywords'] = df['keywords'].apply(lambda x: len(x.split('-')))
    feature_scoring = ['overall_rating', 'food_rating', 'drink_rating', 'service_rating', 'ambience_rating']
    # to calculate the average rating take off the 0s
    for idx, row in df[feature_scoring].iterrows():
        l = [float(i) for i in row.tolist() if float(i) != 0]
        df.loc[idx, 'avg_rating'] = np.mean(l)

    # use the averagerating and subtract the max rating
    df['distance_from_good_results'] = df['avg_rating'].apply(lambda x: 10 - x)
    # create a new column with the multiplication of the avg rating and the number of labels and the number of keywords
    df['words_in_details'] = df['details'].apply(lambda x: len(x.split(' ')))
    df['ratio(words_in_details/num_keywords)'] = (df['words_in_details'] / df['num_keywords']) / 5

    # get suggested to friend
    df['suggested_to_friend'] = df['suggested_to_friend'].apply(lambda x: -3 if x == 'Yes' else 3 if x == 'No' else 1)
    df['bad_score'] = df['distance_from_good_results'] * df['num_labels'] * df['ratio(words_in_details/num_keywords)'] * df['suggested_to_friend']
    # sort the dataframe by the bad score
    df = df.sort_values(by='bad_score', ascending=False)
    return df

def get_best_reviews(df):
    # keep only negative reviews
    df = df[df['sentiment'] == 'POSITIVE']
    df['num_keywords'] = df['keywords'].apply(lambda x: len(x.split('-')))
    # take off no keywords
    df = df[df['num_keywords'] != 0]
    feature_scoring = ['overall_rating', 'food_rating', 'drink_rating', 'service_rating', 'ambience_rating']
    # to calculate the average rating take off the 0s
    for idx, row in df[feature_scoring].iterrows():
        l = sum([float(i) for i in row.tolist()])
        df.loc[idx, 'avg_rating'] = l

    # use the averagerating and subtract the max rating
    # create a new column with the multiplication of the avg rating and the number of labels and the number of keywords
    df['words_in_details'] = df['details'].apply(lambda x: len(x.split(' ')))
    df['ratio(words_in_details/num_keywords)'] = (df['words_in_details'] / df['num_keywords'])
    df['suggested_to_friend'] = df['suggested_to_friend'].apply(lambda x: 3 if x == 'Yes' else -3 if x == 'No' else 1)
    df['bad_score'] = (df['avg_rating'] * df['ratio(words_in_details/num_keywords)'] * df['confidence'].astype(float)  * df['words_in_details'])* df['suggested_to_friend']
    # sort the dataframe by the bad score
    df = df.sort_values(by='bad_score', ascending=False)
    return df

def download_csv(df):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="your_file.csv">Download CSV file</a>'
    return href


def process_direct_feedback(direct_feedback: list, df: pd.DataFrame):
   '''
   This function is used to process the direct feedback and add it to the dataframe

   ---

   params:
      direct_feedback : list of direct feedback files (it should contain only one file.xlsx)
      df: the dataframe that contains the data from the database to which we will add the direct feedback
   
   return:
      df_direct_feedback: the final dataframe with the direct feedback added
   '''
   st.write('Direct Feedback: There are emails as well')
   df_direct_feedback = pd.read_excel(direct_feedback[0])
   # change column names: CAFE == 'Reservation: Venue', 'DATE RECEIVED' == 'Date Submitted', 'FEEDBACK' == 'Feedback: Feedback', 'Source' == 'Platform'
   df_direct_feedback = df_direct_feedback.rename(columns={'CAFE': 'Reservation: Venue', 'DATE RECEIVED': 'Date Submitted', 'FEEDBACK': 'Details', 'Source': 'Platform'})
   # keep only row with "Details" not empty
   df_direct_feedback = df_direct_feedback[df_direct_feedback['Details'] != '' ]
   # add all the columns that are inside the other df
   columns_to_add = df.columns.tolist()
   for col in df_direct_feedback.columns.tolist():
      if col not in columns_to_add:
         columns_to_add.append(col)

   # add the columns that are not in the df_direct_feedback
   for col in columns_to_add:
      if col not in df_direct_feedback.columns.tolist():
         df_direct_feedback[col] = ["" for i in range(len(df_direct_feedback))]

   df_direct_feedback = df_direct_feedback[df.columns]
   # keep only the not empty Details 
   df_direct_feedback = df_direct_feedback[df_direct_feedback['Details'].astype(str) != 'nan']
   # set the same type as the df columns
   df_direct_feedback["Label: Dishoom"] = ["" for i in range(len(df_direct_feedback))]
   # transform the date into datetime

   df_direct_feedback["Date Submitted"] = df_direct_feedback["Date Submitted"].apply(lambda x: str(pd.to_datetime(x).date()))
      # get week, month and day name from the date if there is one
   df_direct_feedback["Week"] = df_direct_feedback.apply(lambda_for_week, axis=1)
   df_direct_feedback["Month"] = df_direct_feedback.apply(lambda_for_month, axis=1)
   df_direct_feedback["Day_Name"] = df_direct_feedback.apply(lambda_for_day_name, axis=1)
   df_direct_feedback["Day_Part"] = df_direct_feedback.apply(lambda_for_day_part, axis=1)
   # convert the datetime into date
   # add year
   df_direct_feedback["Year"] = df_direct_feedback["Date Submitted"].apply(lambda x: str(pd.to_datetime(x).year))
   # add the source
   df_direct_feedback["Source"] = "Direct Feedback"
   # add week year
   df_direct_feedback["Week_Year"] = df_direct_feedback["Week"] + "W" + df_direct_feedback["Year"]
   # add month year
   df_direct_feedback["Month_Year"] = df_direct_feedback["Month"] + "M" + df_direct_feedback["Year"]
   # set date for filter
   df_direct_feedback["date_for_filter"] = df_direct_feedback["Date Submitted"]
   # set day part 
   df_direct_feedback["Day_Part"] = df_direct_feedback["Day_Part"].apply(lambda x: get_day_part(x))
   # set the thumbs up and thumbs down 👍 👎 columns as False
   df_direct_feedback["👍"] = [False for i in range(len(df_direct_feedback))]
   df_direct_feedback["👎"] = [False for i in range(len(df_direct_feedback))]
   df = pd.concat([df, df_direct_feedback], axis=0)
   # add the new columns for the scoring
   df['New Overall Rating'] = 1
   df['New Food Rating'] = 1
   df['New Drink Rating'] = 1
   df['New Service Rating'] = 1
   df['New Ambience Rating'] = 1
   return df
      
def preprocess_single_df(df):
   columns_to_keep = [
   'Date Submitted',
   'Title',
   'Details',
   'Overall Rating',
   'Feedback: Food Rating',
   'Feedback: Drink Rating',
   'Feedback: Service Rating',
   'Feedback: Ambience Rating',
   'Feedback: Recommend to Friend',
   'Reservation: Date',
   'Reservation: Venue',
   'Reservation: Time',
   'Reservation: Overall Rating',
   'Reservation: Food Rating',
   'Reservation: Drinks Rating',
   'Reservation: Service Rating',
   'Reservation: Ambience Rating',
   'Reservation: Recommend to Friend',
   'Reservation: Feedback Notes',
   'Reservation: Updated Date',
   'Label: Dishoom',
   '👍',
   '👎',
   '💡',
   'Source',
   'Week',
   'Month',
   'Day_Name',
   'Day_Part',
   'Year',
   'Week_Year',
   'Month_Year',
   'date_for_filter',
   'Suggested to Friend',
   'New Overall Rating',
   'New Food Rating',
   'New Drink Rating',
   'New Service Rating',
   'New Ambience Rating'
   ]
   # 3. Prepare the dataframes: 
   # add Reservation: Venue when empty (name of the restaurant)
   venue = df["Reservation: Venue"].unique().tolist()
   venue = [v for v in venue if str(v) != 'nan'][0]
   venue = str(venue).replace("'", "")
   df["Reservation: Venue"] = venue
   # add all the columns that we are going to use
   df["Label: Dishoom"] = ["" for i in range(len(df))]
   df['👍'] = False 
   df['👎'] = False
   df['💡'] = False    
   df['Source'] = df['Platform']
   # ADD: Week, Month, Day_Name, Day_Part, Year, Week_Year, Month_Year, date_for_filter
   # there is this sign / and the opposite \ in the date, so we need to check for both
   df["Week"] = df.apply(lambda_for_week, axis=1)
   df["Month"] = df.apply(lambda_for_month, axis=1)
   df["Day_Name"] = df.apply(lambda_for_day_name, axis=1)
   df['Day_Part'] = df.apply(lambda_for_day_part, axis=1)
   df['Year'] = df.apply(lambda x: str(pd.to_datetime(x['Date Submitted']).year) if x['Reservation: Date'] in empty else str(pd.to_datetime(x['Reservation: Date']).year), axis=1)
   df['Week_Year'] = df.apply(lambda x: x['Week'] + 'W' + x['Year'], axis=1)
   df['Month_Year'] = df.apply(lambda x: x['Month'] + 'M' + x['Year'], axis=1)
   df['date_for_filter'] = df.apply(lambda x: str(pd.to_datetime(x['Date Submitted']).date()) if x['Reservation: Date'] in empty else str(pd.to_datetime(x['Reservation: Date']).date()), axis=1)
   df['Suggested to Friend'] = df['Feedback: Recommend to Friend'].apply(lambda x: x if x == 'Yes' or x == 'No' else 'Not Specified')
   # initialize the new scoring columns
   df['New Overall Rating'] = 1
   df['New Food Rating'] = 1
   df['New Drink Rating'] = 1
   df['New Service Rating'] = 1
   df['New Ambience Rating'] = 1
   # set all scores to 0
   df = df[columns_to_keep]
   return df
      
from google_big_query import GoogleBigQuery, TransformationGoogleBigQuery

def get_sales_date(store_id, date, time = None):
   try:
      googleconnection = GoogleBigQuery()

      query_for_only_a_date = f'''
      SELECT *,
         EXTRACT(MONTH FROM DateOfBusiness) AS Month
         FROM `sql_server_on_rds.Dishoom_dbo_dpvHstCheckSummary`
         WHERE DateOfBusiness = '{date}'
               AND FKStoreID IN ({','.join([str(i) for i in store_id])})
      '''
      df = googleconnection.query(query = query_for_only_a_date, as_dataframe = True)
      fig, df = TransformationGoogleBigQuery(df, plot = True).transform()
      # add vertical line on time
      if time is not None and time!= 'nan':
         fig.add_vline(x=time, line_width=10, line_color="red", opacity=0.3)
      st.plotly_chart(fig, use_container_width = True)
   except:
      st.info('We cant display the data')