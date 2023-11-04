'''
author: Roberto Scalas 
date:   2023-10-17 09:37:39.647582
'''

import streamlit as st
st.set_page_config(layout="wide", page_title='Feedback Helper', page_icon='🤖')
import streamlit_antd_components as sac
import pandas as pd
from utils import *
from parameters import *
from graphs import *
from ai_classifier import ArtificialWalla
from translator_walla import Translator
from parameters import options_for_classification, menu_items_lookup, drink_items_lookup

#https://firebase.google.com/docs/firestore/manage-data/add-data

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

def _preprocessing(data):
      '''
      Here we will do the cleaning of the data
      
      - Just filling na with empty string
      ---
      Parameters:
      
         data: pandas dataframe

      Returns:
         data: pandas dataframe
      ---
      '''
      data = data.fillna('nan')
      return data

def _classifing(data):
      '''
      Here we will do the classification of the data
      - Sentiment
      - Confidence
      - Menu Item
      - Keywords
      - Drink Item
      '''
      walla = ArtificialWalla()
      for index, row in data.iterrows():
         sentiment, confidence, menu_items, keywords_, drinks_items = walla.classify_review(review = row['Details'])
         columns_for_rating = ['Overall Rating','Feedback: Food Rating', 'Feedback: Drink Rating','Feedback: Service Rating', 'Feedback: Ambience Rating']
         values = [row['Overall Rating'], row['Feedback: Food Rating'], row['Feedback: Drink Rating'], row['Feedback: Service Rating'], row['Feedback: Ambience Rating']]
         # replace 5.0 with 5
         #as strings
         values = [str(v) for v in values]
         # replace 5.0 with 5
         values = [v.replace('.0', '') for v in values]
         # if all 5 or 0, then the sentiment is positive
         not_positive_values = ['1', '2', '3', '4']
         if all(v not in not_positive_values for v in values):
            sentiment = 'POSITIVE'
            confidence = 1
         else:
            sentiment = 'NEGATIVE'
            confidence = 1         
         data.loc[index, 'Sentiment'] = sentiment
         data.loc[index, 'Confidence'] = confidence
         data.loc[index, 'Menu Item'] = ' '.join(menu_items)
         data.loc[index, 'Keywords'] = ' '.join(keywords_)
         data.loc[index, 'Drink Item'] = ' '.join(drinks_items)

      return data
   
def process_data(df):
         '''
         Here we run the actual transformation of the data
         '''
         df = _preprocessing(df)
         df = _classifing(df)
         df = rescoring(df)
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
   # rename all the columns taking off columns spacesand using _ instead
   return df
      
class FeedBackHelper:
   def __init__(self, name_user):
      '''
      Connect to the database
      '''
      json_key = dict(st.secrets["firebase"])
      cred = credentials.Certificate(json_key)
      try:
         firebase_admin.initialize_app(cred)
      except:
         pass
      self.db = firestore.client()
      self.df = None
      #st.success('Connected to Firestore')
   
   def get_review_by_venue_and_idx(self, venue, idx, give_doc = False):
      notes_ref = self.db.collection(u'feedback').document(venue).collection(u'reviews')
      query = notes_ref.where('idx', '==', str(idx))
      results = query.get()
      if give_doc:
         return results
      else:
         return [result.to_dict() for result in results][0]

   def read(self, show = True):
      '''
      1. Read the data from the database
      2. Create a dataframe
      3. Sort the dataframe by idx
      4. Create a container for each sentiment
      5. Create a delete button
      '''
      data_list = []
      data = self.db.collection('feedback').stream()

      for doc in data:
         reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
         for review in reviews:
            data_list.append(review.to_dict())

      df = pd.DataFrame(data_list)
      if len(df) == 0:  
         st.info('No data found - Please select Upload to upload the data')
         st.stop()
      df = df.sort_values(by=['idx'])
      if show:
         st.write(df)

      all_venues = df['Reservation_Venue'].unique().tolist()

      self.all_data = df
      df_empty = df[df['Details'] == 'nan']
      df = df[df['Details'] != 'nan']
      df_empty = rescoring_empty(df_empty, new=True)

      # adds search bar
      search = st.sidebar.text_input('Search', key='search')
      if search != '':
         df = df[df['Details'].str.contains(search, case=False)]
         df_empty = pd.DataFrame()
         if len(df) == 0:
            st.info('No reviews Found')
            st.stop()

      with st.sidebar.expander('Filters'):
         # filter by sentiment
         sentiment = st.selectbox('Choose the sentiment', ['All', 'POSITIVE', 'NEGATIVE'], key='sentiment', index=0)
         if sentiment != 'All':
            df = df[df['Sentiment'] == sentiment]

         # add toggle for searching only the ones negative with empty label
         only_negative_empty = st.toggle('Only Negative Empty', key='only_negative_empty')
         if only_negative_empty:
            df = df[(df['Sentiment'] == 'NEGATIVE') & (df['Label_Dishoom'] == '')]

         # now filter by thumbs up and thumbs down
         thumbs_up = st.toggle('Show Thumbs Up', key='thumbs_up_filter')
         thumbs_down = st.toggle('Show Thumbs Down', key='thumbs_down_filter')
         suggestions = st.toggle('Show Suggestions', key='suggestions_filter')


         if thumbs_up:
            df = df[df['👍'] == '1']
         if thumbs_down:
            df = df[df['👎'] == '1']
         if suggestions:
            df = df[df['💡'] == '1']

         if len(df) == 0:
            st.info('No reviews found!')
            st.stop()

      if len(df_empty) > 0:
         df_empty = rescoring_empty(df_empty, new=True)
         create_container_for_each_sentiment(df, df_empty)
      self.plot(df)


      def OnDeleteVenueRevs(name):
            # check if the doc exists
            doc_ref = self.db.collection(u'feedback').document(name)
            doc = doc_ref.get()
            if doc.exists:
               # delete the collection
               reviews = self.db.collection(u'feedback').document(name).collection(u'reviews').stream()
               for i, review in enumerate(reviews):
                  self.db.collection(u'feedback').document(name).collection(u'reviews').document(review.id).delete()
               st.write('Deleted Data for ', name)

      delete_all_data = st.sidebar.button('Delete All Data', type = 'primary', use_container_width=True)
      all_venues = df['Reservation_Venue'].unique().tolist()
      
      if delete_all_data:
         with st.spinner('Removing All Data'):
            for venue in all_venues:
               OnDeleteVenueRevs(venue)
         st.success('All data deleted successfully')
         st.stop()

      # create a delete button for the selected venue
      c1,c2 = st.columns(2)
      res_dict = {
         'all_venues' : all_venues,
         'data' : df
      }
      return res_dict

   def upload_excels(self):
      
      # 1. Upload the excel
      uploaded_files = st.file_uploader("Upload Excel", type="xlsx", accept_multiple_files=True, key='upload')

      if uploaded_files != []:
         # read the data
         upload_space = st.empty()
         tabs = st.tabs([str(u.name) for u in uploaded_files])
         for i, tab in enumerate(tabs):
            with tab:
               df = pd.read_excel(uploaded_files[i])
               st.write(df)
      else:
         st.stop()
      # 2. Check if the file is not empty
      def handle_upload():
         if uploaded_files != []:
            # 3. Create a progress bar 
            how_many = len(uploaded_files)
            if how_many != 1:
               my_big_bar = st.progress(0, text=f'Uploading 0/{how_many}')
            # 4. Loop through the files
            
            for i, file in enumerate(uploaded_files):
               # read the file
               df = pd.read_excel(file)

               names = df['Reservation: Venue'].unique().tolist()
               # take off nan
               names = [name for name in names if str(name) != 'nan']
               name = names[0]  
               df = preprocess_single_df(df)
               df['idx'] = [i for i in range(len(df))]
               df = process_data(df)
               df = df.rename(columns=lambda x: x.replace(':', '').replace('(', '').replace(')', '').replace(' ', '_'))

               # transform all in strings
               for col in df.columns.tolist():
                  df[col] = df[col].astype(str)

               # check if the doc exists
               doc_ref = self.db.collection(u'feedback').document(name)
               doc = doc_ref.get()
               my_bar = st.progress(0, text='Uploading data')

               if doc.exists:
                  #st.write('Document already exists')
                  # delete the collection
                  reviews = self.db.collection(u'feedback').document(name).collection(u'reviews').stream()
                  for i, review in enumerate(reviews):
                     self.db.collection(u'feedback').document(name).collection(u'reviews').document(review.id).delete()
                  # upload the data
                  for index, row in df.iterrows():
                     self.db.collection(u'feedback').document(name).collection(u'reviews').add(row.to_dict())
                     my_bar.progress(int((index+1) * 100/len(df)), text=f'Uploading Review {index+1}/{len(df)}')
               else:
                  # empty the doc
                  doc_ref.set({})
                  # upload the data
                  for index, row in df.iterrows():
                     self.db.collection(u'feedback').document(name).collection(u'reviews').add(row.to_dict())
                     # update the bar
                     my_bar.progress(int((index+1) * 100/len(df)), text=f'Uploading Review {index+1}/{len(df)}')
               if how_many != 1:
                  my_big_bar.progress(int((i+1) * 100/how_many), text=f'Uploading {i+1}/{how_many}')
         with st.balloons():
            st.info('All Done - You can go at the scoring section now! 😊')
      upload = upload_space.button('Upload', type='primary', use_container_width=True, on_click=handle_upload)
   
   def edit(self):
      res = self.read(show= False)
      

      # 1. Read the data from the database

      self.df = res['data']
      df = self.df
      # 2. Create the selectbox for the venue
      all_venues = res['all_venues']
      c1,c2 = st.columns(2)
      venue = c1.selectbox('Choose the venue', all_venues, key='venue', index = all_venues.index(st.session_state.venue) if st.session_state else 0)

      # 3. Create the delete button
      def OnDeleteVenueRevs(name):
            # check if the doc exists
            doc_ref = self.db.collection(u'feedback').document(name)
            doc = doc_ref.get()
            if doc.exists:
               st.write('Document already exists')
               # delete the collection
               reviews = self.db.collection(u'feedback').document(name).collection(u'reviews').stream()
               for i, review in enumerate(reviews):
                  self.db.collection(u'feedback').document(name).collection(u'reviews').document(review.id).delete()
               # now delete the doc
               self.db.collection(u'feedback').document(name).delete()
               st.write('Deleted Data for ', name)

      if st.sidebar.button(f'Delete **{venue}**', type = 'primary', use_container_width=True):
         OnDeleteVenueRevs(venue)
      with st.sidebar.expander('Ratings Info'):
               st.write(f'**5** -> **10**')
               st.write(f'**4** -> **8**')
               st.write(f'**3** -> **5**')
               st.write(f'**2** -> **1**')
               st.write(f'**1** -> **1**')

      # 4. Prepare the dataframes
      df = df[df['Reservation_Venue'] == venue] # filter by venue
      
      # take off empty detail
      df['Details'] = df['Details'].apply(lambda x: x.strip())
      df_full = df[df['Details'] != 'nan']
      list_of_index_full = df_full['idx'].unique().tolist()

      # Now we have a dataframe that look like this
      # | idx  | Details | 👍 | 👎  | 💡 |
      # |  10  |  ...    | 1  | 0  | 0  |
      # |  12  |  ...    | 0  | 1  | 0  |
      # |  81  |  ...    | 0  | 0  | 1  |
      # |  91  |  ...    | 1  | 0  | 0  |

      # We want to allow the user to select the index of the reviews that he wants to edit
      # but we need to map the indexes to allow the user to select from a range(1, len(df_full) 
      # instead of a list of indexes that are not consecutive, so we create a dictionary to map
      # the indexes from the fake ones to the real ones 
      # so when we select 1, we will get the index 10 -> # | idx  | Details | 👍 | 👎  | 💡 |
      #                                                    |  10  |  ...    | 1  | 0  | 0  |
      # and when we select 2, we will get the index 12 -> # | idx  | Details | 👍 | 👎  | 💡 |
      #                                                    |  12  |  ...    | 0  | 1  | 0  | 

      from_real_to_fake = {i+1 : index for i, index in enumerate(list_of_index_full)}

      # 5
      # We want to avoid loosing the index when we filter the dataframe so we save the index in the session state
      if 'index' not in st.session_state:
         st.session_state.index = 1
      if 'last_index' not in st.session_state:
         st.session_state.last_index = 1
      
      def on_change_n():
         st.session_state.last_index = st.session_state.index

      index = c2.number_input('Choose the index', min_value=1, max_value=len(df_full), value = st.session_state.last_index, on_change=on_change_n)

      edit_tab, venue_tab = st.tabs(['Edit', 'Venue Capacity'])
      
      with edit_tab:
         with st.form('scoring'):
            col_buttons = st.columns(5)
            c1_button = col_buttons[4]
            c2_button = col_buttons[0]


            #st.write(index, from_real_to_fake[index])
            #st.write(from_real_to_fake)
            #st.write('Fake index: ', index, 'Real index: ', from_real_to_fake[index])
            
            with st.spinner('Loading review...'):
               st.write(index)
               st.write(from_real_to_fake[index])
               st.write(len(df_full))
               doc = self.get_review_by_venue_and_idx(venue, from_real_to_fake[index], give_doc=True)
            try:
               review = doc[0].to_dict()
            except:
               st.rerun()

            st.markdown(review['Details'])
            #st.stop()
            #st.stop()
            # get the values
            # overall_rating = c1.number_input(f'Overall Rating: {review["Overall_Rating"]}', min_value=1, max_value=5, value=int(review['New_Overall_Rating']), key = 'overall' + str(index))
            # food_rating = c2.number_input(f'Food Rating: {review["Feedback_Food_Rating"]}', min_value=1, max_value=5, value=int(review['New_Food_Rating']), key = 'food' + str(index))
            # drink_rating = c3.number_input(f'Drink Rating: {review["Feedback_Drink_Rating"]}', min_value=1, max_value=5, value=int(review['New_Drink_Rating']), key = 'drink' + str(index))
            # service_rating = c4.number_input(f'Service Rating: {review["Feedback_Service_Rating"]}', min_value=1, max_value=5, value=int(review['New_Service_Rating']), key = 'service' + str(index))
            # ambience_rating = c5.number_input(f'Ambience Rating: {review["Feedback_Ambience_Rating"]}', min_value=1, max_value=5, value=int(review['New_Ambience_Rating']), key = 'ambience' + str(index))
            # update dishoom label

            # same with sac.rate


            value_map = {
                        5: 10,
                        4: 8,
                           3: 7,
                              2: 5,
                              1: 1,
                              0:'nan'
                        }

            nans_map = ['nan', 0, '0', '']
            with st.spinner('Creating Cards...'):
               col_best,col_worst,col_sugg = st.columns(3)
               col_food,col_drinks = st.columns(2)
               c_ov_r,c_fo_r,c_dr_r,c_se_r,c_am_r = st.columns(5)

               # get all the informations
               best_rev = df_full[df_full['👍'] == '1']
               worst_rev = df_full[df_full['👎'] == '1']
               suggestions_rev = df_full[df_full['💡'] == '1']
               is_this_best = review['👍'] == '1'
               is_this_worst = review['👎'] == '1'
               is_this_suggestion = review['💡'] == '1'

               if len(best_rev) == 3 and not is_this_best:
                  is_best = False
                  col_best.info('Already selected 3 👍')
               else:
                  is_best = col_best.toggle('👍',review['👍'] == '1', key = f'is_good{from_real_to_fake[index]}={venue}')

               if len(worst_rev) == 3 and not is_this_worst:
                  is_worst = False
                  col_worst.info('Already selected 3 👎')
               else:
                  is_worst = col_worst.toggle('👎',review['👎'] == '1', key= f'is_bad{from_real_to_fake[index]}={venue}')

               if len(suggestions_rev) == 3 and not is_this_suggestion:
                  is_suggestion = False
                  col_sugg.write('Already selected 3 💡')
               else:
                  is_suggestion = col_sugg.toggle('💡',review['💡'] == '1', key= f'is_suggestion{from_real_to_fake[index]}={venue}')

               def clean_column_entries(review, col_name):
                  '''
                  This function will clean the column entries
                  example:
                  'Menu_Item': 'Chicken Ruby - House Black Daal - Dishoom Calamaris'

                  will become:
                  ['Chicken Ruby', 'House Black Daal', 'Dishoom Calamari']
                  '''
                  col_values = review[col_name]
                  col_values = col_values.split('-') if '-' in col_values else [col_values]
                  col_values = [l.strip() for l in col_values if l != '']
                  return col_values

               new_food = col_food.multiselect('Food Items', 
                                               menu_items_lookup, 
                                               default=clean_column_entries(review, 'Menu_Item'), 
                                               key= 'food_item' + str(from_real_to_fake[index])+venue)
               
               new_drink = col_drinks.multiselect('Drink Items',
                                                   drink_items_lookup, 
                                                   default=clean_column_entries(review, 'Drink_Item'), 
                                                   key='drink_item' + str(from_real_to_fake[index])+venue)
               
               new_label = st.multiselect('Label Dishoom',
                                          options_for_classification, 
                                          default=clean_column_entries(review, 'Label_Dishoom'), 
                                          key='label' + str(from_real_to_fake[index])+venue)

               with c_ov_r:
                  overall_rating = sac.rate(label=f'Overall Rating: **{review["Overall_Rating"]}**', value=int(review['New_Overall_Rating']), count=value_map[float(review['Overall_Rating']) if review['Overall_Rating'] not in nans_map else 5], key = 'overall' + str(from_real_to_fake[index])+venue)
               with c_fo_r:
                  food_rating = sac.rate(label=f'Food Rating: **{review["Feedback_Food_Rating"]}**', value=int(review['New_Food_Rating']), count=value_map[float(review['Feedback_Food_Rating']) if review['Feedback_Food_Rating']not in nans_map else 5], key = 'food' + str(from_real_to_fake[index])+venue)
               with c_dr_r:
                  drink_rating = sac.rate(label=f'Drink Rating: **{review["Feedback_Drink_Rating"]}**', value=int(review['New_Drink_Rating']), count=value_map[float(review['Feedback_Drink_Rating']) if review['Feedback_Drink_Rating']not in nans_map else 5], key = 'drink' + str(from_real_to_fake[index])+venue)
               with c_se_r:
                  service_rating = sac.rate(label=f'Service Rating: **{review["Feedback_Service_Rating"]}**', value=int(review['New_Service_Rating']), count=value_map[float(review['Feedback_Service_Rating']) if review['Feedback_Service_Rating'] not in nans_map  else 5], key = 'service' + str(from_real_to_fake[index])+venue)
               with c_am_r:
                  ambience_rating = sac.rate(label=f'Ambience Rating: **{review["Feedback_Ambience_Rating"]}**', value=int(review['New_Ambience_Rating']), count=value_map[float(review['Feedback_Ambience_Rating']) if review['Feedback_Ambience_Rating'] not in nans_map else 5], key = 'ambience' + str(from_real_to_fake[index])+venue)
                  
            # update the review
            review['New_Overall_Rating'] = overall_rating
            review['New_Food_Rating'] = food_rating
            review['New_Drink_Rating'] = drink_rating
            review['New_Service_Rating'] = service_rating
            review['New_Ambience_Rating'] = ambience_rating
            review['Label_Dishoom'] = ' - '.join(new_label)
            review['Menu_Item'] = ' - '.join(new_food)
            review['Drink_Item'] = ' - '.join(new_drink)
            review['👍'] = '1' if is_best else '0'
            review['👎'] = '1' if is_worst else '0'
            review['💡'] = '1' if is_suggestion else '0'

            # update the
            def OnUpdateButton(review):
               with st.spinner('Updating review...'):
                  doc[0].reference.get().reference.update(review)
               st.success('Review updated successfully')
            
            def OnDeleteSingleRev(index):
               with st.spinner('Deleting review...'):
                  doc[0].reference.delete()
               st.success('Review deleted successfully')

            if c1_button.form_submit_button('Update', type='primary', use_container_width=True):
               OnUpdateButton(review, index)
            
            if c2_button.form_submit_button('Delete', type='secondary', use_container_width=True):
               OnDeleteSingleRev(index)   
      
      with venue_tab:
            venue_map = {
               'Dishoom Covent Garden': 1,
               'Dishoom Shoreditch': 2,
               'Dishoom Kings Cross': 3,
               'Dishoom Carnaby': 4,
               'Dishoom Edinburgh': 5,
               'Dishoom Kensington': 6,
               'Dishoom Manchester': 7,
               'Dishoom Birmingham': 8,
               'Dishoom Canary Wharf': 9
            }
            
            # get the id from the name
            store_id = venue_map[venue]
            date = review['Reservation_Date']
            time = review['Reservation_Time']
            get_sales_date(store_id= [store_id], date = date, time = time)
            st.stop()
        
   def download(self):
      c1,c2,c3 = st.columns([0.3,0.5, 0.2])

      c1.subheader('Download')
      data_list = []
      data = self.db.collection('feedback').stream()

      for doc in data:
         reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
         for review in reviews:
            data_list.append(review.to_dict())

      df = pd.DataFrame(data_list)
      if len(df) == 0:  
         st.info('No data found - Please select Upload to upload the data')
         st.stop()
      df = df.sort_values(by=['idx'])
      all_venues = df['Reservation_Venue'].unique().tolist()
      all_venues =  ['All'] + all_venues
      
      if len(df) == 0:  
         st.info('No data found - Please select Upload to upload the data')
         st.stop()
      df = df.sort_values(by=['idx'])
      
      venue = st.selectbox('Select the Venue', options = all_venues, index = 0)
      if venue != 'All':
         df = df[df['Reservation_Venue'] == venue]
      
      name_file = c2.text_input('File Name', value ='labelled_reviews' if venue == 'All' else f'labelled_rev_{venue}')
      @st.cache
      def convert_df(df):
         # IMPORTANT: Cache the conversion to prevent computation on every rerun
         return df.to_csv().encode('utf-8')

      csv = convert_df(df)

      c3.download_button(
         label="Download data as CSV",
         data=csv,
         file_name=name_file,
         mime='text/csv',
         type = 'primary'
      )
      st.write(f'{len(df)} Reviews')
      st.write(df)
      self.plot(df)

      st.stop()

   def ai_assistant(self):
      if 'data' not in st.session_state:

      
         data_list = []
         data = self.db.collection('feedback').stream()

         for doc in data:
            reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
            for review in reviews:
               data_list.append(review.to_dict())

         df = pd.DataFrame(data_list)
         st.session_state.data = df

      from templates.ai_mod import final_page_ai
      final_page_ai(st.session_state.data)

   def reporting(self):
      '''
      1. Read the data from the database
      2. Create a dataframe
      3. Sort the dataframe by idx
      4. Create a container for each sentiment
      5. Create a delete button
      '''
      
      data_list = []
      data = self.db.collection('feedback').stream()

      for doc in data:
         reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
         for review in reviews:
            data_list.append(review.to_dict())

      df = pd.DataFrame(data_list)
      df = df.sort_values(by=['idx'])
      #st.write(len(df))

      data = df
      # get unique venues
      list_of_venue = data['Reservation_Venue'].unique()
      # for each venue get the ones with negative feedback
      for i, venue in enumerate(list_of_venue):
         venue_data = data[data['Reservation_Venue'] == venue]
         venue_data_to_lab = venue_data[venue_data['Sentiment'] == 'NEGATIVE']
         # take off empty detail
         venue_data_to_lab = venue_data_to_lab[venue_data_to_lab['Details'] != '']
         # take off nan
         venue_data_to_lab = venue_data_to_lab[venue_data_to_lab['Details'] != 'nan']
         #st.write(len(venue_data_to_lab))
         # get the total number of reviews
         tot_ = len(venue_data_to_lab) + 6
         tot_done = len(venue_data_to_lab[venue_data_to_lab['Label_Dishoom'] != ''])
         tot_not_done = len(venue_data_to_lab[venue_data_to_lab['Label_Dishoom'] == ''])
         tot_done_before = len(venue_data_to_lab[venue_data_to_lab['Label_Dishoom'] == 'Done'])
         # get total thumbs up and thumbs down
         # thumbs up emoji is  👍
         # thumbs down emoji is 👎
         thumbs_up = venue_data[venue_data['👍'] == '1']
         thumbs_down = venue_data[venue_data['👎'] == '1']

         # add the number to the total
         tot_done += len(thumbs_up) + len(thumbs_down)

         # get suggestions 
         suggestions = venue_data[venue_data['💡'] == '1']
         number_of_thumbs_up = len(thumbs_up)
         number_of_thumbs_down = len(thumbs_down)
         
         
         try:
               message = venue + f' **{round(tot_done/tot_*100, 2)}%**' 
         except:
               message = venue
         
         with st.expander(message):
               tot_already_done = len(venue_data_to_lab[venue_data_to_lab['Label_Dishoom'] != ''])
               tab_pie, tab_good, tab_bad, tab_sugg, tab_g = st.tabs([f'Reviews {len(venue_data_to_lab)}/{tot_already_done}',
                                                               f'Good {number_of_thumbs_up}/3',
                                                               f'Bad {number_of_thumbs_down}/3',
                                                               f'Suggestions {len(suggestions)}',
                                                               f'Labelling Graphs',
                                                               ])


               with tab_pie:
                  # now create a pie chart
                  fig = go.Figure(data=[go.Pie(labels=['Done', 'Not Done'], values=[tot_done, tot_not_done])])
                  fig.update_layout(title_text=venue)
                  # green for done, red for not done
                  fig.update_traces(marker_colors=['green', 'red'])
                  # set opacity
                  fig.update_traces(opacity=0.6, textinfo='percent+label')
                  # set size 200x200
                  st.plotly_chart(fig, use_container_width=True)


               with tab_good:
                  for good in thumbs_up['Details'].tolist():
                     st.write(good)

               with tab_bad:
                  for bad in thumbs_down['Details'].tolist():
                     st.write(bad)

               with tab_sugg:
                  for sugg in suggestions['Details'].tolist():
                     st.write(sugg)

               with tab_g:
                  create_chart_totals_labels(venue_data_to_lab, st.container())

      # add a complete df 
      with st.expander('View all data'):
         data = data.rename(columns={'Overall Rating': 'Overall', 'Feedback: Food Rating': 'Food', 'Feedback: Service Rating': 'Service', 'Feedback: Ambience Rating': 'Ambience',
                                       'Feedback: Drink Rating': 'Drink'})
         st.write(data)

      # create a download link
      def get_table_download_link(data):
         # rename the columns that have emoji
         data = data.rename(columns={'👍': 'thumbs_up', '👎': 'thumbs_down', '💡': 'suggestions'})
         # create a link to download the dataframe
         csv = data.to_csv(index=False)
         b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
         href = f'<a href="data:file/csv;base64,{b64}" download="data.csv">Download csv file</a>'
         return href


      st.markdown(get_table_download_link(data), unsafe_allow_html=True)

   def plot(self,df):
      final = df[df['Details'] != 'nan']
      container_keywords = st.sidebar.container()
      with st.expander('Graphs 📉', expanded=False): # graph emoji 📈 or 📊 or 📉 
         tabs = st.tabs(['Graphs', 'Keywords', 'Pie Chart', 'Source Analysis', 'Day Analysis', 'Hour Analysis', 'Week Analysis', 'Month Analysis', 'Totals'])

         with tabs[0]:
            create_timeseries_graph(final)

         with tabs[1]:
            create_graph_keywords_as_a_whole(final, container = container_keywords)

         with tabs[2]:
            create_pie_chart(final)
         
         with tabs[3]:
            create_graph_for_source_analysis(final)

         with tabs[4]:
            create_graph_for_day_analysis(final)
         
         with tabs[5]:
            create_graph_for_hour_analysis(final)

         with tabs[6]:
            create_graph_for_week_analysis(final)

         with tabs[7]:
            create_graph_for_month_analysis(final)
         with tabs[8]:
            create_chart_totals_labels(final, st.container())

   def run(self):
      choice = self.create_sidebar_menu()
      if choice == 'Feedback':
         if self.df is None:
               self.read(show = False)
               st.stop()  

      if choice == 'Scoring':
            self.edit()
            st.stop()
      elif choice == 'Upload':
            self.upload_excels()
            st.stop()
      elif choice == 'Download':
            self.download()
            st.stop()
      elif choice == 'AI Assistant':
            self.ai_assistant()
            st.stop()
      elif choice == 'Reporting':
            self.reporting()
            st.stop()
      elif choice == 'Settings':
            self.settings()
            st.stop()

   def create_sidebar_menu(self, with_db = True):
      with st.sidebar:
         menu = sac.menu([
               sac.MenuItem('Feedback', icon='database', children=[
                  sac.MenuItem('Scoring', icon='brush'),
                  sac.MenuItem('Upload', icon='upload'),
                  sac.MenuItem('Download', icon='download'),
               ]),
               sac.MenuItem('AI Assistant', icon='robot'),
               sac.MenuItem('Reporting', icon='share'),
               sac.MenuItem('Settings', icon='gear'),
                  
         ], open_all=False)
         return menu

if __name__ == "__main__":
   from login_script import login
   def main(name_user):
      try: 
         fb = FeedBackHelper(name_user)
         fb.run()
      except Exception as e:
         st.write(e)
         st.write('Please check the connection with the database')
         st.stop()

   DEBUG = False
   CONFIG_FILE = "config.yaml"    
   login(render_func=main,
               config_file=CONFIG_FILE)
