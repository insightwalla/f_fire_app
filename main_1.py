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
      #st.success('Connected to Firestore')

   def read(self, show = True):
      '''
      1. Read the data from the database
      2. Create a dataframe
      3. Sort the dataframe by idx
      4. Create a container for each sentiment
      5. Create a delete button
      '''
      df = pd.DataFrame()
      data = self.db.collection('feedback').stream()
      for doc in data:
         reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
         for i, review in enumerate(reviews):
            #st.write(review.to_dict())
            # make it a dataframe
            df = pd.concat([df, pd.DataFrame(review.to_dict(), index=[i])], axis=0)
            # sort by idx
      if len(df) == 0:  
         st.info('No data found - Please select Upload to upload the data')
         st.stop()
      df = df.sort_values(by=['idx'])
      if show:
         st.write(df)

      all_venues = df['Reservation_Venue'].unique().tolist()
      res_dict = {
         'all_venues' : all_venues,
         'data' : df
      }
      self.all_data = df
      df_empty = df[df['Details'] == 'nan']
      df = df[df['Details'] != 'nan']

      if 'venue' not in st.session_state:
         try:
            st.session_state.venue = df['Reservation_Venue'].unique().tolist()[0]
         except:
            st.session_state.venue = None

      self.venue = st.session_state.venue
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
         for venue in all_venues:
            OnDeleteVenueRevs(venue)
         st.success('All data deleted successfully')

      # create a delete button for the selected venue
      c1,c2 = st.columns(2)
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
                  st.write('Document already exists')
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

      upload = upload_space.button('Upload', type='primary', use_container_width=True, on_click=handle_upload)
   
   def edit(self):
      res = self.read(show= False)
      df = res['data']
      all_venues = res['all_venues']

      # create a selectbox to choose the venue
      c1,c2 = st.columns(2)
      self.venue = c1.selectbox('Choose the venue', all_venues, key='venue')

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

      if st.sidebar.button(f'Delete {self.venue}', type = 'primary', use_container_width=True):
         OnDeleteVenueRevs(self.venue)

      venue = self.venue
      # get the data for the venue
      df = df[df['Reservation_Venue'] == self.venue]

      # strip the space in details
      df['Details'] = df['Details'].apply(lambda x: x.strip())
      df_full = df[df['Details'] != 'nan']
      list_of_index_full = df_full['idx'].unique().tolist()
      # st.write(len(df_full))
      # st.write(df_full)
      # st.write(len(list_of_index_full))
      from_real_to_fake = {i+1 : index for i, index in enumerate(list_of_index_full)}
      #st.write('All Data', len(df), 'Data with Details', len(df_full))
      index = c2.number_input('Choose the index', min_value=1, max_value=len(df_full), value = 1, step=1)

      def get_review_by_venue_and_idx(venue, idx, give_doc = False):
         notes_ref = self.db.collection(u'feedback').document(venue).collection(u'reviews')
         query = notes_ref.where('idx', '==', str(idx))
         results = query.get()
         res = [result.to_dict() for result in results] 
         if give_doc:
            return results
         else:
            return res[0] if res else None
      
      tab1, tab2 = st.tabs(['Edit', 'Venue Capacity'])
      with tab1:
         with st.form('scoring'):
            col_buttons = st.columns(5)
            c1_button = col_buttons[4]
            c2_button = col_buttons[0]
            #st.write(index, from_real_to_fake[index])
            #st.write(from_real_to_fake)
            #st.write('Fake index: ', index, 'Real index: ', from_real_to_fake[index])
            with st.spinner('Loading review...'):
               doc = get_review_by_venue_and_idx(venue, from_real_to_fake[index], give_doc=True)
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
               c1,c2,c3 = st.columns(3)
               # get all the best 
               best_rev = df_full[df_full['👍'] == '1']
               worst_rev = df_full[df_full['👎'] == '1']
               suggestions_rev = df_full[df_full['💡'] == '1']
               is_this_best = review['👍'] == '1'
               is_this_worst = review['👎'] == '1'
               is_this_suggestion = review['💡'] == '1'

               if len(best_rev) == 3 and not is_this_best:
                  is_best = False
                  c1.write('You have already selected 3 best reviews')
               else:
                  is_best = c1.toggle('👍',review['👍'] == '1', key = f'is_good{index}={venue}')

               if len(worst_rev) == 3 and not is_this_worst:
                  is_worst = False
                  c2.write('You have already selected 3 worst reviews')
               else:
                  is_worst = c2.toggle('👎',review['👎'] == '1', key= f'is_bad{index}={venue}')

               if len(suggestions_rev) == 3 and not is_this_suggestion:
                  is_suggestion = False
                  c3.write('You have already selected 3 suggestions')
               else:
                  is_suggestion = c3.toggle('💡',review['💡'] == '1', key= f'is_suggestion{index}={venue}')

               label_dishoom = review['Label_Dishoom'].split('-') if '-' in review['Label_Dishoom'] else [review['Label_Dishoom']]
               label_dishoom = [l.strip() for l in label_dishoom if l != '']

               # food items
               food_items = review['Menu_Item'].split('-') if '-' in review['Menu_Item'] else [review['Menu_Item']]
               food_items = [l.strip() for l in food_items if l != '']

               # drink items
               drink_items = review['Drink_Item'].split('-') if '-' in review['Drink_Item'] else [review['Drink_Item']]
               drink_items = [l.strip() for l in drink_items if l != '']

               c1,c2 = st.columns(2)
               new_food = c1.multiselect('Food Items', menu_items_lookup, default=food_items, key='food_item' + str(index)+venue)
               new_drink = c2.multiselect('Drink Items', drink_items_lookup, default=drink_items, key='drink_item' + str(index)+venue)
               new_label = st.multiselect('Label Dishoom', options_for_classification, default=label_dishoom, key='label' + str(index)+venue)
               c1,c2,c3,c4,c5 = st.columns(5)

               with c1:
                  overall_rating = sac.rate(label=f'Overall Rating: **{review["Overall_Rating"]}**', value=int(review['New_Overall_Rating']), count=value_map[float(review['Overall_Rating']) if review['Overall_Rating'] not in nans_map else 5], key = 'overall' + str(index)+venue)
               with c2:
                  food_rating = sac.rate(label=f'Food Rating: **{review["Feedback_Food_Rating"]}**', value=int(review['New_Food_Rating']), count=value_map[float(review['Feedback_Food_Rating']) if review['Feedback_Food_Rating']not in nans_map else 5], key = 'food' + str(index)+venue)
               with c3:
                  drink_rating = sac.rate(label=f'Drink Rating: **{review["Feedback_Drink_Rating"]}**', value=int(review['New_Drink_Rating']), count=value_map[float(review['Feedback_Drink_Rating']) if review['Feedback_Drink_Rating']not in nans_map else 5], key = 'drink' + str(index)+venue)
               with c4:
                  service_rating = sac.rate(label=f'Service Rating: **{review["Feedback_Service_Rating"]}**', value=int(review['New_Service_Rating']), count=value_map[float(review['Feedback_Service_Rating']) if review['Feedback_Service_Rating'] not in nans_map  else 5], key = 'service' + str(index)+venue)
               with c5:
                  ambience_rating = sac.rate(label=f'Ambience Rating: **{review["Feedback_Ambience_Rating"]}**', value=int(review['New_Ambience_Rating']), count=value_map[float(review['Feedback_Ambience_Rating']) if review['Feedback_Ambience_Rating'] not in nans_map else 5], key = 'ambience' + str(index)+venue)
                  
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
            def OnUpdateButton(review, index):
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
      with tab2:
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

   def download(self):
      st.write('Download')
      
      def get_table_download_link(data):
         # rename the columns that have emoji
         data = data.rename(columns={'👍': 'thumbs_up', '👎': 'thumbs_down', '💡': 'suggestions'})
         # create a link to download the dataframe
         csv = data.to_csv(index=False)
         b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
         href = f'<a href="data:file/csv;base64,{b64}" download="data.csv">Download csv file</a>'
         return href
      
      df = pd.DataFrame()
      data = self.db.collection(u'feedback').stream()
      for doc in data:
         reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
         for i, review in enumerate(reviews):
            #st.write(review.to_dict())
            # make it a dataframe
            df = pd.concat([df, pd.DataFrame(review.to_dict(), index=[i])], axis=0)
            # sort by idx
      if len(df) == 0:  
         st.info('No data found - Please select Upload to upload the data')
         st.stop()
      df = df.sort_values(by=['idx'])
      if st.button('Download Data'):
         get_table_download_link(df)
         st.markdown(get_table_download_link(df), unsafe_allow_html=True)




   def ai_assistant(self):
      if 'data' not in st.session_state:

         df = pd.DataFrame()
         data = self.db.collection(u'feedback').stream()
         for doc in data:
            reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
            for i, review in enumerate(reviews):
                  #st.write(review.to_dict())
                  # make it a dataframe
                  df = pd.concat([df, pd.DataFrame(review.to_dict(), index=[i])], axis=0)
            # sort by idx
         df = df.sort_values(by=['idx'])
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
      df = pd.DataFrame()
      data = self.db.collection(u'feedback').stream()
      for doc in data:
         reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
         for i, review in enumerate(reviews):
               #st.write(review.to_dict())
               # make it a dataframe
               df = pd.concat([df, pd.DataFrame(review.to_dict(), index=[i])], axis=0)
         # sort by idx
      df = df.sort_values(by=['idx'])
      st.write(len(df))

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
         st.write(len(venue_data_to_lab))
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
      if choice == 'Scoring':
         with st.spinner('Loading Scoring...'):
            self.edit()
      elif choice == 'Upload':
         with st.spinner('Loading Upload...'):
            self.upload_excels()
      elif choice == 'Download':
         with st.spinner('Loading Download...'):
            self.download()
      elif choice == 'AI Assistant':
         with st.spinner('Loading AI Assistant...'):
            self.ai_assistant()
      elif choice == 'Reporting':
         with st.spinner('Loading Reporting...'):
            self.reporting()
      elif choice == 'Settings':
         with st.spinner('Loading Settings...'):
            self.settings()

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