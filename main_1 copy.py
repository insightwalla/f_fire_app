
'''
author: Roberto Scalas 
date:   2023-10-17 09:37:39.647582
'''

import streamlit as st
from streamlit_pills import pills
import streamlit_antd_components as sac
import pandas as pd
from utils import *
from parameters import *
from graphs import *
from ai_classifier import ArtificialWalla
from translator_walla import Translator
from parameters import options_for_classification, menu_items_lookup, drink_items_lookup

#https://firebase.google.com/docs/firestore/manage-data/add-data

import streamlit as st
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

from ai_classifier import ArtificialWalla as walla

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
   def __init__(self):
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
      data = self.db.collection(u'feedback').stream()
      for doc in data:
         reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
         for i, review in enumerate(reviews):
            #st.write(review.to_dict())
            # make it a dataframe
            df = pd.concat([df, pd.DataFrame(review.to_dict(), index=[i])], axis=0)
            # sort by idx
      df = df.sort_values(by=['idx'])
      if show:
         st.write(df)

      all_venues = df['Reservation_Venue'].unique().tolist()
      res_dict = {
         'all_venues' : all_venues,
         'data' : df
      }

      df_empty = df[df['Details'] == 'nan']
      df = df[df['Details'] != 'nan']
      df_empty = rescoring_empty(df_empty, new=True)
      create_container_for_each_sentiment(df, df_empty)


      def OnDeleteAll():
         # delete all the data
         data = self.db.collection(u'feedback').stream()
         for doc in data:
            reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
            for i, review in enumerate(reviews):
               self.db.collection(u'feedback').document(doc.id).collection(u'reviews').document(review.id).delete()
            self.db.collection(u'feedback').document(doc.id).delete()
         st.success('All the data was deleted successfully')

      def OnDeleteVenueRevs():
         # delete all the reviews for the venue
         data = self.db.collection(u'feedback').stream()
         for doc in data:
            if doc.id == self.venue:
               reviews = self.db.collection(u'feedback').document(doc.id).collection(u'reviews').stream()
               for i, review in enumerate(reviews):
                  self.db.collection(u'feedback').document(doc.id).collection(u'reviews').document(review.id).delete()
               st.success('All the reviews for the venue were deleted successfully')
               st.stop()
      # create a delete button 

      ButtonDeleteAll = st.sidebar.button('Delete all the data', type = 'primary', use_container_width=True, on_click=OnDeleteAll)

      # create a delete button for the selected venue
      c1,c2 = st.columns(2)
      ButtonDeleteVenueRevs = c2.button('Delete all the reviews for the venue', type = 'secondary', use_container_width=True, on_click=OnDeleteVenueRevs)
      return res_dict

   def upload_excels(self):
      
      # 1. Upload the excel
      uploaded_files = st.file_uploader("Upload Excel", type="xlsx", accept_multiple_files=True, key='upload')

      # 2. Check if the file is not empty
      if uploaded_files is not None:
         # 3. Create a progress bar 
         my_big_bar = st.progress(0, text='Uploading data')
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
               # delete the collection
               reviews = self.db.collection(u'feedback').document(name).collection(u'reviews').stream()
               for i, review in enumerate(reviews):
                  self.db.collection(u'feedback').document(name).collection(u'reviews').document(review.id).delete()

               # upload the data
               for index, row in df.iterrows():
                  self.db.collection(u'feedback').document(name).collection(u'reviews').add(row.to_dict())
                  # update the bar
                  my_bar.progress(int((index+1) * 100/len(df)), text='Uploading data')
            else:
               # empty the doc
               doc_ref.set({})
               # upload the data
               for index, row in df.iterrows():
                  self.db.collection(u'feedback').document(name).collection(u'reviews').add(row.to_dict())
                  # update the bar
                  my_bar.progress(int((index+1) * 100/len(df)), text='Uploading data')
            st.success('Data uploaded successfully')
            my_big_bar.progress(int((i+1) * 100/len(uploaded_files)), text='Uploading data')
   
   def edit(self):
      res = self.read(show= False)
      df = res['data']
      all_venues = res['all_venues']

      # create a selectbox to choose the venue
      c1,c2 = st.columns(2)
      self.venue = c1.selectbox('Choose the venue', all_venues)
      venue = self.venue

      # get the data for the venue
      df = df[df['Reservation_Venue'] == venue]
      st.write(len(df))


      # strip the space in details
      df['Details'] = df['Details'].apply(lambda x: x.strip())
      df_full = df[df['Details'] != 'nan']
      list_of_index_full = df_full['idx'].unique().tolist()
      # st.write(len(df_full))
      # st.write(df_full)
      # st.write(len(list_of_index_full))
      from_real_to_fake = {i : index for i, index in enumerate(list_of_index_full)}

      index = c2.number_input('Choose the index', min_value=0, max_value=len(df_full)-1, value = 0, step=1)

      def get_review_by_venue_and_idx(venue, idx, give_doc = False):
         notes_ref = self.db.collection(u'feedback').document(venue).collection(u'reviews')
         query = notes_ref.where('idx', '==', str(idx))
         results = query.get()
         res = [result.to_dict() for result in results] 
         if give_doc:
            return results
         else:
            return res[0] if res else None
      
      with st.form('scoring'):
         col_buttons = st.columns(5)
         c1_button = col_buttons[4]
         c2_button = col_buttons[0]
         #st.write(index, from_real_to_fake[index])
         #st.write(from_real_to_fake)
         st.write('Fake index: ', index, 'Real index: ', from_real_to_fake[index])
         doc = get_review_by_venue_and_idx(venue, from_real_to_fake[index], give_doc=True)
         review = doc[0].to_dict()
         st.write(review['Details'])
         #st.stop()
         #st.stop()
         # get the values
         c1,c2,c3,c4,c5 = st.columns(5)
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
         with c1:
            overall_rating = sac.rate(label=f'Overall Rating: **{review["Overall_Rating"]}**', value=int(review['New_Overall_Rating']), count=value_map[float(review['Overall_Rating']) if review['Overall_Rating'] not in nans_map else 5], key = 'overall' + str(index))
         with c2:
            food_rating = sac.rate(label=f'Food Rating: **{review["Feedback_Food_Rating"]}**', value=int(review['New_Food_Rating']), count=value_map[float(review['Feedback_Food_Rating']) if review['Feedback_Food_Rating']not in nans_map else 5], key = 'food' + str(index))
         with c3:
            drink_rating = sac.rate(label=f'Drink Rating: **{review["Feedback_Drink_Rating"]}**', value=int(review['New_Drink_Rating']), count=value_map[float(review['Feedback_Drink_Rating']) if review['Feedback_Drink_Rating']not in nans_map else 5], key = 'drink' + str(index))
         with c4:
            service_rating = sac.rate(label=f'Service Rating: **{review["Feedback_Service_Rating"]}**', value=int(review['New_Service_Rating']), count=value_map[float(review['Feedback_Service_Rating']) if review['Feedback_Service_Rating'] not in nans_map  else 5], key = 'service' + str(index))
         with c5:
            ambience_rating = sac.rate(label=f'Ambience Rating: **{review["Feedback_Ambience_Rating"]}**', value=int(review['New_Ambience_Rating']), count=value_map[float(review['Feedback_Ambience_Rating']) if review['Feedback_Ambience_Rating'] not in nans_map else 5], key = 'ambience' + str(index))
         
         label_dishoom = review['Label_Dishoom'].split('-') if '-' in review['Label_Dishoom'] else [review['Label_Dishoom']]
         label_dishoom = [l.strip() for l in label_dishoom if l != '']

         # food items
         food_items = review['Menu_Item'].split('-') if '-' in review['Menu_Item'] else [review['Menu_Item']]
         food_items = [l.strip() for l in food_items if l != '']

         # drink items
         drink_items = review['Drink_Item'].split('-') if '-' in review['Drink_Item'] else [review['Drink_Item']]
         drink_items = [l.strip() for l in drink_items if l != '']

         c1,c2 = st.columns(2)
         new_food = c1.multiselect('Food Items', menu_items_lookup, default=food_items, key='food_item' + str(index))
         new_drink = c2.multiselect('Drink Items', drink_items_lookup, default=drink_items, key='drink_item' + str(index))
         new_label = st.multiselect('Label Dishoom', options_for_classification, default=label_dishoom, key='label' + str(index))

         # update the review
         review['New_Overall_Rating'] = overall_rating
         review['New_Food_Rating'] = food_rating
         review['New_Drink_Rating'] = drink_rating
         review['New_Service_Rating'] = service_rating
         review['New_Ambience_Rating'] = ambience_rating
         review['Label_Dishoom'] = ' - '.join(new_label)
         review['Menu_Item'] = ' - '.join(new_food)
         review['Drink_Item'] = ' - '.join(new_drink)

         # update the
         def OnUpdateButton(review, index):
            with st.spinner('Updating review...'):
               doc[0].reference.get().reference.update(review)
            st.success('Review updated successfully')
         
         def OnDeleteSingleRev(index):
            # get the doc
            doc_ref = self.db.collection(u'feedback').document(venue).collection(u'reviews')
            query = doc_ref.where('idx', '==', str(from_real_to_fake[index]))
            results = query.get()
            res = [result.to_dict() for result in results]
            doc = doc_ref.document(res[0]['idx'])
            doc.delete()
            st.success('Review deleted successfully')
         
         UpdateButton = c1_button.form_submit_button('Update', type='primary', use_container_width=True, on_click=OnUpdateButton, args=(review, index))
         DeleteButton = c2_button.form_submit_button('Delete', type='secondary', use_container_width=True, on_click=OnDeleteSingleRev, args=(index,))

   def run(self):
      choice = self.create_sidebar_menu()
      st.write(choice)
      if choice == 'Edit':
         self.edit()
      elif choice == 'Upload':
         self.upload_excels()
      elif choice == 'Settings':
         st.write(choice)

   def create_sidebar_menu(self, with_db = True):
        with st.sidebar:
            menu = sac.menu([
                sac.MenuItem('Feedback', icon='database', children=[
                     sac.MenuItem('Edit', icon='brush'),
                     sac.MenuItem('Upload', icon='upload'),
                     sac.MenuItem('Download', icon='download'),
                     sac.MenuItem('Delete', icon='trash'),
                  ]),

                sac.MenuItem('AI Assistant', icon='robot'),
                sac.MenuItem('Settings', icon='gear', children=[
                    sac.MenuItem('AI', icon='robot'),
                    sac.MenuItem('Vault', icon='lock'),
                    sac.MenuItem('Theme', icon='brush'),
                    sac.MenuItem('About', icon='info-circle'),
                ]),
                    
            ], open_all=False)
            return menu
        
if __name__ == "__main__":
   try: 
      st.set_page_config(layout="wide")
      fb = FeedBackHelper()
      fb.run()
   except Exception as e:
      st.write(e)
      st.write('Please check the connection with the database')
      st.stop()