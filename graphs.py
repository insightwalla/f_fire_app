# Plots
import streamlit as st
import pandas as pd
import numpy as np
from collections import Counter

import plotly.express as px
import plotly.graph_objects as go
from utils import *

def create_graphs_no_rev(df):
   df = df.copy()
   #st.write(df)
   scores = []
   columns_to_rescore = ['Feedback: Food Rating', 'Feedback: Drink Rating', 'Feedback: Service Rating', 'Feedback: Ambience Rating', 'Overall Rating']
   
   for _, row in df.iterrows():
      all_scores = row[columns_to_rescore].values
      # no zeros or empty
      all_scores = all_scores[all_scores != 0]
      # get average score
      avg_score = np.mean(all_scores)
      scores.append(avg_score)

   # create a dataframe with the scores
   df_scores = pd.DataFrame(scores, columns=['Score'])
   # get average score
   avg_score = np.mean(df_scores['Score'])

   # create a average for each row
   df['Average Score'] = scores

   col1, col2 = st.columns(2)
   col1.metric("Average Score", round(avg_score, 2))
   col2.metric("Total Reviews", len(df))

   fig = px.histogram(df_scores, x='Score', nbins=20, title='Distribution of scores')
   fig.update_layout(
      xaxis_title_text='Score',
      yaxis_title_text='Count',
      bargap=0.2,
      bargroupgap=0.1
   )


   c1,c2 = st.columns(2)
   c1.plotly_chart(fig, use_container_width=True)

   # group by day and get the average score
   df['date_for_filter'] = pd.to_datetime(df['date_for_filter'])
   df_day = df.groupby(['date_for_filter']).agg({'Average Score': 'mean'}).reset_index()

   fig = go.Figure()
   
   fig.add_trace(go.Bar(x=df_day['date_for_filter'], y=df_day['Average Score'], name='Average Score'))
   fig.update_layout(
      xaxis_title_text='Date',
      yaxis_title_text='Average Score',
      bargap=0.2,
      bargroupgap=0.1
   )
   # change color of bar if the value is less than the average
   fig.update_traces(marker_color='green')
   fig.add_trace(go.Scatter(x=df_day['date_for_filter'], y=[avg_score]*len(df_day), name='Average Score', mode='lines', marker_color='red'))


   c2.plotly_chart(fig, use_container_width=True)

def create_graph_keywords_as_a_whole(df, container = None):
   
   feat_for_key = ['Keywords', 'Sentiment']
   keywords_columns_sentiment = df[feat_for_key].copy()
   keywords_columns_sentiment.loc[:, 'Keywords'] = keywords_columns_sentiment['Keywords'].apply(lambda x: x.split('-'))
   keywords_columns_sentiment = keywords_columns_sentiment.explode('Keywords')
   # take off empty keywords
   keywords_columns_sentiment = keywords_columns_sentiment[keywords_columns_sentiment['Keywords'] != '']
   keywords_columns_sentiment = keywords_columns_sentiment.groupby(['Keywords', 'Sentiment']).size().reset_index(name='Count')
   # order by total
   keywords_columns_sentiment = keywords_columns_sentiment.sort_values(by=['Count'], ascending=False)
   fig = go.Figure()
   color = {'POSITIVE': 'green', 'NEGATIVE': 'red', 'neutral': 'lightblue'}
   for sentiment in keywords_columns_sentiment['Sentiment'].unique():
      # add a bar group for each sentiment
      fig.add_trace(go.Bar(x=keywords_columns_sentiment[keywords_columns_sentiment['Sentiment'] == sentiment]['Keywords'], y=keywords_columns_sentiment[keywords_columns_sentiment['Sentiment'] == sentiment]['Count'], name=sentiment, opacity=0.5, marker_color = color[sentiment]))
   fig.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
   fig.update_layout(xaxis_tickangle=-45)
   # no legend and title
   fig.update_layout(showlegend=False)
   
   if container != None:
      st.plotly_chart(fig, use_container_width=True)
   else:
      st.plotly_chart(fig, use_container_width=True)

def create_timeseries_graph(df):
   # check that
   df['Date_Submitted'] = pd.to_datetime(df['Date_Submitted'])
   df['Date_Submitted'] = df['Date_Submitted'].dt.date
   # same for reservation date
   df['Reservation_Date'] = pd.to_datetime(df['Reservation_Date'])
   df['Reservation_Date'] = df['Reservation_Date'].dt.date

   # create a new column called date_to_plot, is the reseravtion date if it is not empty, otherwise is the date submitted
   df['Date_to_plot'] = df['Reservation_Date'].fillna(df['Date_Submitted'])
   df['Date_to_plot'] = pd.to_datetime(df['Date_to_plot'])
   df['Date_to_plot'] = df['Date_to_plot'].dt.date

   # take off empty details   
   df = df[df['Details'] != '']

   df = df.groupby(['Date_to_plot', 'Sentiment']).size().reset_index(name='Count')

   fig = go.Figure()
   color = {'POSITIVE': 'green', 'NEGATIVE': 'red', 'neutral': 'lightblue'}
   for sentiment in df['Sentiment'].unique():
      # add a bar group for each sentiment
      fig.add_trace(go.Bar(x=df[df['Sentiment'] == sentiment]['Date_to_plot'], y=df[df['Sentiment'] == sentiment]['Count'], name=sentiment, opacity=0.5, marker_color = color[sentiment]))

   fig.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
   fig.update_layout(xaxis_tickangle=-45)
   st.plotly_chart(fig, use_container_width=True)

   # create a graph divided by day part

def create_timeseries_graph_section(df, container, col_date = 'date_for_filter', section = 'Product'):
   # filter by section
   df = filter_only_food_related_reviews(df)
   df_for_later = df.copy()
   df['Date to plot'] = pd.to_datetime(df[col_date])
   df['Date to plot'] = df['Date to plot'].dt.date

   # take off empty details   
   df = df[df['details'] != '']

   df = df.groupby(['Date to plot', 'sentiment']).size().reset_index(name='Count')

   fig = go.Figure()
   color = {'POSITIVE': 'green', 'NEGATIVE': 'red', 'neutral': 'lightblue'}
   for sentiment in df['sentiment'].unique():
      # add a bar group for each sentiment
      fig.add_trace(go.Bar(x=df[df['sentiment'] == sentiment]['Date to plot'], y=df[df['sentiment'] == sentiment]['Count'], name=sentiment, opacity=0.5, marker_color = color[sentiment]))

   fig.update_layout(barmode='stack', xaxis={'categoryorder':'total descending'})
   fig.update_layout(xaxis_tickangle=-45)
   st.plotly_chart(fig, use_container_width=True)

   # create a graph divided by day part

def create_container_for_each_sentiment(df, df_empty = None):
   # plot total negative, neutral, positive
   positive = df[df['Sentiment'] == 'POSITIVE']
   negative = df[df['Sentiment'] == 'NEGATIVE']
   neutral = df[df['Sentiment'] == 'neutral']
   neutral = neutral[neutral['Details'] != '']
   
   number_of_total_reviews = len(df)
   with st.expander('Reviews **{}**'.format(number_of_total_reviews + len(df_empty))):
      tab_empty, tab_positive, tab_negative, tab_neutral = st.tabs([f'Empty **{len(df_empty)}**', f'Positive **{len(positive)}**', f'Negative **{len(negative)}**', f'Neutral **{len(neutral)}**'])
      with tab_positive:
         st.write(positive)
      with tab_negative:
         st.write(negative)
      with tab_neutral:
         st.write(neutral)
      with tab_empty:
         df = df_empty 
         scores = []
         columns_to_rescore = ['Feedback_Food_Rating', 'Feedback_Drink_Rating', 'Feedback_Service_Rating', 'Feedback_Ambience_Rating', 'Overall_Rating']
         # transform in int if possible
         # if value in column to rescore is empty then change to 0
         for column in columns_to_rescore:
            df[column] = df[column].fillna(0).astype(int)
         for _, row in df.iterrows():
            all_scores = row[columns_to_rescore].values
            # no zeros or empty
            all_scores = all_scores[all_scores != 0]
            # get average score
            avg_score = np.mean(all_scores)
            scores.append(avg_score)

         # create a dataframe with the scores
         df_scores = pd.DataFrame(scores, columns=['Score'])
         # take off na  
         avg_score = np.mean(df_scores['Score'])

         # create a average for each row
         df['Average Score'] = scores

         col1, col2 = st.columns(2)
         col1.metric(label = "Average Score", value = f'{round(avg_score*10, 1)}%')
         col2.metric("Total Reviews", len(df))

         # take an average of each column in the list
         averages = []
         for column in columns_to_rescore:
            # take off the nan values
            val = df[column].values
            val = val[~np.isnan(val)]
            # take off the zeros
            val = val[val != 0]
            averages.append(np.mean(val))

         columns_scores = st.columns(len(columns_to_rescore))
         for i,avg in enumerate(averages):
            columns_scores[i].metric(label = columns_to_rescore[i] ,value = f'{round(avg*10, 1)}%')

         fig = px.histogram(df_scores, x='Score', nbins=20, title='Distribution of scores', opacity=0.5, text_auto=True)
         fig.update_layout(
            xaxis_title_text='Score',
            yaxis_title_text='Count',
            bargap=0.2,
            bargroupgap=0.1
         )


         c1,c2 = st.columns(2)
         c1.plotly_chart(fig, use_container_width=True)

         # group by day and get the average score
         df.loc[:,'date_for_filter'] = pd.to_datetime(df.loc[:,'date_for_filter'])
         df_day = df.groupby(['date_for_filter']).agg({'Average Score': 'mean'}).reset_index()

         fig = go.Figure()
         
         fig.add_trace(go.Bar(x=df_day['date_for_filter'], y=df_day['Average Score'], name='Average Score', text=df_day['Average Score'], opacity=0.5))
         fig.update_layout(
            xaxis_title_text='Date',
            yaxis_title_text='Average Score',
            bargap=0.2,
            bargroupgap=0.1
         )
         # change color of bar if the value is less than the average
         fig.update_traces(marker_color='green')
         fig.add_trace(go.Scatter(x=df_day['date_for_filter'], y=[avg_score]*len(df_day), name='Average Score', mode='lines', marker_color='red', text = [avg_score]*len(df_day), opacity=0.5))

         c2.plotly_chart(fig, use_container_width=True)
         st.write(df_empty)

def create_pie_chart(df):
   # plot total negative, neutral, positive
   positive = df[df['Sentiment'] == 'POSITIVE']
   negative = df[df['Sentiment'] == 'NEGATIVE']
   neutral = df[df['Sentiment'] == 'neutral']
   neutral = neutral[neutral['Details'] != '']
   # create a pie chart
   labels = ['Positive', 'Negative', 'Neutral']
   values = [len(positive), len(negative), len(neutral)]
   # transform to percent
   values = [round((v / sum(values)) * 100, 1) for v in values]
   # create the figure
   fig = go.Figure(data=[go.Pie(labels=labels, values=values)])
   fig.update_layout(showlegend=False)
   fig.update_traces(hoverinfo=f'label+percent', textinfo='value', textfont_size=20, opacity=0.5,
                     marker=dict(colors=['green', 'red', 'lightblue']))
   fig.update_traces(textposition='inside', textinfo='percent')
   # ADD title
   fig.update_layout(title_text='Overall sentiment')
   st.plotly_chart(fig, use_container_width=True)

   # 
#
def create_graph_for_week_analysis(df):
   features_time = ['Week_Year', 'Sentiment']
   df = df[features_time].groupby(features_time).size().reset_index(name='Count')

   fig = go.Figure()

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'POSITIVE']['Week_Year'], y=df[df['Sentiment'] == 'POSITIVE']['Count'], name='POSITIVE', opacity=0.5, marker_color = 'green'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'NEGATIVE']['Week_Year'], y=df[df['Sentiment'] == 'NEGATIVE']['Count'], name='NEGATIVE', opacity=0.5, marker_color = 'red'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'neutral']['Week_Year'], y=df[df['Sentiment'] == 'neutral']['Count'], name='NEUTRAL', opacity=0.5, marker_color = 'lightblue'))

   fig.update_layout(xaxis_tickangle=-45)
   fig.update_layout(barmode='stack')
   # set title to "Distribution by week"
   fig.update_layout(title_text='Distribution by Week')
   st.plotly_chart(fig, use_container_width=True)

def create_graph_for_day_analysis(df):
   features_time = ['Day_Name', 'Sentiment']
   df = df[features_time].groupby(features_time).size().reset_index(name='Count')

   # same that above
   fig = go.Figure()

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'POSITIVE']['Day_Name'], y=df[df['Sentiment'] == 'POSITIVE']['Count'], name='POSITIVE', opacity=0.5, marker_color = 'green'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'NEGATIVE']['Day_Name'], y=df[df['Sentiment'] == 'NEGATIVE']['Count'], name='NEGATIVE', opacity=0.5, marker_color = 'red'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'neutral']['Day_Name'], y=df[df['Sentiment'] == 'neutral']['Count'], name='NEUTRAL', opacity=0.5, marker_color = 'lightblue'))

   fig.update_layout(xaxis_tickangle=-45)

   # stack the bars
   fig.update_layout(barmode='stack')
   # set title to "Distribution by day"
   fig.update_layout(title_text='Distribution by Day')

   # order the day of the week from monday to sunday
   order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
   fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray':order})
   st.plotly_chart(fig, use_container_width=True)

def create_graph_for_hour_analysis(df):
   features_time = ['Day_Part', 'Sentiment']
   df = df[features_time].groupby(features_time).size().reset_index(name='Count')

   # same that above
   fig = go.Figure()

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'POSITIVE']['Day_Part'], y=df[df['Sentiment'] == 'POSITIVE']['Count'], name='POSITIVE', opacity=0.5, marker_color = 'green'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'NEGATIVE']['Day_Part'], y=df[df['Sentiment'] == 'NEGATIVE']['Count'], name='NEGATIVE', opacity=0.5, marker_color = 'red'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'neutral']['Day_Part'], y=df[df['Sentiment'] == 'neutral']['Count'], name='NEUTRAL', opacity=0.5, marker_color = 'lightblue'))

   fig.update_layout(xaxis_tickangle=-45)

   # stack the bars
   fig.update_layout(barmode='stack')

   # order the day of the week from breakfast, lunch, dinner, night
   order = ['Breakfast', 'Lunch', 'Dinner', 'Late Night', 'Not Specified']
   fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray':order})
   # set title to "Distribution by day"
   fig.update_layout(title_text='Distribution by Day Part')
   st.plotly_chart(fig, use_container_width=True)

def create_graph_for_month_analysis(df):
   features_time = ['Month_Year', 'Sentiment']
   df = df[features_time].groupby(features_time).size().reset_index(name='Count')

   # same that above
   fig = go.Figure()
   # set title to "Distribution by month"
   fig.update_layout(title_text='Distribution by Month')

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'POSITIVE']['Month_Year'], y=df[df['Sentiment'] == 'POSITIVE']['Count'], name='POSITIVE', opacity=0.5, marker_color = 'green'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'NEGATIVE']['Month_Year'], y=df[df['Sentiment'] == 'NEGATIVE']['Count'], name='NEGATIVE', opacity=0.5, marker_color = 'red'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'neutral']['Month_Year'], y=df[df['Sentiment'] == 'neutral']['Count'], name='NEUTRAL', opacity=0.5, marker_color = 'lightblue'))

   fig.update_layout(xaxis_tickangle=-45)

   # stack the bars

   fig.update_layout(barmode='stack')

   st.plotly_chart(fig, use_container_width=True)

def create_graph_for_source_analysis(df):
   # count the source considering the sentiment as well
   features_time = ['Source', 'Sentiment']
   df = df[features_time].groupby(features_time).size().reset_index(name='Count')
   
   # same that above
   fig = go.Figure()

   # add the bars

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'POSITIVE']['Source'], y=df[df['Sentiment'] == 'POSITIVE']['Count'], name='POSITIVE', opacity=0.5, marker_color = 'green'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'NEGATIVE']['Source'], y=df[df['Sentiment'] == 'NEGATIVE']['Count'], name='NEGATIVE', opacity=0.5, marker_color = 'red'))

   fig.add_trace(go.Bar(x=df[df['Sentiment'] == 'neutral']['Source'], y=df[df['Sentiment'] == 'neutral']['Count'], name='NEUTRAL', opacity=0.5, marker_color = 'lightblue'))

   fig.update_layout(xaxis_tickangle=-45)

   # stack the bars
   fig.update_layout(barmode='stack')

   # set title to "Distribution by source"
   fig.update_layout(title_text='Distribution by Source')

   st.plotly_chart(fig, use_container_width=True)

def create_pie_chart_completion(data_frame, container_totals):
   '''
   This graph is used inside the sections: Product, Service, Ambience
   
   param: data_frame: the data frame with the data
   param: container_totals: the container where the graph will be placed

   return: the graph
   '''
   tot_rows = len(data_frame)
   tot_row_with_label = len(data_frame[data_frame['label'].str.len() > 0])
   message = f"{tot_row_with_label}/{tot_rows}"

   fig_completion_pie = go.Figure()
   fig_completion_pie.add_trace(go.Pie(labels=['Completed', 'Not Completed'], values=[tot_row_with_label, tot_rows-tot_row_with_label], hole=.3, opacity=0.8))
   fig_completion_pie.update_layout(title_text='Completion' + ' ' + message)
   # if value is betweeen 0 and 0.4 then set to red
   fig_completion_pie.update_traces(marker=dict(colors=['#00cc96', '#EF553B']))

   # set size to very small
   fig_completion_pie.update_layout(height=300, width=300)
   container_totals.plotly_chart(fig_completion_pie, use_container_width=True)

def create_chart_totals_labels(data_frame, container_totals):
   '''
   This graph is used inside the sections: Product, Service, Ambience

   param: data_frame: the data frame with the data
   param: container_totals: the container where the graph will be placed

   return: the graph

   ---

   It creates a graph with the totals of the labels used in the column `label`
   in our case the labels are the categories of the feedback form (e.g. Food, Service, Ambience)

   '''

   # 7. create a new graph with all the labels and their counts
   labels = data_frame['Label_Dishoom'].tolist()
   #st.write('This is the labels: {}'.format(labels))
   labels = [clean_label(l) for l in labels]
   #st.write('This is the labels after clean: {}'.format(labels))
   labels = [item for sublist in labels for item in sublist]
   labels = [l for l in labels if l != '']
   labels = Counter(labels)
   labels = pd.DataFrame(labels.items(), columns=['Label_Dishoom', 'count'])

   fig = go.Figure()
   fig.add_trace(go.Bar(x=labels['Label_Dishoom'], y=labels['count'], opacity=0.8))
   fig.update_layout(xaxis={'categoryorder':'total descending'})
   # set color to index
   fig.update_traces(marker_color= '#01cc96')
   # set ticks to 45 degrees
   fig.update_xaxes(tickangle=45)
   fig.update_layout(title_text='Labels')
   # take off the axis and write the number of labels
   fig.update_layout(showlegend=False)
   # no axis
   fig.update_yaxes(showticklabels=False)
   fig.update_traces(text=labels['count'])
   st.plotly_chart(fig, use_container_width=True)

def create_chart_totals_food_and_drinks(data_frame : pd.DataFrame,
                                         container_plot : st.container,
                                           other_df : pd.DataFrame):
   '''
   This graph is used inside the section: Food and Drinks

   param: data_frame: the data frame with the data
   param: container_plot: the container where the graph will be placed
   param: other_df: the data frame with the data from the other section

   return: the graph

   ---

   It creates a graph with the totals of the food and drinks used in the column `food` and `drink_item`
   in our case the labels are the categories of the feedback form (e.g. Food, Service, Ambience)

   '''
   # get the food
   food = data_frame['menu_item'].tolist()
   # split at the -
   if len(food) > 0:
      food = [item.split('-') for item in food if item != '']
      food = [item for sublist in food for item in sublist]
      food = [f for f in food if f != '']
   else:
      food = []
   # flatten the list
   
   food = Counter(food)
   food = pd.DataFrame(food.items(), columns=['menu_item', 'count'])

   # get the drinks
   drinks = other_df['drink_item'].tolist()
   # split at the - 
   drinks = [item.split('-') for item in drinks]
   # flatten the list
   drinks = [item for sublist in drinks for item in sublist]
   # take off the spaces
   drinks = [item.strip() for item in drinks]
   # take off the empty strings
   drinks = [item for item in drinks if item != '']
   drinks = Counter(drinks)
   drinks = pd.DataFrame(drinks.items(), columns=['drink_item', 'count'])


   # create the food graph
   fig_food = go.Figure()
   fig_food.add_trace(go.Bar(x=food['menu_item'], y=food['count'], opacity=0.8))
   fig_food.update_layout(title_text='Food', 
                          xaxis={'categoryorder':'total descending'})
   fig_food.update_traces(text=food['count'],
                          marker_color='lightsalmon')
   fig_food.update_yaxes(showticklabels=False)
   fig_food.update_xaxes(tickangle=45)

   # create the drinks graph
   fig_drink = go.Figure()
   fig_drink.add_trace(go.Bar(x=drinks['drink_item'], y=drinks['count'], opacity=0.8))
   fig_drink.update_layout(title_text='Drinks', 
                           xaxis={'categoryorder':'total descending'})
   fig_drink.update_traces(text=drinks['count'], marker_color= 'lightblue')
   fig_drink.update_yaxes(showticklabels=False)
   fig_drink.update_xaxes(tickangle=45)

   # show the graphs
   container_plot.plotly_chart(fig_food, use_container_width=True)
   container_plot.plotly_chart(fig_drink, use_container_width=True)

def creating_keywords_graphs(key_words_list, df, container):
  
    df = df.copy()
    df['keywords'] = df['keywords'].str.split('-')
    df = df.explode('keywords').dropna(subset=['keywords', 'sentiment'])
    df['keywords'] = df['keywords'].str.strip()

    final_df = df[df['keywords'].isin(key_words_list)]
    final_df = final_df.groupby(['keywords', 'sentiment']).size().reset_index(name='count')

    color = {'POSITIVE': 'green', 'NEGATIVE': 'red', 'neutral': 'lightblue'}
    fig = px.bar(final_df, x="keywords", y="count", color='sentiment', 
                 barmode='stack', color_discrete_map=color, opacity=0.5)
    
    # set color for each sentiment
    # sort by count ascending
    fig.update_layout(
        title="Keywords",
        xaxis_title="Keywords",
        yaxis_title="Count",
        legend_title="Sentiment",
        xaxis_tickangle=-45)
    
    # take off the legend
    fig.update_layout(showlegend=False)

    container.plotly_chart(fig, use_container_width=True)
