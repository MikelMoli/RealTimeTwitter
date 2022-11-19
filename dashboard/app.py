import os
import sys
import time
sys.path.append('./')
sys.path.append('./src')

import yaml
import psycopg2
from database_controller import DatabaseController

import plotly.express as px
import streamlit as st

from settings import DATABASE_CONNECTION

# TODO: Separate creation functions and populate functions

class Dashboard:

    def __init__(self):
        self._db_controller = DatabaseController()



    def _create_first_row_kpis(self):
        r1_c1, r1_c2, r1_c3 = st.columns(3)
        kpi_values = self._db_controller._get_kpi_values()
        self.pos_count_kpi = r1_c1.empty()
        self.neu_count_kpi = r1_c2.empty()
        self.neg_count_kpi = r1_c3.empty()

    def _create_first_row_tabs(self):
        c = st.container()
        whitespace = 30
        tab_names = ['Positive Score Distribution', 
                     'Neutral Score Distribution', 
                     'Negative Score Distribution']
        with c:

            t1, t2, t3 = st.tabs([s.center(whitespace,"\u2001") for s in tab_names])
                            
            with t1:
                df = px.data.tips()
                t1_fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",
                   hover_data=df.columns)
                t1.plotly_chart(t1_fig)

            with t2:
                df = px.data.tips()
                t2_fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",
                   hover_data=df.columns)
                t2.plotly_chart(t2_fig)

            with t3:
                df = px.data.tips()
                t3_fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",
                   hover_data=df.columns)
                t3.plotly_chart(t3_fig)
          
    def _create_second_row_kpis(self):
        _, c, _ =  st.columns(3)
        with c:
            st.metric(label='Nº of tweets with Hate Speech', value=225)

    def _create_second_row_wordcloud_and_user_list(self):
        c1, c2 = st.columns([3, 1])
        with c1:
            df = px.data.tips()
            t3_fig = px.histogram(df, x="total_bill", y="tip", color="sex", marginal="rug",
                                hover_data=df.columns)
            c1.plotly_chart(t3_fig) 
        
        with c2:
            df = px.data.tips()
            st.dataframe(df[['total_bill']])

    def real_time_data_population(self):
        
        kpi_values = self._db_controller._get_kpi_values()

        with self.pos_count_kpi:
            st.metric("Positive Count", value=kpi_values['POS'])
        
        with self.neu_count_kpi:
            st.metric(label='Neutral Count', value=kpi_values['NEU'])

        with self.neg_count_kpi:
            st.metric(label='Neutral Count', value=kpi_values['NEG']) 

    def run(self):
        st.set_page_config(layout="wide")
        self._create_first_row_kpis()
        self._create_first_row_tabs()

        self._create_second_row_kpis()
        self._create_second_row_wordcloud_and_user_list()
        
        while True:
            start_time = time.time()
            ref_time = time.time()
            while (ref_time - start_time) > 0.5: # half a second
                ref_time = time.time()

            self.real_time_data_population()
        


if __name__ == '__main__':
    d = Dashboard()
    d.run()
