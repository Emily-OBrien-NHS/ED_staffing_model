import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from ED_staffing_model import default_params, ED_staffing_model, run_the_model
import time
from itertools import product
#c:\Users\obriene2\venvs\.streamlit_venv\Scripts\Activate.ps1 
#streamlit run Streamlit_ED_staffing_model.py

####################################################################CONFIG
st.set_page_config(page_title="ED Staffing Model",
                   page_icon="🏥",
                   layout="wide",
                   initial_sidebar_state="expanded",
                   menu_items={'About': "Model to simulate ED staffing levels"})

st.title('ED Staffing Model')
st.write('''Go through each section to upload required files and adjust model settings.  Hit run
         at the bottom once happy with settings to run the ED staffing model.''')

####################################################################UPLOAD DEMAND FILE
st.divider()
st.markdown('## Current ED Demand')
st.write('Please upload the recent ED demand csv file')
demand = None
col1, col2 = st.columns(2)

with col1:
    valid_file = False
    demand_file = st.file_uploader("Choose a file", width=500)
    if demand_file is not None:
        try:
            demand = pd.read_csv(demand_file, date_format='%d/%m/%Y')
            if  not ({'Location', 'Dt', 'Hr', 'Arrivals'}.issubset(set(demand.columns))):
                st.error("Please upload the correct input file")
            else:
                valid_file = True
        except Exception as e:
            st.error(f'Invalid file: {e}')

########Manipulate demand file
#Get all dates and hours to account for hours were 0 attend.
if valid_file:
    all_vals = pd.DataFrame(product(demand['Location'].drop_duplicates(),
                                        demand['Dt'].drop_duplicates(),
                                        demand['Hr'].drop_duplicates()),
                                columns=['Location', 'Dt', 'Hr'])
    all_vals['wkdy'] = pd.to_datetime(all_vals['Dt'], format='%d/%m/%Y').dt.dayofweek

    demand = all_vals.merge(demand, on=['Location', 'Dt', 'Hr'], how='outer').fillna(0)

    #Group up to get average arrivals per hour by location, pivot into usable format.
    demand = demand.groupby(['Location', 'wkdy', 'Hr'], as_index=False)['Arrivals'].mean()
    amb_demand = demand.loc[demand['Location'] == 'Ambulatory'].pivot(index='Hr', columns='wkdy', values='Arrivals')
    maj_demand = demand.loc[demand['Location'] == 'Majors'].pivot(index='Hr', columns='wkdy', values='Arrivals')
    res_demand = demand.loc[demand['Location'] == 'Resus'].pivot(index='Hr', columns='wkdy', values='Arrivals')
    pae_demand = demand.loc[demand['Location'] == 'Paeds'].pivot(index='Hr', columns='wkdy', values='Arrivals')

    with col2:
        st.write('Average daily ED arrivals by area in the uploaded file:')
        st.dataframe(demand.groupby(['Location', 'wkdy'], as_index=False)['Arrivals'].sum()
                    .groupby('Location')['Arrivals'].mean().round(), width='content')


####################################################################CAPACITIES, TIMINGS AND STREAMING
st.divider()
st.markdown('## Capacy and Timings')

#Run Time
st.write('###### Set number of days to run the model for:')
run_time = st.number_input('Simulation run time (days)', min_value=1, max_value=730, step=1,
                               value=365)

st.write('###### Set average task durations and location capacities')

#Set title for each area
cols = st.columns(4)
for col, area in zip(cols, ['Ambulatory', 'Majors', 'Resus', 'Paeds']):
    with col:
        st.markdown(f'### {area}')

#Add timings sliders for each area under one header
st.markdown('#### Timings')
amb, maj, res, pae = st.columns(4)
with amb:
    amb_time_triage = st.slider('Ambulatory Triage Time (mins)', min_value=1, max_value=180, step=1,
                                value=default_params.amb_time_triage)
    amb_time_assess = st.slider('Ambulatory Assessment Time (mins)', min_value=1, max_value=240, step=1, 
                                value=default_params.amb_time_assess)
    amb_time_wait = st.slider('Ambulatory Wait for Spec Time (mins)', min_value=1, max_value=240, step=1,
                              value=default_params.amb_time_wait)
    amb_time_decisi = st.slider('Ambulatory Decision Time (mins)', min_value=1, max_value=240, step=1,
                                value=default_params.amb_time_decisi)
with maj:
    maj_time_triage = st.slider('Majors Triage Time (mins)', min_value=1, max_value=180, step=1,
                                value=default_params.maj_time_triage)
    maj_time_assess = st.slider('Majors Assessment Time (mins)', min_value=1, max_value=240, step=1, 
                                value=default_params.maj_time_assess)
    maj_time_wait = st.slider('Majors Wait for Spec Time (mins)', min_value=1, max_value=240, step=1,
                              value=default_params.maj_time_wait)
    maj_time_decisi = st.slider('Majors Decision Time (mins)', min_value=1, max_value=240, step=1,
                                value=default_params.maj_time_decisi)
with res:
    res_time_assess = st.slider('Resus Assessment Time (mins)', min_value=1, max_value=300, step=1, 
                                value=default_params.res_time_assess)
    res_time_wait = st.slider('Resus Wait for Spec Time (mins)', min_value=1, max_value=240, step=1,
                              value=default_params.res_time_wait)
    res_time_decisi = st.slider('Resus Decision Time (mins)', min_value=1, max_value=240, step=1,
                                value=default_params.res_time_decisi)
with pae:
    pae_time_assess = st.slider('Paeds Assessment Time (mins)', min_value=1, max_value=240, step=1, 
                                value=default_params.pae_time_assess)
    pae_time_wait = st.slider('Paeds Wait for Spec Time (mins)', min_value=1, max_value=240, step=1,
                              value=default_params.pae_time_wait)
    pae_time_decisi = st.slider('Paeds Decision Time (mins)', min_value=1, max_value=240, step=1,
                                value=default_params.pae_time_decisi)

#Add capacity sliders for each area under one header
st.markdown('#### Capacities')
amb, maj, res, pae = st.columns(4)
with amb:
    amb_triage_cap = st.slider('Ambulatory Triage Capacity', min_value=1, max_value=15, step=1,
                               value=default_params.amb_triage_cap)
    amb_assess_cap = st.slider('Ambulatory Assessment Cubicles', min_value=1, max_value=50, step=1,
                               value=default_params.amb_assess_cap)
with maj:
    maj_triage_cap = st.slider('Majors Triage Capacity', min_value=1, max_value=15, step=1,
                               value=default_params.maj_triage_cap)
    maj_assess_cap = st.slider('Majors Assessment Cubicles', min_value=1, max_value=50, step=1,
                               value=default_params.maj_assess_cap)
with res:
    res_assess_cap = st.slider('Resus Assessment Cubicles', min_value=1, max_value=50, step=1,
                               value=default_params.res_assess_cap)
with pae:
    pae_assess_cap = st.slider('Paeds Assessment Cubicles', min_value=1, max_value=50, step=1,
                               value=default_params.pae_assess_cap)
    
#Add sliders for streaming percentage in each area under one header
st.markdown('#### Streaming')
amb, maj, res, pae = st.columns(4)
with amb:
    amb_stream = st.slider('Ambulatory Streaming', min_value=0.0, max_value=1.0, step=0.01, value=0.16)
with maj:
    maj_stream = st.slider('Resus Streaming', min_value=0.0, max_value=1.0, step=0.01, value=0.09)
with res:
    res_stream = st.slider('Resus Streaming', min_value=0.0, max_value=1.0, step=0.01, value=0.03)
with pae:
    pae_stream = st.slider('Paeds Streaming', min_value=0.0, max_value=1.0, step=0.01, value=0.09)

stream = {'Ambulatory':amb_stream, 'Majors':maj_stream, 'Resus':res_stream, 'Paeds':pae_stream}
####################################################################STAFFING LEVELS
st.divider()
st.markdown('## Staffing Levels')

#User can chose between inputting a staff rota, or running with infinite staff.
model_choice = st.radio('Run the model with pre-defined staffing rota? Or run to get optimum staffing?',
                        ['Define Staffing', 'Optimum Staffing'], horizontal=True)

if model_choice == 'Define Staffing':
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('### Weekday')
        #Other parameters (staff levels, staff usage)
        wkdy_staff_df = pd.DataFrame({'Total Consultants':[1, 1, 0, 0, 0, 0, 0, 0, 2, 2, 2, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 2, 2],
                                    'Total Middle Tier'  :[6, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6],
                                    'Total Residents'    :[6, 6, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 6, 6]},
                                    index=np.linspace(0, 23, 24))
        wkdy_staff_df.index.name = 'Hour'
        wkdy_staff = st.data_editor(wkdy_staff_df)
        st.line_chart(wkdy_staff)

    with col2:
        st.markdown('### Weekend')
        #Other parameters (staff levels, staff usage)
        wknd_staff_df = pd.DataFrame({'Total Consultants':[1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2, 2, 3, 3, 2, 2, 2, 2, 2, 2, 1, 1],
                                    'Total Middle Tier':[4, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4],
                                    'Total Residents'  :[6, 6, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 6, 6]},
                                    index=np.linspace(0, 23, 24))
        wknd_staff_df.index.name = 'Hour'
        wknd_staff = st.data_editor(wknd_staff_df)
        st.line_chart(wknd_staff)
else:
    wkdy_staff = pd.DataFrame({'Total Consultants':[np.inf]*24,
                                    'Total Middle Tier':[np.inf]*24,
                                    'Total Residents':[np.inf]*24},
                                    index=np.linspace(0, 23, 24))
    wknd_staff = pd.DataFrame({'Total Consultants':[np.inf]*24,
                                    'Total Middle Tier':[np.inf]*24,
                                    'Total Residents':[np.inf]*24},
                                    index=np.linspace(0, 23, 24))

####################################################################STAFFING REQUIREMENTS
st.divider()
st.markdown('## Staffing Requirements')

depts = ['Ambulatory', 'Majors', 'Resus', 'Paeds']
staff = ['Consultant', 'Middle Tier', 'Resident']
amb, maj, res, pae = st.tabs(depts)

def staff_requirements(dept, tab, desc_default):
    with tab:
        st.caption('Staff types who can perform tasks, in order of preference/priority')
        triage = st.multiselect(label=f'{dept} Triage', options=staff,
                                  default=['Consultant', 'Middle Tier'])
        ass_dec = st.multiselect(label=f'{dept} Assessment or Decision', options=staff,
                                  default=desc_default)
        area_dict = {'Triage':triage, 'Assessment or Descision':ass_dec}
    return area_dict
        
amb_staffing = staff_requirements('Ambulatory', amb, ['Middle Tier', 'Resident', 'Consultant'])
maj_staffing = staff_requirements('Majors', maj, ['Resident', 'Middle Tier', 'Consultant'])
res_staffing = staff_requirements('Resus', res, ['Middle Tier', 'Consultant', 'Resident'])
pae_staffing = staff_requirements('Paeds', pae, ['Middle Tier', 'Resident', 'Consultant'])

####################################################################SET PARAMETERS FOR MODEL
#Get the parameters in a usable format
args = default_params()
#update defaults to selections
args.run_name = 'streamlit'
args.run_time = run_time*60*24
args.amb_time_triage = amb_time_triage
args.amb_time_assess = amb_time_assess
args.amb_time_wait = amb_time_wait
args.amb_time_decisi = amb_time_decisi
args.maj_time_triage = maj_time_triage
args.maj_time_assess = maj_time_assess
args.maj_time_wait = maj_time_wait
args.maj_time_decisi = maj_time_decisi
args.res_time_assess = res_time_assess
args.res_time_wait = res_time_wait
args.res_time_decisi = res_time_decisi
args.pae_time_assess = pae_time_assess
args.pae_time_wait = pae_time_wait
args.pae_time_decisi = pae_time_decisi
args.amb_triage_cap = amb_triage_cap
args.amb_assess_cap = amb_assess_cap
args.maj_triage_cap = maj_triage_cap
args.maj_assess_cap = maj_assess_cap
args.res_assess_cap = res_assess_cap
args.pae_assess_cap = pae_assess_cap
args.wkdy_staff = wkdy_staff
args.wknd_staff = wknd_staff
args.amb_staffing = amb_staffing
args.maj_staffing = maj_staffing
args.res_staffing = res_staffing
args.pae_staffing = pae_staffing
args.demand = demand
args.stream = stream

####################################################################MODEL RESULTS
args.pat_res = []
args.occ_staff_res = []

def streamlit_results(pat, occ, run_time):
     #Add table of averages from simulation run
     st.subheader('Averages of the model run (time in minutes)')
     st.dataframe(pat.head(5))
     st.dataframe(occ.head(5))

####################################################################RUN THE MODEL
st.divider()
st.markdown('## Run the Model')

#Button to run simulation
if st.button('Run Model'):
    st.subheader('Model Progress:')
    with st.empty():
        t0 = time.time()
        with st.spinner('Simulating patient arrivals and discharges...'):
            pat, occ = run_the_model(args)
        t1 = time.time()
        #run_time = t1-t0
    st.success('Done!')
    streamlit_results(pat, occ, run_time)
