import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import math
from ED_staffing_model import default_params, ED_staffing_model, run_the_model
import time
from itertools import product
import matplotlib.pyplot as plt
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

    inp_demand = (demand.groupby(['Location', 'wkdy'], as_index=False)['Arrivals'].sum()
                  .groupby('Location')['Arrivals'].mean().round())
    with col2:
        st.write('Average daily ED arrivals by area in the uploaded file:')
        st.dataframe(inp_demand, width='content')


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
    amb_stream = st.slider('Ambulatory Streaming (%)', min_value=0, max_value=100, step=1, value=16) / 100
with maj:
    maj_stream = st.slider('Resus Streaming (%)', min_value=0, max_value=100, step=1, value=9) / 100
with res:
    res_stream = st.slider('Resus Streaming (%)', min_value=0, max_value=100, step=1, value=3) / 100
with pae:
    pae_stream = st.slider('Paeds Streaming (%)', min_value=0, max_value=100, step=1, value=9) / 100

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
    st.text('Model will run slower as will run twice.  First run will be with infinite staffing, '\
            'second run will use the average staffing levels produced by the infitine run to produce '\
            'results of ideal staffing.')
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
#results
args.pat_res = []
args.occ_staff_res = []

####################################################################MODEL RESULTS
def q25(x):
    return x.quantile(0.25)
def q75(x):
    return x.quantile(0.75)
quartile_label = '25-75 quartiles'

def hour_of_day_area_plot(agg_figures, hours):
        # #plot
        fig, ([ax1, ax2], [ax3, ax4]) = plt.subplots(2, 2, figsize=(18, 10), sharex=True)
        fig.suptitle('Arrivals by Hour of Day', fontsize=24)
        for ax, area in zip([ax1, ax2, ax3, ax4], ['Ambulatory', 'Majors', 'Resus', 'Paeds']):
            data = agg_figures.loc[area].copy()
            data = data.reset_index().merge(pd.DataFrame(hours), on='Arrival Hour', how='right').set_index('Arrival Hour').fillna(0)
            ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
            ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
            ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
            ax.set_title(area, fontsize=18)
            ax.tick_params(axis='both',  which='major', labelsize=18)
        plt.legend(fontsize=18)
        fig.supxlabel('Hour of Day', fontsize=18)
        fig.supylabel('Arrivals', fontsize=18)
        fig.tight_layout()
        st.pyplot(fig)



def streamlit_results(pat, occ, run_time):
    ################################################Summary Numbers
    #calculate additional columns
    pat['Day'] = (pat['Arrival'] / (24*60)).apply(math.floor)
    pat['LoS'] = pat['Leave'] - pat['Arrival']
    pat['not 4hr breach'] = np.where(pat['LoS'] < 240, True, False)

    occ['Day'] = (occ['Time'] / (24*60)).apply(math.floor)
    occ['Hour'] = (np.where((occ['Day']*(24*60)) != 0, occ['Time'] % (occ['Day']*(24*60)), occ['Time']) / 60)
    occ['Hour'] = occ['Hour'].apply(math.floor)
    occ['Day of Week'] = occ['Day'] % 7

    ####Calculate average arrivals per day
    out_demand = (pat.groupby(['Area', 'Day', 'Run'], as_index=False)['Patient ID'].count()
                  .groupby('Area')['Patient ID'].mean())
    #compare to inputted demand
    demand = pd.DataFrame(inp_demand).join(pd.DataFrame(out_demand)).round()
    demand.columns = ['Input', 'Output']
    #create check for if input and outted arrivals are different
    demand['warn'] = ((abs(demand['Output'] - demand['Input']) / demand['Input']) > 0.3)

    ####4hr performance
    #overall
    all_4hr = 100 * (pat['not 4hr breach'].sum() / pat['Patient ID'].count())
    #By area
    perf_4hr = pat.groupby('Area').agg({'not 4hr breach':'sum', 'Patient ID':'count'})
    perf_4hr['4hr'] = (100 * perf_4hr['not 4hr breach'] / perf_4hr['Patient ID']).round().astype(str) + '%'

    #####Los
    #overall
    all_los = pat['LoS'].mean()
    #area
    los_df = pat.groupby('Area')['LoS'].mean().round()

    #####Display in streamlit
    col1, col2, col3 = st.columns(3)
    #Arrivals
    with col1:
        st.subheader('Arrivals')
        st.text('Comparison between the average number of daily arrivals in the input csv file '\
                'against the number of daily arrivals recorded in the model output.  Significant '\
                'differences in the numbers here suggest that patients are getting stuck in the '\
                'model and never getting recorded at the end.''')
        st.dataframe(demand[['Input', 'Output']], width='content')
        #Warning if input and output arrivals different
        if demand['warn'].sum() > 0:
            areas = ', '.join(demand.loc[demand['warn']].index)
            st.error(f'Input and Output numbers are significantly different for {areas}.  This suggests a blockage in the process.') 

    #4 hr performance
    with col2:
        st.subheader('4 Hour Performance')
        st.markdown(f'##### Overall 4hr Performance: {all_4hr:.2f}%')
        st.dataframe(perf_4hr['4hr'].round(), width='content')
    
    #LoS
    with col3:
        st.subheader('Average Length of Stay (Mins)')
        st.markdown(f'##### Overall LoS: {all_los:.0f} mins')
        st.dataframe(los_df, width='content')

    ###########################################################Plots
    hours = pat['Arrival Hour'].drop_duplicates().sort_values()
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    staff_members = ['Consultants', 'Middle Tier', 'Residents']
    areas = ['Paeds', 'Ambulatory', 'Majors', 'Resus']

    ######################Hour of Day Plots
    st.divider()
    st.markdown('## Hour of Day Plots')
    st.text("Choose which plot you'd like to view:")
    tab_names = ['Arrivals', 'Staff Usage', 'Location Utilisation', 'Location Queues', '4hr Performance', 'LoS']
    plot_tabs = st.tabs(['Arrivals', 'Staff Usage', 'Location Utilisation', 'Location Queues', '4hr Performance', 'LoS'])
    
    #Arrivals plot
    with plot_tabs[0]:
        agg_figures = (pat.groupby(['Run', 'Area', 'Day', 'Arrival Hour'], as_index=False)['Patient ID'].count()
                        .groupby(['Area', 'Arrival Hour'])['Patient ID'].agg(['min', q25,'mean', q75, 'max']))
        hours = pat['Arrival Hour'].drop_duplicates().sort_values()
        hour_of_day_area_plot(agg_figures, hours)
    #Staff Usage
    with plot_tabs[1]:
        agg_figures = occ.groupby('Hour')[staff_members].agg(['min', q25,'mean', q75, 'max'])
        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 10), sharex=True, sharey=True)
        fig.suptitle(f'Staff Usage', fontsize=24)
        for ax, staff in zip([ax1, ax2, ax3], staff_members):
            data = agg_figures[staff].copy()
            ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
            ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
            ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
            ax.set_title(staff, fontsize=18)
            ax.tick_params(axis='both',  which='major', labelsize=18)
        plt.legend(fontsize=18)
        fig.supxlabel('Hour of Day', fontsize=18)
        fig.supylabel('Number in Use', fontsize=18)
        fig.tight_layout()
        st.pyplot(fig)

    #Location utilisation
    with plot_tabs[2]:
        loc_cols = ['Amb Triage Use', 'Amb Assessment Use', 'Res Assessment Use',
                    'Maj Triage Use', 'Maj Assessment Use',  'Pae Assessment Use']
        agg_figures = occ.groupby('Hour')[loc_cols].agg(['min', q25,'mean', q75, 'max'])
        fig, ([ax1, ax2, ax3], [ax4, ax5, ax6]) = plt.subplots(2, 3, figsize=(18, 10), sharex=True, sharey=True)
        fig.suptitle(f'Staff Usage', fontsize=24)
        for ax, col in zip([ax1, ax2, ax3, ax4, ax5, ax6], loc_cols):
            data = agg_figures[col].copy()
            ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
            ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
            ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
            ax.set_title(col, fontsize=18)
            ax.tick_params(axis='both',  which='major', labelsize=18)
        plt.legend(fontsize=18)
        fig.supxlabel('Hour of Day', fontsize=18)
        fig.supylabel('Average Queue Length', fontsize=18)
        fig.tight_layout()
        st.pyplot(fig)

    #location queues
    with plot_tabs[3]:
        queue_cols = ['Amb Triage Queue', 'Amb Assessment Queue', 'Res Assessment Queue',
                      'Maj Triage Queue', 'Maj Assessment Queue', 'Pae Assessment Queue']
        agg_figures = occ.groupby('Hour')[queue_cols].agg(['min', q25,'mean', q75, 'max'])

        fig, ([ax1, ax2, ax3], [ax4, ax5, ax6]) = plt.subplots(2, 3, figsize=(18, 10), sharex=True, sharey=True)
        fig.suptitle(f'Staff Usage', fontsize=24)
        for ax, col in zip([ax1, ax2, ax3, ax4, ax5, ax6], queue_cols):
            data = agg_figures[col].copy()
            ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
            ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
            ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
            ax.set_title(col, fontsize=18)
            ax.tick_params(axis='both',  which='major', labelsize=18)
        plt.legend(fontsize=18)
        fig.supxlabel('Hour of Day', fontsize=18)
        fig.supylabel('Average Queue Length', fontsize=18)
        fig.tight_layout()
        st.pyplot(fig)
    
    #4hr performance
    with plot_tabs[4]:
        agg_figures = pat.groupby(['Run', 'Area', 'Day', 'Arrival DoW', 'Arrival Hour'], as_index=False)['not 4hr breach'].agg(['sum', 'count'])
        agg_figures['4 hour performance'] = agg_figures['sum'] / agg_figures['count']
        agg_figures = agg_figures.groupby(['Area', 'Arrival Hour'])['4 hour performance'].agg(['min', q25,'mean', q75, 'max'])
        hours = pat['Arrival Hour'].drop_duplicates().sort_values()
        hour_of_day_area_plot(agg_figures, hours)

    #LoS
    with plot_tabs[5]:
        agg_figures = pat.groupby(['Area', 'Arrival Hour'])['LoS'].agg(['min', q25,'mean', q75, 'max'])
        hours = pat['Arrival Hour'].drop_duplicates().sort_values()
        hour_of_day_area_plot(agg_figures, hours)


    #################################################Day of week plots
    st.divider()
    st.markdown('## Day of Week Plots')
    st.text("Choose which plot you'd like to view:")
    #Some of these will be by area by Dow, how are we going to do that?
    tab_names = ['Arrivals', 'Staff Usage', 'Location Utilisation', 'Location Queues', '4hr Performance', 'LoS']
    plot_tabs = st.tabs(['Arrivals', 'Staff Usage', 'Location Utilisation', 'Location Queues', '4hr Performance', 'LoS'])
    
    #output tables for data visualisation while building
    st.divider()
    st.subheader('TABLES')
    st.dataframe(pat.head(5))
    st.dataframe(occ.head(5))


####################################################################RUN THE MODEL
st.divider()
st.markdown('## Run the Model')

#Button to run simulation
if st.button('Run Model'):
    st.subheader('Model Progress:')
    with st.empty():
        with st.spinner('Simulating patient arrivals and discharges...'):
            pat, occ = run_the_model(args)
            #If running with optimum staffing, re-run with the average usage from that model
            if model_choice == 'Optimum Staffing':
                with st.spinner('Re-running with optimum staffing levels...'):
                    #Get columns to work out average staffing usage in infinite model
                    occ['Day'] = (occ['Time'] / (24*60)).apply(math.floor)
                    occ['Hour'] = (np.where((occ['Day']*(24*60)) != 0, occ['Time'] % (occ['Day']*(24*60)), occ['Time']) / 60)
                    occ['Hour'] = occ['Hour'].apply(math.floor)
                    occ['Day of Week'] = occ['Day'] % 7
                    staff_members = ['Consultants', 'Middle Tier', 'Residents']
                    cols = ['Total Consultants', 'Total Middle Tier', 'Total Residents']
                    #Calculate average staffing usage
                    wkdy_staff = (occ.loc[occ['Day of Week'] < 5].groupby('Hour')[staff_members]
                                  .mean().apply(np.ceil).astype(int))
                    wknd_staff = (occ.loc[occ['Day of Week'] >= 5].groupby('Hour')[staff_members]
                                  .mean().apply(np.ceil).astype(int))
                    wkdy_staff.columns = cols
                    wknd_staff.columns = cols
                    args.wkdy_staff = wkdy_staff
                    args.wknd_staff = wknd_staff
                    #reset results lists
                    args.pat_res = []
                    args.occ_staff_res = []
                    #re-run the model
                    pat, occ = run_the_model(args)
    st.success('Done!')
    st.divider()
    st.markdown('## Results')
    streamlit_results(pat, occ, run_time)
