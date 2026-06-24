import streamlit as st
import pandas as pd
import numpy as np
import io
import base64
import xlsxwriter
from utils.rota_generation import optimal_rota
from utils.plots import hour_results_plots, day_results_plots
import math
from ED_staffing_model import default_params, ED_staffing_model, run_the_model
from itertools import product
#c:\Users\obriene2\venvs\.streamlit_venv\Scripts\Activate.ps1
#streamlit run Streamlit_ED_staffing_model.py

###################################################################################################
                                        ####PAGE CONFIG####
###################################################################################################
st.set_page_config(page_title="ED Staffing Model",
                   page_icon="🏥",
                   layout="wide",
                   initial_sidebar_state="expanded",
                   menu_items={'About': "Model to simulate ED staffing levels"})

st.title('ED Staffing Model')
st.markdown('## Model instructions')
st.write('Model runs based on the below image/flow diagram.  Please go through each input '\
            'tab to upload current demand and check/amend any input parameters before running the '\
            'model.')
st.image('images/1 - Model Flow.png')

###################################################################################################
                                     ####INPUT PARAMETERS####
###################################################################################################

st.markdown('# Input Parameters')
input_tabs = st.tabs(['Current ED Demand', 'Capacity and Timings', 'Staffing Levels', 'Staffing Requirements'])

####################################################################UPLOAD DEMAND FILE
with input_tabs[0]:
    st.markdown('## Current ED Demand')
    demand = None
    amb_demand = None
    maj_demand = None
    res_demand = None
    pae_demand = None
    col1, col2, col3 = st.columns(3)
    #ED Demand CSV upload
    with col1:
        st.write('Please upload the recent ED demand file')
        valid_demand_file = False
        demand_file = st.file_uploader("Choose an excel file", width=500, key='demand')
        if demand_file is not None:
            try:
                demand = pd.read_excel(demand_file, date_format='%d/%m/%Y')
                if  not ({'Location', 'Dt', 'Hr', 'Arrivals'}.issubset(set(demand.columns))):
                    st.error("Please upload the correct input file")
                else:
                    valid_demand_file = True
            except Exception as e:
                st.error(f'Invalid file: {e}')

    ########Manipulate demand file
    #Get all dates and hours to account for hours were 0 attend.
    if valid_demand_file:
        all_vals = pd.DataFrame(product(demand['Location'].drop_duplicates(),
                                        demand['Dt'].drop_duplicates(),
                                        demand['Hr'].drop_duplicates()),
                                    columns=['Location', 'Dt', 'Hr'])
        all_vals['wkdy'] = pd.to_datetime(all_vals['Dt'], dayfirst=True).dt.dayofweek

        demand = all_vals.merge(demand, on=['Location', 'Dt', 'Hr'], how='outer').fillna(0)

        #Group up to get average arrivals per hour by location, pivot into usable format.
        demand = demand.groupby(['Location', 'wkdy', 'Hr'], as_index=False)['Arrivals'].mean()
        amb_demand = demand.loc[demand['Location'] == 'Ambulatory'].pivot(index='Hr', columns='wkdy', values='Arrivals')
        maj_demand = demand.loc[demand['Location'] == 'Majors'].pivot(index='Hr', columns='wkdy', values='Arrivals')
        res_demand = demand.loc[demand['Location'] == 'Resus'].pivot(index='Hr', columns='wkdy', values='Arrivals')
        pae_demand = demand.loc[demand['Location'] == 'Paeds'].pivot(index='Hr', columns='wkdy', values='Arrivals')

        inp_demand = (demand.groupby(['Location', 'wkdy'], as_index=False)['Arrivals'].sum()
                    .groupby('Location')['Arrivals'].mean().round())
        ########Add plot of daily demand for visualisation
        with col2:
            st.pyplot(demand.groupby(['Location', 'Hr'], as_index=False)['Arrivals'].mean()
                            .pivot(index='Hr', columns='Location', values='Arrivals')
                            .plot(title='Hourly Arrivals in File').figure)    
        ########Add summary of average arrivals per day
        with col3:
            st.write('Average daily ED arrivals by area in the uploaded file:')
            st.dataframe(inp_demand, width='content')


##################################################################CAPACITIES, TIMINGS AND STREAMING
with input_tabs[1]:
    st.markdown('## Capacy and Timings')

    #Run Time
    st.write('###### Set number of days to run the model for:')
    run_time = st.number_input('Simulation run time (days)', min_value=1, max_value=730, step=1,
                                value=120)

    st.write('###### Set average task durations and location capacities:')
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
        amb_stream = st.slider('Ambulatory Streaming (%)', min_value=0, max_value=100, step=1,
                            value=16) / 100
    with maj:
        maj_stream = st.slider('Resus Streaming (%)', min_value=0, max_value=100, step=1,
                            value=9) / 100
    with res:
        res_stream = st.slider('Resus Streaming (%)', min_value=0, max_value=100, step=1,
                            value=3) / 100
    with pae:
        pae_stream = st.slider('Paeds Streaming (%)', min_value=0, max_value=100, step=1,
                            value=9) / 100

    stream = {'Ambulatory':amb_stream, 'Majors':maj_stream, 'Resus':res_stream, 'Paeds':pae_stream}

####################################################################STAFFING LEVELS
with input_tabs[2]:
    st.markdown('## Staffing Levels')
    wkdy_staff = pd.DataFrame()
    wknd_staff = pd.DataFrame()

    #User can chose between inputting a staff rota, or running with infinite staff.
    model_choice = st.radio('Run the model with pre-defined staffing rota? Or run to get optimum staffing?',
                            ['Define Staffing', 'Upload File', 'Optimum Staffing'], horizontal=True)

    if model_choice == 'Upload File':
        st.write('Please upload a staffing excel file')
        valid_file = False
        staffing_file = st.file_uploader("Choose an excel file", width=500, key='staff')
        if staffing_file is not None:
            try:
                wkdy_staff = pd.read_excel(staffing_file, index_col='Hour', sheet_name='Weekday', date_format='%d/%m/%Y')
                wknd_staff = pd.read_excel(staffing_file, index_col='Hour', sheet_name='Weekend', date_format='%d/%m/%Y')
                #verify correct file
                if  not ({'Total Consultants', 'Total Middle Tier', 'Total Residents'}.issubset(set(wkdy_staff.columns))):
                    st.error("Please upload the correct input file")
                else:
                    valid_file = True
            except Exception as e:
                st.error(f'Invalid file: {e}')
            #add plots of staffing
            col1, col2 = st.columns(2)
            with col1:
                st.markdown('### Weekday')
                st.line_chart(wkdy_staff)
            with col2:
                st.markdown('### Weekend')
                st.line_chart(wknd_staff)

    #If define staffing selected, show editable staffing dataframes
    elif model_choice == 'Define Staffing':
        col1, col2 = st.columns(2)
        with col1:
            st.markdown('### Weekday')
            wkdy_staff_df = pd.DataFrame({'Total Consultants':[1, 1, 0, 0, 0, 0, 0, 0, 2, 2, 2, 3, 3,
                                                            3, 4, 4, 4, 4, 4, 4, 4, 4, 2, 2],
                                        'Total Middle Tier':[6, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 5,
                                                            5, 5, 5, 5, 5, 5, 5, 5, 5, 6, 6],
                                        'Total Residents'  :[6, 6, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 6,
                                                            6, 6, 6, 6, 6, 5, 5, 5, 5, 6, 6]},
                                        index=np.linspace(0, 23, 24))
            wkdy_staff_df.index.name = 'Hour'
            wkdy_staff = st.data_editor(wkdy_staff_df)
            st.line_chart(wkdy_staff)

        with col2:
            st.markdown('### Weekend')
            #Other parameters (staff levels, staff usage)
            wknd_staff_df = pd.DataFrame({'Total Consultants':[1, 1, 0, 0, 0, 0, 0, 0, 1, 1, 2, 2, 2,
                                                            2, 3, 3, 2, 2, 2, 2, 2, 2, 1, 1],
                                        'Total Middle Tier':[4, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 5,
                                                            5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4],
                                        'Total Residents'  :[6, 6, 4, 4, 4, 4, 4, 4, 3, 3, 3, 3, 6,
                                                            6, 6, 6, 6, 6, 5, 5, 5, 5, 6, 6]},
                                        index=np.linspace(0, 23, 24))
            wknd_staff_df.index.name = 'Hour'
            wknd_staff = st.data_editor(wknd_staff_df)
            st.line_chart(wknd_staff)
    else:
        st.text('Model will run slower as will run twice.  First run will be with infinite staffing, '\
                'second run will use the average staffing levels produced by the infitine run to '\
                'produce results of ideal staffing.')
        wkdy_staff = pd.DataFrame({'Total Consultants':[np.inf]*24,
                                'Total Middle Tier':[np.inf]*24,
                                'Total Residents':[np.inf]*24},
                                index=np.linspace(0, 23, 24))
        wknd_staff = pd.DataFrame({'Total Consultants':[np.inf]*24,
                                'Total Middle Tier':[np.inf]*24,
                                'Total Residents':[np.inf]*24},
                                index=np.linspace(0, 23, 24))

####################################################################STAFFING REQUIREMENTS
with input_tabs[3]:
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
args.amb_demand = amb_demand
args.maj_demand = maj_demand
args.res_demand = res_demand
args.pae_demand = pae_demand
args.stream = stream
#results
args.pat_res = []
args.occ_staff_res = []


###################################################################################################
                                        ####RUN THE MODEL####
###################################################################################################
def av_staffing(logic):
    staff = (occ.loc[logic].groupby('Hour')[['Consultants', 'Middle Tier', 'Residents']].mean().apply(np.ceil).astype(int))
    staff.columns = ['Total Consultants', 'Total Middle Tier', 'Total Residents']
    return staff

st.divider()
st.markdown('# Run the Model')

#Button to run simulation
if st.button('Run Model'):
    #Quick valid file check
    if not valid_demand_file:
        st.warning('Please upload the ED demand file and check other inputs before running the model')
    else:
        st.subheader('Model Progress:')
        with st.empty():
            with st.spinner('Simulating patient arrivals and discharges...'):
                pat, occ = run_the_model(args)
                st.session_state['pat'] = pat
                st.session_state['occ'] = occ
                st.session_state['args'] = args
    ####################################################################OPTIMUM STAFFING OPTION
                #If running with optimum staffing, re-run with the average usage from that model
                if model_choice == 'Optimum Staffing':
                    with st.spinner('Re-running with optimum staffing levels...'):
                        #Get columns to work out average staffing usage in infinite model
                        occ['Day'] = (occ['Time'] / (24*60)).apply(math.floor)
                        occ['Hour'] = pd.Series(np.where((occ['Day']*(24*60)) != 0,
                                                occ['Time'] % (occ['Day']*(24*60)), occ['Time']) / 60
                                                ).apply(math.floor)
                        occ['Day of Week'] = occ['Day'] % 7
                        #Calculate average staffing usage of infinite model
                        args.wkdy_staff = av_staffing(occ['Day of Week'] < 5)
                        args.wknd_staff = av_staffing(occ['Day of Week'] >= 5)
                        #reset results lists
                        args.pat_res = []
                        args.occ_staff_res = []
                        #re-run the model
                        pat, occ = run_the_model(args)
                        st.session_state['pat'] = pat
                        st.session_state['occ'] = occ
                        st.session_state['args'] = args

###################################################################################################
                                            ####RESULTS####
###################################################################################################
if 'pat' in st.session_state:
    pat = st.session_state['pat']
    occ = st.session_state['occ']
    args = st.session_state['args']   
    #Display results
    st.success('Done!')
    st.divider()
    st.markdown('# Results')

###################################################################DOWNLOADABLE STAFFING EXCEL FILE
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    args.wkdy_staff.to_excel(writer, sheet_name="Weekday")
    args.wknd_staff.to_excel(writer, sheet_name="Weekend")
    writer.close()
    processed_data = output.getvalue()
    b64 = base64.b64encode(processed_data)
    download_link = f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="Staffing.xlsx">Download Staffing.xlsx</a>'
    st.markdown(download_link, unsafe_allow_html=True)

####################################################################RESULTS TABS
    res_tabs = st.tabs(['Summary Figures', 'Hour of Day Plots', 'Day of Week Plots'])

####################################################################SUMMARY NUMBERS
    with res_tabs[0]:
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

        st.dataframe(pat.groupby('Day')['Patient ID'].count() / (pat['Run'].max() + 1))

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

####################################################################PLOTS
    with res_tabs[1]:
        hour_results_plots(st.session_state['pat'], st.session_state['occ'])
    with res_tabs[2]:
        day_results_plots(st.session_state['pat'], st.session_state['occ'])

####################################################################OPTIMAL STAFFING ROTA
    st.markdown('## Optimal Shift Rota')
    shift_length = st.slider('Shift length (Hours):', min_value=4, max_value=16, value=8)
    if st.button('Calculate shifts'):
        if 'args' not in st.session_state:
            st.warning('Please run the model first')
        else:
            args = st.session_state['args']

            col1, col2, col3 = st.columns(3)

            with col1:
                st.subheader('Consultants')
            with col2:
                st.subheader('Middle Tier')
            with col3:
                st.subheader('Residents')

            st.markdown('### Weekday')
            col1, col2, col3 = st.columns(3)
            with col1:
                headline, outstr = optimal_rota(args.wkdy_staff['Total Consultants'].tolist(), shift_length)
                st.markdown(f'###### {headline}')
                st.text(outstr)
            with col2:
                headline, outstr = optimal_rota(args.wkdy_staff['Total Middle Tier'].tolist(), shift_length)
                st.markdown(f'###### {headline}')
                st.text(outstr)
            with col3:
                headline, outstr = optimal_rota(args.wkdy_staff['Total Residents'].tolist(), shift_length)
                st.markdown(f'###### {headline}')
                st.text(outstr)

            st.markdown('### Weekend')
            col1, col2, col3 = st.columns(3)
            with col1:
                headline, outstr = optimal_rota(args.wknd_staff['Total Consultants'].tolist(), shift_length)
                st.markdown(f'###### {headline}')
                st.text(outstr)
            with col2:
                headline, outstr = optimal_rota(args.wknd_staff['Total Middle Tier'].tolist(), shift_length)
                st.markdown(f'###### {headline}')
                st.text(outstr)
            with col3:
                headline, outstr = optimal_rota(args.wknd_staff['Total Residents'].tolist(), shift_length)
                st.markdown(f'###### {headline}')
                st.text(outstr)
