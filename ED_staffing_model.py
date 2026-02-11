import os
import simpy
import random
import math
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from itertools import product
import matplotlib.pyplot as plt

class default_params():
    ########cl3 Engine
    cl3_engine = create_engine('mssql+pyodbc://@cl3-data/DataWarehouse?'\
                           'trusted_connection=yes&driver=ODBC+Driver+17'\
                               '+for+SQL+Server')
    ########General Params
    run_name = 'ED Staffing Model'
    #run times and iterations
    run_time = 24*60*365#525600
    run_days = int(run_time/(60*24))
    iterations = 5
    sample_time = 15

    ###################EVENTS
    ########Mean Event Timings (_time_) and Capacities (_cap)
    #Ambulatory
    amb_time_triage = 7
    amb_time_assess = 50
    amb_time_wait = 60
    amb_time_decisi = 15
    amb_triage_cap = 2
    amb_assess_cap = 23
    #Majors
    maj_time_triage = 10
    maj_time_assess = 90
    maj_time_wait = 60
    maj_time_decisi = 25
    maj_triage_cap = 2
    maj_assess_cap = 18
    #Resus
    res_time_assess = 120
    res_time_wait = 60
    res_time_decisi = 50
    res_assess_cap = 7
    #Paeds
    pae_time_assess = 50
    pae_time_wait = 60
    pae_time_decisi = 15
    pae_assess_cap = 10
    #Streaming
    stream_sql = """SELECT
                    CASE WHEN LocationDescription LIKE '%Paed%' THEN 'Paeds'
                        WHEN LocationDescription LIKE '%Ambulatory%' THEN 'Ambulatory'
                        WHEN LocationDescription LIKE '%Majors%' THEN 'Majors'
                        WHEN LocationDescription LIKE '%Resus%' THEN 'Resus'
                        END AS [Location],
                    (SUM(CASE WHEN DischargeStatusDescription LIKE 'Streamed to%' THEN 1 ELSE 0 END)*1.0
                        / COUNT(att.NCAttendanceId)) AS [Streamed]
                    FROM DataWarehouse.ed.vw_EDAttendanceLocationHistory loc 
                    INNER JOIN Datawarehouse.ed.vw_EDAttendance att
                    ON att.ncattendanceId = loc.NCAttendanceId
                    WHERE LocationOrder = 1 AND ArrivalDateTime >= DATEADD(MONTH, -3, GETDATE())
                    GROUP BY CASE WHEN LocationDescription LIKE '%Paed%' THEN 'Paeds'
                        WHEN LocationDescription LIKE '%Ambulatory%' THEN 'Ambulatory'
                        WHEN LocationDescription LIKE '%Majors%' THEN 'Majors'
                        WHEN LocationDescription LIKE '%Resus%' THEN 'Resus'
                        END"""
    stream = dict(pd.read_sql(stream_sql, cl3_engine).values)
    
    ###################STAFFING
    ########Staffing Numbers
    no_consultants = np.inf
    no_middle_tier = np.inf
    no_resident = np.inf

    ########Staffing Requirements
    #staff appear in order of preference and priority
    triage_ordering = ['Consultant', 'Middle Tier']
    amb_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['Middle Tier', 'Resident', 'Consultant']}
    maj_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['Resident', 'Middle Tier', 'Consultant']}
    res_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['Middle Tier', 'Consultant', 'Resident']}
    pae_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['Middle Tier', 'Resident', 'Consultant']}
    
    ###################DEMAND
    #Need to pull in current demand and work out the average number of arrivals
    #to each location by hour of the day.
    demand_sql = """SELECT
                    CASE WHEN LocationDescription LIKE '%Paed%' THEN 'Paeds'
                        WHEN LocationDescription LIKE '%Ambulatory%' THEN 'Ambulatory'
                        WHEN LocationDescription LIKE '%Majors%' THEN 'Majors'
                        WHEN LocationDescription LIKE '%Resus%' THEN 'Resus'
                        END AS [Location],
                    CONVERT(DATE, ArrivalDateTime) AS Dt,
                    DATEPART(HOUR, ArrivalDateTime) AS Hr,
                    COUNT(ArrivalDateTime) AS Arrivals
                    FROM DataWarehouse.ed.vw_EDAttendanceLocationHistory loc 
                    INNER JOIN Datawarehouse.ed.vw_EDAttendance att
                    ON att.ncattendanceId = loc.NCAttendanceId
                    WHERE LocationOrder = 1
                        AND ArrivalDateTime >= DATEADD(MONTH, -3, GETDATE())
                    GROUP BY
                    CASE WHEN LocationDescription LIKE '%Paed%' THEN 'Paeds'
                        WHEN LocationDescription LIKE '%Ambulatory%' THEN 'Ambulatory'
                        WHEN LocationDescription LIKE '%Majors%' THEN 'Majors'
                        WHEN LocationDescription LIKE '%Resus%' THEN 'Resus' END, 
                    CONVERT(DATE, ArrivalDateTime),
                    DATEPART(HOUR, ArrivalDateTime) """
    demand = pd.read_sql(demand_sql, cl3_engine)
    #Get all dates and hours to account for hours were 0 attend.
    all_vals = pd.DataFrame(product(demand['Location'].drop_duplicates(),
                                    demand['Dt'].drop_duplicates(),
                                    demand['Hr'].drop_duplicates()),
                            columns=['Location', 'Dt', 'Hr'])
    all_vals['wkdy'] = pd.to_datetime(all_vals['Dt']).dt.dayofweek

    demand = all_vals.merge(demand, on=['Location', 'Dt', 'Hr'], how='outer').fillna(0)

    #Group up to get average arrivals per hour by location, pivot into usable format.
    demand = demand.groupby(['Location', 'wkdy', 'Hr'], as_index=False)['Arrivals'].mean()
    amb_demand = demand.loc[demand['Location'] == 'Ambulatory'].pivot(index='Hr', columns='wkdy', values='Arrivals')
    maj_demand = demand.loc[demand['Location'] == 'Majors'].pivot(index='Hr', columns='wkdy', values='Arrivals')
    res_demand = demand.loc[demand['Location'] == 'Resus'].pivot(index='Hr', columns='wkdy', values='Arrivals')
    pae_demand = demand.loc[demand['Location'] == 'Paeds'].pivot(index='Hr', columns='wkdy', values='Arrivals')

    ###################RESULTS
    pat_res = []
    occ_staff_res = []

    ###################ADMIN
    cl3_engine.dispose()

class spawn_patient:
    def __init__(self, p_id, area, time, dow, hour, stream_perc):
        #patient id
        self.id = p_id
        #Record area
        self.area = area
        #Record probability of streaming
        self.streamed = (True if random.uniform(0,1)
                        <= stream_perc else False)
        #recrord timings
        self.arrival_time = time
        self.arrival_wkdy = dow
        self.arrival_hour = hour
        self.triage_time = np.nan
        self.triage_staff = np.nan
        self.assessment_time = np.nan
        self.assessment_staff = np.nan
        self.wait_for_spec_time = np.nan
        self.decision_time = np.nan
        self.decision_staff = np.nan
        self.leave_time = np.nan

class ED_staffing_model:
    def __init__(self, run_number, input_params):
        #Set up lists to record results
        self.patient_results = []
        self.occ_staff_results = []
        #start environment, set patient counter to 0 and set run number
        self.env = simpy.Environment()
        self.input_params = input_params
        self.patient_counter = 0
        self.run_number = run_number
        #establish staff dictionary
        self.staff = {'Consultant' : simpy.Resource(self.env, capacity=input_params.no_consultants),
                      'Middle Tier' : simpy.Resource(self.env, capacity=input_params.no_middle_tier),
                      'Resident' : simpy.Resource(self.env, capacity=input_params.no_resident)}
        self.amb_triage = simpy.PriorityResource(self.env, capacity=input_params.amb_triage_cap)
        self.maj_triage = simpy.PriorityResource(self.env, capacity=input_params.maj_triage_cap)
        self.amb_assess = simpy.PriorityResource(self.env, capacity=input_params.amb_assess_cap)
        self.maj_assess = simpy.PriorityResource(self.env, capacity=input_params.maj_assess_cap)
        self.res_assess = simpy.PriorityResource(self.env, capacity=input_params.res_assess_cap)
        self.pae_assess = simpy.PriorityResource(self.env, capacity=input_params.pae_assess_cap)

    ##############################MODEL TIME##############################
    def model_time(self, time):
        #Work out what day and time it is in the model.
        day = math.floor(time / (24*60))
        day_of_week = day % 7
        #If day 0, hour is time / 60, otherwise it is the remainder time once
        #divided by number of days
        hour = math.floor((time % (day*(24*60)) if day != 0 else time) / 60)
        return day, day_of_week, hour
    
    ##############################ORDERED REQUESTS##############################
    def ordered_requests(self, order_lst):
        #Requst each staff member in order of priority, wait until one is returned
        requests = {req: self.staff[req].request() for req in order_lst}
        result = yield simpy.events.AnyOf(self.env, requests.values())
        #Find which staff member fulfilled the request, cancel others
        req_found = False
        staff_found = None
        staff_req = None
        for res_name, req in requests.items():
            if (req in result) and (not req_found):
                staff_found = res_name
                staff_req = req
                req_found = True
            else:
                self.staff[res_name].release(req)
        return staff_found, staff_req

    ##############################ARRIVALS##############################
    def arrivals(self, area):
        #####Get the demand data for the area
        if area == 'Ambulatory':
            demand = self.input_params.amb_demand      
        elif area == 'Majors':
            demand = self.input_params.maj_demand
        elif area == 'Resus':
            demand = self.input_params.res_demand
        elif area == 'Paeds':
            demand = self.input_params.pae_demand
        #####Timeout until first arrival
        initial_arr = demand.iloc[0, 0]
        intr_arr = (60 / initial_arr).round()
        yield self.env.timeout(intr_arr)

        while True:
            #####Get model time variables
            time = self.env.now
            day, day_of_week, hour = self.model_time(time)
            #####up patient counter, spawn a new patient and begin process
            self.patient_counter += 1
            p = spawn_patient(self.patient_counter, area, time, day_of_week, hour,
                              self.input_params.stream[area])
           # print(f'{area}: patient {p.id} spawned, starting model at time {self.env.now}')
            self.env.process(self.ED_journey(p))
            #####time out until the next patient arrival
            hr_arrs = demand.loc[hour, day_of_week].copy()
            #If more than 1 arrival per hour, use inter arrival time
            if hr_arrs >= 1:
                inter_arr = 60 / hr_arrs
            #Else use it as a probability that a patient will arrive in that hour
            else:
                inter_arr = 60
                arr_bool = True
                while arr_bool:
                    #Get the arrival rate of the next hour
                    time += 60
                    day, day_of_week, hour = self.model_time(time)
                    #hour = hour + 1 if hour != 23 else 0
                    hr_arrs = demand.loc[hour, day_of_week]
                    #If the random choice predicts an arrival in the next hour,
                    #Exit the loop and time out until then.  Else, add another hour
                    #to the timeout time and check if an arrival in the next hour.
                    if random.uniform(0,1) <= hr_arrs:
                        arr_bool = False
                    else:
                        inter_arr += 60
            #Time out until next patient
            sampled_interarrival = round(random.expovariate(1.0 / inter_arr))
            yield self.env.timeout(sampled_interarrival)

    ##############################ED JOURNEY##############################
    def ED_journey(self, patient):
        #Get the times used for each area
        area = patient.area
        if area == 'Ambulatory':
            staffing = self.input_params.amb_staffing.copy()
            triage_time = self.input_params.amb_time_triage
            assess_time = self.input_params.amb_time_assess
            decisi_time = self.input_params.amb_time_decisi
            wait_time = self.input_params.amb_time_wait
        elif area == 'Majors':
            staffing = self.input_params.maj_staffing.copy()
            triage_time = self.input_params.maj_time_triage
            assess_time = self.input_params.maj_time_assess
            decisi_time = self.input_params.maj_time_decisi
            wait_time = self.input_params.maj_time_wait
        elif area == 'Resus':
            staffing = self.input_params.res_staffing.copy()
            assess_time = self.input_params.res_time_assess
            decisi_time = self.input_params.res_time_decisi
            wait_time = self.input_params.res_time_wait
        elif area == 'Paeds':
            staffing = self.input_params.pae_staffing.copy()
            assess_time = self.input_params.pae_time_assess
            decisi_time = self.input_params.pae_time_decisi
            wait_time = self.input_params.pae_time_wait
        
        patient.arrival_time = self.env.now

        #####TRIAGE
        if patient.area in ['Ambulatory', 'Majors']:
           # print(f'{patient.area}: patient {patient.id} requesting triage at time {patient.arrival_time}')
            #Space request based on if Ambulatory or Majors
            space_req = self.amb_triage.request() if area == 'Ambulatory' else self.maj_triage.request()
            with space_req:
                yield space_req
                #Request a staff member in order of priority
                name, staff_req = yield from self.ordered_requests(staffing['Triage'])
                #Record the time the process begins and timeout for triage time
                patient.triage_time = self.env.now
                patient.triage_staff = name
                sampled_triage_time = round((random.expovariate(1.0 / triage_time)))
                yield self.env.timeout(sampled_triage_time)
                self.staff[name].release(staff_req)

        #####STREAMING
        if patient.streamed:
            patient.leave_time = self.env.now
           # print(f'{patient.area}: patient {patient.id} triaged and streamed at time {patient.leave_time}')
            self.store_patient_results(patient)
        
        else:
        #####ASSESSMENT
           # print(f'{patient.area}: patient {patient.id} triaged, requesting assessment at {self.env.now}')
            if area == 'Ambulatory':
                space_req = self.amb_assess.request()
            elif area == 'Majors':
                space_req = self.maj_assess.request()
            elif area == 'Resus':
                space_req = self.res_assess.request()
            elif area == 'Paeds':
                space_req = self.pae_assess.request()

            #Space request
            with space_req:
                yield space_req
                #Staff request
                name, staff_req = yield from self.ordered_requests(staffing['Assessment or Descision'])
                #Record the time the process begins
                patient.assessment_time = self.env.now
                patient.assessment_staff = name
                #Timeout for process time
                sampled_assess_time = round((random.expovariate(1.0 / assess_time)))
                yield self.env.timeout(sampled_assess_time)
                self.staff[name].release(staff_req)
               # print(f'{name} released')
            
        #####WAIT FOR SPEC/INVESTIGATIONS
           # print(f'{patient.area}: patient {patient.id} waiting for spec at {self.env.now}')
            #Record time and timeout for waiting time
            patient.wait_for_spec_time = self.env.now
            sampled_wait_time = round((random.expovariate(1.0 / wait_time)))
            yield self.env.timeout(sampled_wait_time)

        #####DECISION
           # print(f'{patient.area}: patient {patient.id} starting getting decision {self.env.now}')
            #Staff request
            name, staff_req = yield from self.ordered_requests(staffing['Assessment or Descision'])
            #Record the time the process begins
            patient.decision_time = self.env.now
            patient.decision_staff = name
            #Timeout for process time
            sampled_decision_time = round((random.expovariate(1.0 / decisi_time)))
            yield self.env.timeout(sampled_decision_time)
            self.staff[name].release(staff_req)

         #####EXIT MODEL
            patient.leave_time = self.env.now
           # print(f'{patient.area}: patient {patient.id} exits model at time {patient.leave_time}')
            self.store_patient_results(patient)

    #################RECORD RESULTS####################
    def store_patient_results(self, patient):
        self.patient_results.append([self.run_number,
                                     patient.id,
                                     patient.area,
                                     patient.arrival_time,
                                     patient.arrival_hour,
                                     patient.arrival_wkdy,
                                     patient.triage_time,
                                     patient.triage_staff,
                                     patient.assessment_time,
                                     patient.assessment_staff,
                                     patient.wait_for_spec_time,
                                     patient.decision_time,
                                     patient.decision_staff,
                                     patient.leave_time])
    
    def store_staff_and_occ(self):
        while True:
            self.occ_staff_results.append([self.run_number,
                                           self.staff['Consultant']._env.now,
                                           #staff
                                           self.staff['Consultant'].count,
                                           self.staff['Middle Tier'].count,
                                           self.staff['Resident'].count,
                                           #Triage Locations
                                           len(self.amb_triage.queue),
                                           self.amb_triage.count,
                                           len(self.maj_triage.queue),
                                           self.maj_triage.count,
                                           #Assessment Locations
                                           len(self.amb_assess.queue),
                                           self.amb_assess.count,
                                           len(self.maj_assess.queue),
                                           self.maj_assess.count,
                                           len(self.res_assess.queue),
                                           self.res_assess.count,
                                           len(self.pae_assess.queue),
                                           self.pae_assess.count])
            yield self.env.timeout(self.input_params.sample_time)
      
########################RUN#######################
    def run(self):
        self.env.process(self.arrivals('Ambulatory'))
        self.env.process(self.arrivals('Majors'))
        self.env.process(self.arrivals('Resus'))
        self.env.process(self.arrivals('Paeds'))
        self.env.process(self.store_staff_and_occ())
        self.env.run(until = self.input_params.run_time)
        default_params.pat_res += self.patient_results
        default_params.occ_staff_res += self.occ_staff_results
        return self.patient_results, self.occ_staff_results

def export_results(pat_results, occ_staff_results):
    patient_df = pd.DataFrame(pat_results,
                              columns=['Run', 'Patient ID', 'Area', 'Arrival',  'Arrival Hour',
                                       'Arrival DoW', 'Triage', 'Traige Staff', 'Assessment',
                                       'Assessment Staff', 'Wait for Spec', 'Decision',
                                       'Decision Staff', 'Leave'])

    occupancy_df = pd.DataFrame(occ_staff_results,
                                columns=['Run', 'Time',
                                         'Consultants', 'Middle Tier', 'Residents',
                                         'Amb Triage Queue', 'Amb Triage Use',
                                         'Maj Triage Queue', 'Maj Triage Use',
                                         'Amb Assessment Queue', 'Amb Assessment Use',
                                         'Maj Assessment Queue', 'Maj Assessment Use',
                                         'Res Assessment Queue', 'Res Assessment Use',
                                         'Pae Assessment Queue', 'Pae Assessment Use',])

    return patient_df, occupancy_df


def run_the_model(input_params):
    #run the model for the number of iterations specified
    for run in range(input_params.iterations):
        print(f"Run {run+1} of {input_params.iterations}")
        model = ED_staffing_model(run, input_params)
        model.run()
    patient_df, occ_df = export_results(input_params.pat_res,
                                        input_params.occ_staff_res)
    return patient_df, occ_df

###############Run and save the model
pat, occ = run_the_model(default_params)
os.chdir('G:/PerfInfo/Performance Management/OR Team/Emily Projects/Discrete Event Simulation/ED Staffing Model')
pat.to_csv(f'Outputs/{default_params.run_name} Patients.csv')
occ.to_csv(f'Outputs/{default_params.run_name} Occupancy.csv')

############Print highlevel figures
#Check arrival numbers/demand profile.
pat['Day'] = (pat['Arrival'] / (24*60)).apply(math.floor)
arrs = ((pat.groupby(['Area', 'Arrival Hour'])['Patient ID'].count()
        / ((pat['Day'].max()+1) * (pat['Run'].max()+1))).reset_index()
        .pivot(index='Arrival Hour', columns='Area', values='Patient ID'))
print('----Average arrivals')
print(arrs.sum())

#Occupancy summary numbers
occ['Day'] = (occ['Time'] / (24*60)).apply(math.floor)
occ['Hour'] = (np.where((occ['Day']*(24*60)) != 0, occ['Time'] % (occ['Day']*(24*60)), occ['Time']) / 60)
occ['Hour'] = occ['Hour'].apply(math.floor)
occ['Day of Week'] = occ['Day'] % 7
print('----Average Staff Usage')
print(occ[['Consultants', 'Middle Tier', 'Residents']].mean())


##################################################################################################
############################################PLOTS#################################################
##################################################################################################
hours = pat['Arrival Hour'].drop_duplicates().sort_values()
days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
staff_members = ['Consultants', 'Middle Tier', 'Residents']

def q25(x):
    return x.quantile(0.25)
def q75(x):
    return x.quantile(0.75)
quartile_label = '25-75 quartiles'

##################Arrivals
#######Daily Arrivals
agg_figures = (pat.groupby(['Run', 'Area', 'Day', 'Arrival Hour'], as_index=False)['Patient ID'].count()
                  .groupby(['Area', 'Arrival Hour'])['Patient ID'].agg(['min', q25,'mean', q75, 'max']))
hours = pat['Arrival Hour'].drop_duplicates().sort_values()
#plot
fig, ([ax1, ax2], [ax3, ax4]) = plt.subplots(2, 2, figsize=(20, 10), sharex=True)
fig.suptitle('Arrivals by Hour of Day', fontsize=24)

for ax, area in zip([ax1, ax2, ax3, ax4], ['Ambulatory', 'Majors', 'Resus', 'Paeds']):
    data = agg_figures.loc[area].copy()
    ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
    ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
    ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
    ax.set_title(area, fontsize=18)
    ax.tick_params(axis='both',  which='major', labelsize=18)
plt.legend(fontsize=18)
fig.supxlabel('Hour of Day', fontsize=18)
fig.supylabel('Arrivals', fontsize=18)
fig.tight_layout()
plt.savefig(f'Plots/Arrivals - {default_params.run_name}.png', bbox_inches='tight')
plt.close()


#######DoW arrivals
agg_figures = (pat.groupby(['Run', 'Area', 'Day', 'Arrival DoW', 'Arrival Hour'], as_index=False)['Patient ID'].count()
                  .groupby(['Area', 'Arrival DoW', 'Arrival Hour'])['Patient ID'].agg(['min', q25,'mean', q75, 'max']))
hours = pat['Arrival Hour'].drop_duplicates().sort_values()

for area in ['Ambulatory', 'Majors', 'Resus', 'Paeds']:
    area_data = agg_figures.loc[area].copy()
    #plot
    fig, ([ax1, ax2, ax3, ax4], [ax5, ax6, ax7, ax8]) = plt.subplots(2, 4, figsize=(20, 10), sharex=True, sharey=True)
    fig.suptitle(f'{area} - Arrivals by Day of Week', fontsize=24)
    for i, ax in enumerate([ax1, ax2, ax3, ax4, ax5, ax6, ax7]):
        data = area_data.loc[i].copy()
        ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
        ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
        ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
        ax.set_title(days_of_week[i], fontsize=18)
        ax.tick_params(axis='both',  which='major', labelsize=18)
    plt.legend(fontsize=18)
    fig.supxlabel('Hour of Day', fontsize=18)
    fig.supylabel('Arrivals', fontsize=18)
    fig.tight_layout()
    ax8.axis('off')
    plt.savefig(f'Plots/Arrivals in {area}  by DoW - {default_params.run_name}.png', bbox_inches='tight')
    plt.close()

#######################Staff Usage
agg_figures = occ.groupby('Hour')[staff_members].agg(['min', q25,'mean', q75, 'max'])
fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 8), sharex=True, sharey=True)
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
plt.savefig(f'Plots/Staff Usage All - {default_params.run_name}.png', bbox_inches='tight')
plt.close()


########Staff Usage by day of week
agg_figures = occ.groupby(['Day of Week', 'Hour'])[staff_members].agg(['min', q25,'mean', q75, 'max'])
for staff in ['Consultants', 'Middle Tier', 'Residents']:
    staff_data = agg_figures[staff].copy()
    #plot
    fig, ([ax1, ax2, ax3, ax4], [ax5, ax6, ax7, ax8]) = plt.subplots(2, 4, figsize=(20, 10), sharex=True, sharey=True)
    fig.suptitle(f'{staff} - Usage by Day of Week', fontsize=24)
    for i, ax in enumerate([ax1, ax2, ax3, ax4, ax5, ax6, ax7]):
        data = staff_data.loc[i].copy()
        ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
        ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
        ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
        ax.set_title(days_of_week[i], fontsize=18)
        ax.tick_params(axis='both',  which='major', labelsize=18)
    plt.legend(fontsize=18)
    fig.supxlabel('Hour of Day', fontsize=18)
    fig.supylabel('Number in Use', fontsize=18)
    fig.tight_layout()
    ax8.axis('off')
    plt.savefig(f'Plots/Staff Usage {staff} by Day of Week - {default_params.run_name}.png', bbox_inches='tight')
    plt.close()

########Triage and Assessment space usage
agg_figures = occ.groupby('Hour')[['Amb Assessment Use', 'Maj Assessment Use',
            'Res Assessment Use',  'Pae Assessment Use']].agg(['min', q25,'mean', q75, 'max'])

fig, ([ax1, ax2], [ax3, ax4]) = plt.subplots(2, 2, figsize=(20, 8), sharex=True)
fig.suptitle(f'Assessment Space Usage', fontsize=24)
for ax, loc in zip([ax1, ax2, ax3, ax4], ['Ambulatory', 'Majors', 'Resus', 'Paeds']):
    data = agg_figures[f'{loc[:3]} Assessment Use'].copy()
    ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
    ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
    ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
    ax.set_title(loc, fontsize=18)
    ax.tick_params(axis='both',  which='major', labelsize=18)
plt.legend(fontsize=18)
fig.supxlabel('Hour of Day', fontsize=18)
fig.supylabel('Number in Use', fontsize=18)
fig.tight_layout()
plt.savefig(f'Plots/Space Usage Assessment - {default_params.run_name}.png', bbox_inches='tight')
plt.close()

##########Triage space usage
agg_figures = occ.groupby('Hour')[['Amb Triage Use', 'Maj Triage Use']].agg(['min', q25,'mean', q75, 'max'])

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8), sharex=True, sharey=True)
fig.suptitle(f'Triage Space Usage', fontsize=24)
for ax, loc in zip([ax1, ax2, ax3], ['Ambulatory', 'Majors']):
    data = agg_figures[f'{loc[:3]} Triage Use'].copy()
    ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
    ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
    ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
    ax.set_title(loc, fontsize=18)
    ax.tick_params(axis='both',  which='major', labelsize=18)
plt.legend(fontsize=18)
fig.supxlabel('Hour of Day', fontsize=18)
fig.supylabel('Number in Use', fontsize=18)
fig.tight_layout()
plt.savefig(f'Plots/Space Usage Triage  - {default_params.run_name}.png', bbox_inches='tight')
plt.close()
