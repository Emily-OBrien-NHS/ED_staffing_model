import simpy
import random
import math
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from itertools import product

class default_params():
    ########General Params
    run_name = 'ED Staffing Model'
    #run times and iterations
    run_time = 1440
    run_days = int(run_time/(60*24))
    iterations = 2#10
    sample_time = 60

    ###################EVENTS
    ########Mean Event Timings (_time_) and Capacities (_cap)
    #Ambulatory
    amb_time_triage = 7
    amb_time_assess = 50
    amb_time_decisi = 15
    amb_triage_cap = 2
    amb_assess_cap = 23
    amb_time_wait = 60
    #Majors
    maj_mean_triage = 10
    maj_mean_assess = 90
    maj_mean_decisi = 25
    maj_triage_cap = 2
    maj_assess_cap = 18
    #Resus
    res_mean_assess = 120
    res_mean_decisi = 50
    res_assess_cap = 7
    #Paeds
    pae_mean_assess = 50
    pae_mean_decisi = 15
    pae_assess_cap = 10
    #Streaming
    stream_perc = 0.3
    
    ###################STAFFING
    ########Staffing Numbers
    no_consultants = np.inf
    no_middle_tier = np.inf
    no_resident = np.inf

    ########Staffing Requirements
    #staff appear in order of preference and priority
    triage_ordering = ['Cons', 'MT']
    amb_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['MT', 'Res', 'Cons']}
    maj_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['Res', 'MT', 'Cons']}
    res_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['MT', 'Cons', 'Res']}
    pae_staffing = {'Triage':triage_ordering,
                    'Assessment or Descision':['MT', 'Res', 'Cons']}
    
    ###################DEMAND
    #Need to pull in current demand and work out the average number of arrivals
    #to each location by hour of the day.
    cl3_engine = create_engine('mssql+pyodbc://@cl3-data/DataWarehouse?'\
                           'trusted_connection=yes&driver=ODBC+Driver+17'\
                               '+for+SQL+Server')
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
                       demand['Hr'].drop_duplicates()), columns=['Location', 'Dt', 'Hr'])
    demand = all_vals.merge(demand, on=['Location', 'Dt', 'Hr'], how='outer').fillna(0)

    #Group up to get average arrivals per hour by location, pivot into usable format.
    demand = (demand.groupby(['Location', 'Hr'], as_index=False)['Arrivals'].mean()
                    .pivot(index='Hr', columns='Location', values='Arrivals'))

    ###################RESULTS
    pat_res = []
    occ_staff_res = []

class spawn_patient:
    def __init__(self, p_id, stream_perc):
        #patient id
        self.id = p_id
        #Record arrival mode
        self.area = ''
        #Record probability of streaming
        self.streamed = (True if random.uniform(0,1)
                        <= stream_perc else False)
        #recrord timings
        self.arrival_time = np.nan
        self.triage_time = np.nan
        self.assessment_time = np.nan
        self.wait_for_spec_time = np.nan
        self.decision_time = np.nan
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
        #establish staff
        self.consultant = simpy.Resource(self.env, capacity=input_params.no_consultants)
        self.middle_tier = simpy.Resource(self.env, capacity=input_params.no_middle_tier)
        self.resident = simpy.Resource(self.env, capacity=input_params.no_resident)
        #establish locations
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
    
    ##############################ARRIVALS##############################
    def amb_arrivals(self):
        hrly_arr = self.input_params.demand['Ambulatory'].copy()
        intr_arr = 60 / hrly_arr
        yield self.env.timeout(intr_arr[0])
        while True:
            #up patient counter and spawn a new SSU patient
            self.patient_counter += 1
            p = spawn_patient(self.patient_counter,
                              self.input_params.stream_perc)
            p.area = 'Ambulatory'
            print(f'patient {p.id} spawned in Ambulatory, starting model at time {self.env.now}')
            #begin patient to ED process
            self.env.process(self.ED_journey(p))
            #randomly sample the time until the next patient arrival
            #Get model time variables
            time = self.env.now
            day, day_of_week, hour = self.model_time(time)
            sampled_interarrival = round(random.expovariate(1.0 / intr_arr[hour]))
            yield self.env.timeout(sampled_interarrival)       

    ##############################ED JOURNEY##############################
    def ED_journey(self, patient):
        patient.arrival_time = self.env.now

        print(f'patient {patient.id} requesting triage at time {patient.arrival_time}')
        #Request triage
        with self.amb_triage.request() as req:
            yield req
            with self.consultant.request() as req:
                yield req
                patient.triage_time = self.env.now
                sampled_triage_time = round((random.expovariate(1.0
                                                / self.input_params.amb_time_triage)))
                yield self.env.timeout(sampled_triage_time)

        #Streamed patients leave
        if patient.streamed:
            patient.leave_time = self.env.now
            print(f'patient {patient.id} triaged and streamed at time {patient.leave_time}')
            self.store_patient_results(patient)
        else:
            #Request assessment
            print(f'patient {patient.id} triaged, requesting assessment at {self.env.now}')
            with self.amb_assess.request() as req:
                yield req
                with self.middle_tier.request() as req:
                    yield req
                    patient.assessment_time = self.env.now
                    sampled_assess_time = round((random.expovariate(1.0
                                                    / self.input_params.amb_time_assess)))
                    yield self.env.timeout(sampled_assess_time)
            
            #Wait for specialty/Investigations
            print(f'patient {patient.id} waiting for spec at {self.env.now}')
            patient.wait_for_spec_time = self.env.now
            sampled_wait_time = round((random.expovariate(1.0 / self.input_params.amb_time_wait)))
            yield self.env.timeout(sampled_wait_time)

            #Decision
            print(f'patient {patient.id} getting decision {self.env.now}')
            with self.middle_tier.request() as req:
                yield req
                patient.decision_time = self.env.now
                sampled_decision_time = round((random.expovariate(1.0
                                                    / self.input_params.amb_time_decisi)))
                yield self.env.timeout(sampled_decision_time)

            #Leave model
            patient.leave_time = self.env.now
            print(f'patient {patient.id} streamed at time {patient.leave_time}')
            self.store_patient_results(patient)

    #################RECORD RESULTS####################
    def store_patient_results(self, patient):
        self.patient_results.append([self.run_number, patient.id,
                                     patient.area,
                                     patient.arrival_time,
                                     patient.triage_time,
                                     patient.assessment_time,
                                     patient.wait_for_spec_time,
                                     patient.decision_time,
                                     patient.leave_time])
    
    def store_staff_and_occ(self):
        while True:
            self.occ_staff_results.append([self.run_number,
                                               self.consultant._env.now,
                                               #staff
                                               self.consultant.count,
                                               self.middle_tier.count,
                                               self.resident.count,
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
                                               self.pae_assess.count,
                                               ])
            yield self.env.timeout(self.input_params.sample_time)
      
########################RUN#######################
    def run(self):
        self.env.process(self.amb_arrivals())
        self.env.process(self.store_staff_and_occ())
        self.env.run(until = self.input_params.run_time)
        default_params.pat_res += self.patient_results
        default_params.occ_staff_res += self.occ_staff_results
        return self.patient_results, self.occ_staff_results

def export_results(pat_results, occ_staff_results):
    patient_df = pd.DataFrame(pat_results,
                              columns=['Run', 'Patient ID', 'Area', 'Arrival', 'Triage',
                                       'Assessment', 'Wait for Spec', 'Decision', 'Leave'])

    occupancy_df = pd.DataFrame(occ_staff_results,
                                columns=['Run', 'Time',
                                         'Consultants',
                                         'Middle Tier',
                                         'Residents',
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

pat, occ = run_the_model(default_params)
x=5