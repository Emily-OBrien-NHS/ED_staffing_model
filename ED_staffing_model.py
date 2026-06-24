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
    #cl3_engine = create_engine('mssql+pyodbc://@cl3-data/DataWarehouse?'\
    #                           'trusted_connection=yes&driver=ODBC+Driver+17'\
    #                           '+for+SQL+Server')
    ########General Params
    run_name = 'baseline'
    #run times and iterations
    run_time = 24*60*14#365
    run_days = int(run_time/(60*24)) 
    iterations = 2#0
    sample_time = 15
    print_outs = False

    ###################EVENTS
    ########Mean Event Timings (_time_) and Capacities (_cap)
    #Ambulatory
    amb_time_triage = 7
    amb_time_assess = 50
    amb_time_wait = 60
    amb_time_decisi = 15
    amb_triage_cap = 5
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
    #stream = dict(pd.read_sql(stream_sql, cl3_engine).values)
    stream = None
    
    ###################STAFFING
    ########Staffing Numbers
    #staff_file = 'G:/PerfInfo/Performance Management/OR Team/Emily Projects/Discrete Event Simulation/ED Staffing Model/Staffing Inputs/Proposed Staffing input.xlsx'
    #wkdy_staff = pd.read_excel(staff_file, sheet_name='Weekday', index_col=0)
    #wknd_staff = pd.read_excel(staff_file, sheet_name='Weekend', index_col=0)
    #wkdy_staff.iloc[:, :] = np.inf 
    #wknd_staff.iloc[:, :] = np.inf
    wkdy_staff = None
    wknd_staff = None


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
    #demand = pd.read_sql(demand_sql, cl3_engine)
    demand = pd.DataFrame({'Location':[''], 'Dt':[''], 'Hr':[''], 'Arrivals':[0]})

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
    print(demand.groupby(['Location', 'wkdy'], as_index=False)['Arrivals'].sum().groupby('Location')['Arrivals'].mean())

    ###################RESULTS
    pat_res = []
    occ_staff_res = []

    ###################ADMIN
    #cl3_engine.dispose()

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

class DynamicResource(simpy.Resource):
    #Create own dynamic resource that can change capacity, and have no capacity
    #(by having capacity = 1 then immediately claiming it)

    #Initiate dynamic resource and list of blocked resources (simulating)
    def __init__(self, env, capacity=1):
        super().__init__(env, capacity=max(1, capacity))
        self._blockers = []
        self._blocked = False
    #Resets resource capacity each hour
    def set_capacity(self, new_capacity):
        if new_capacity == 0:
            self._enable_block()
        else:
            self._disable_block()
            self._capacity = new_capacity
            self._trigger_put(None)  # wake up any queued requests
    #If capacity is 0, make capacity = 1 (to avoid errors) and immediately take the resource
    def _enable_block(self):
        if not self._blocked:
            self._blocked = True
            self._capacity = 1
            req = super().request()
            self._blockers.append(req)
            self._trigger_put(None)
    #If capacity > 0, set capacity to that and release any blocks
    def _disable_block(self):
        if self._blocked:
            self._blocked = False
            for req in self._blockers:
                super().release(req)
            self._blockers.clear()
    #Returns the capacity
    @property
    def effective_capacity(self):
        return 0 if self._blocked else self._capacity
    
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
        self.staff = {'Consultant':  DynamicResource(self.env, capacity=1),
                      'Middle Tier': DynamicResource(self.env, capacity=1),
                      'Resident':    DynamicResource(self.env, capacity=1)}
        #establish location resources
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

    ##############################STAFFING ROTA##############################
    def staffing_rota(self):
        if default_params.print_outs:
            print('Starting staff rota')
        #cons_block, mt_block, res_block = False, False, False
        while True:
            #Get the time
            day, day_of_week, hour = self.model_time(self.env.now)
            wkdy = True if day_of_week not in [5, 6] else False
            #Work out which staff to use if weekday vs weekend
            staff_no = (self.input_params.wkdy_staff.loc[hour].copy() if wkdy
                        else self.input_params.wknd_staff.loc[hour].copy())
            
            if default_params.print_outs:
                print('-----------------')
                pr_str = 'Weekday' if wkdy else 'Weekend'
                print(f'Staff at hour {hour} on day {day} - {pr_str}:')
                print(staff_no[['Total Consultants', 'Total Middle Tier', 'Total Residents']])

            #Get the numbers of each individual staff member
            no_consultants = staff_no['Total Consultants']
            no_middle_tier = staff_no['Total Middle Tier']
            no_resident = staff_no['Total Residents']

            # Mutate existing resources — patients mid-process are unaffected
            self.staff['Consultant'].set_capacity(no_consultants)
            self.staff['Middle Tier'].set_capacity(no_middle_tier)
            self.staff['Resident'].set_capacity(no_resident)

            if default_params.print_outs:
                print(f'Staff levels updated: to {self.staff['Consultant'].capacity}, {self.staff['Middle Tier'].capacity}, {self.staff['Resident'].capacity}')
                print('---------------------------')
            #repeat every hour
            yield self.env.timeout(60)
       
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
                # CRITICAL: Different handling depending on whether request was granted
                if req.triggered:
                    # Request was granted (another AnyOf winner) — must release properly
                    self.staff[res_name].release(req)
                else:
                    # Request is still queued — cancel it instead
                    req.cancel()
                #self.staff[res_name].release(req)
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
            if default_params.print_outs:
                print(f'{area} patient {p.id}: spawned, starting model at time {self.env.now}')
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
            if default_params.print_outs:
                print(f'{patient.area} patient {patient.id}: requesting triage at time {patient.arrival_time}')
            #Space request based on if Ambulatory or Majors
            space_req = self.amb_triage.request() if area == 'Ambulatory' else self.maj_triage.request()
            with space_req:
                yield space_req
                #Request a staff member in order of priority
                if default_params.print_outs:
                    print(f'{patient.area} patient {patient.id}: requesting {staffing['Triage']} for triage at time {patient.arrival_time}')
                name, staff_req = yield from self.ordered_requests(staffing['Triage'])
                if default_params.print_outs:
                    print(f'{patient.area} patient {patient.id}: using {name} for triage at {self.env.now}')
                #Record the time the process begins and timeout for triage time
                patient.triage_time = self.env.now
                patient.triage_staff = name
                sampled_triage_time = round((random.expovariate(1.0 / triage_time)))
                yield self.env.timeout(sampled_triage_time)
                self.staff[name].release(staff_req)
                if default_params.print_outs:
                    print(f'{patient.area} patient {patient.id}: releasing {name} for triage at {self.env.now}')
            

        #####STREAMING
        if patient.streamed:
            patient.leave_time = self.env.now
            #print(f'{patient.area}: patient {patient.id} triaged and streamed at time {patient.leave_time}')
            self.store_patient_results(patient)
        
        else:
        #####ASSESSMENT
            #print(f'{patient.area}: patient {patient.id} triaged, requesting assessment at {self.env.now}')
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
                if default_params.print_outs:
                    print(f'{patient.area} patient {patient.id}: requesting {staffing['Assessment or Descision']} for assessment at time {patient.arrival_time}')
                name, staff_req = yield from self.ordered_requests(staffing['Assessment or Descision'])
                #Record the time the process begins
                if default_params.print_outs:
                    print(f'{patient.area} patient {patient.id}: using {name} for Assessment at {self.env.now}')
                patient.assessment_time = self.env.now
                patient.assessment_staff = name
                #Timeout for process time
                sampled_assess_time = round((random.expovariate(1.0 / assess_time)))
                yield self.env.timeout(sampled_assess_time)
                self.staff[name].release(staff_req)
                if default_params.print_outs:
                    print(f'{patient.area} patient {patient.id}: releasing {name} for Assessment at {self.env.now}')
            
        #####WAIT FOR SPEC/INVESTIGATIONS
           # print(f'{patient.area}: patient {patient.id} waiting for spec at {self.env.now}')
            #Record time and timeout for waiting time
            patient.wait_for_spec_time = self.env.now
            sampled_wait_time = round((random.expovariate(1.0 / wait_time)))
            yield self.env.timeout(sampled_wait_time)

        #####DECISION
           # print(f'{patient.area}: patient {patient.id} starting getting decision {self.env.now}')
            #Staff request
            if default_params.print_outs:
                print(f'{patient.area} patient {patient.id}: requesting {staffing['Assessment or Descision']} for decision at time {patient.arrival_time}')
            name, staff_req = yield from self.ordered_requests(staffing['Assessment or Descision'])
            #Record the time the process begins
            if default_params.print_outs:
                print(f'{patient.area} patient {patient.id}: using {name} for decision at {self.env.now}')
            patient.decision_time = self.env.now
            patient.decision_staff = name
            #Timeout for process time
            sampled_decision_time = round((random.expovariate(1.0 / decisi_time)))
            yield self.env.timeout(sampled_decision_time)
            self.staff[name].release(staff_req)
            if default_params.print_outs:
                print(f'{patient.area} patient {patient.id}: releasing {name} for decision at {self.env.now}')

         #####EXIT MODEL
            patient.leave_time = self.env.now
            if default_params.print_outs:
                print(f'{patient.area} patient {patient.id}: exits model at time {patient.leave_time}')
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
            if default_params.print_outs:
                print(f'OCC CHECK: {self.staff['Consultant'].count} Consultants, {self.staff['Middle Tier'].count} Middle Tier and {self.staff['Resident'].count} Residents in use at time {self.env.now}')
            
            def real_count(res):
                #In-use count excluding any blocker requests
                return res.count - len(res._blockers)
            
            self.occ_staff_results.append([self.run_number,
                                           self.staff['Consultant']._env.now,
                                           #staff
                                           real_count(self.staff['Consultant']),
                                           real_count(self.staff['Middle Tier']),
                                           real_count(self.staff['Resident']),
                                           #staff queue
                                           len(self.staff['Consultant'].queue),
                                           len(self.staff['Middle Tier'].queue),
                                           len(self.staff['Resident'].queue),
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
        self.env.process(self.staffing_rota())
        self.env.process(self.arrivals('Ambulatory'))
        self.env.process(self.arrivals('Majors'))
        self.env.process(self.arrivals('Resus'))
        self.env.process(self.arrivals('Paeds'))
        self.env.process(self.store_staff_and_occ())
        self.env.run(until = self.input_params.run_time)
        self.input_params.pat_res += self.patient_results
        self.input_params.occ_staff_res += self.occ_staff_results
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
                                         'Consultant Queue', 'Middle Tier Queue', 'Residents Queue',
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

# ###############Run and save the model
# pat, occ = run_the_model(default_params)
