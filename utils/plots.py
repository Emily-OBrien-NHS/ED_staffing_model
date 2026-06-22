import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd

###################################################################################################
                                            ####UTILITIES####
###################################################################################################
days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
staff_members = ['Consultants', 'Middle Tier', 'Residents']
areas = ['Paeds', 'Ambulatory', 'Majors', 'Resus']
quartile_label = '25-75 quartiles'

def q25(x):
    return x.quantile(0.25)
def q75(x):
    return x.quantile(0.75)

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

###################################################################################################
                                            ####HOUR PLOTS####
###################################################################################################
def hour_results_plots(pat, occ):
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


###################################################################################################
                                            ####DAY OF WEEK####
###################################################################################################
def day_results_plots(pat, occ):
    areas = ['Paeds', 'Ambulatory', 'Majors', 'Resus']
    st.divider()
    st.markdown('## Day of Week Plots')
    st.text("Choose which plot you'd like to view:")
    plot_tabs = st.tabs(['Arrivals', 'Staff Usage', 'LoS', '4hr Performance'])
    
    #Arrivals
    with plot_tabs[0]:
        agg_figures = (pat.groupby(['Run', 'Area', 'Day', 'Arrival DoW', 'Arrival Hour'], as_index=False)['Patient ID'].count()
                        .groupby(['Area', 'Arrival DoW', 'Arrival Hour'])['Patient ID'].agg(['min', q25,'mean', q75, 'max']))
        hours = pat['Arrival Hour'].drop_duplicates().sort_values()
        for area in areas:
            area_data = agg_figures.loc[area].copy()
            fig, ([ax1, ax2, ax3, ax4], [ax5, ax6, ax7, ax8]) = plt.subplots(2, 4, figsize=(18, 10), sharex=True, sharey=True)
            fig.suptitle(f'{area} - Arrivals by Day of Week', fontsize=24)
            for i, ax in enumerate([ax1, ax2, ax3, ax4, ax5, ax6, ax7]):
                data = area_data.loc[i].copy()
                data = data.reset_index().merge(pd.DataFrame(hours), on='Arrival Hour', how='right').set_index('Arrival Hour').fillna(0)
                ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
                ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
                ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
                ax.set_title(days_of_week[i], fontsize=18)
                ax.tick_params(axis='both',  which='major', labelsize=18)
            #plt.legend(fontsize=18)
            fig.supxlabel('Hour of Day', fontsize=18)
            fig.supylabel('Arrivals', fontsize=18)
            fig.tight_layout()
            ax8.axis('off')
            st.pyplot(fig)
            plt.close(fig)

    with plot_tabs[1]:
        #Staff Usage
        agg_figures = occ.groupby(['Day of Week', 'Hour'])[staff_members].agg(['min', q25,'mean', q75, 'max'])
        for staff in ['Consultants', 'Middle Tier', 'Residents']:
            staff_data = agg_figures[staff].copy()
            #plot
            fig, ([ax1, ax2, ax3, ax4], [ax5, ax6, ax7, ax8]) = plt.subplots(2, 4, figsize=(18, 10), sharex=True, sharey=True)
            fig.suptitle(f'{staff} - Usage by Day of Week', fontsize=24)
            for i, ax in enumerate([ax1, ax2, ax3, ax4, ax5, ax6, ax7]):
                data = staff_data.loc[i].copy()
                ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
                ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
                ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
                ax.set_title(days_of_week[i], fontsize=18)
                ax.tick_params(axis='both',  which='major', labelsize=18)
            #plt.legend(fontsize=18)
            fig.supxlabel('Hour of Day', fontsize=18)
            fig.supylabel('Number in Use', fontsize=18)
            fig.tight_layout()
            ax8.axis('off')
            st.pyplot(fig)
            plt.close(fig)

    with plot_tabs[2]:
        #LoS
        agg_figures = pat.groupby(['Area', 'Arrival DoW', 'Arrival Hour'])['LoS'].agg(['min', q25,'mean', q75, 'max']) 
        for area in areas:
            area_data = agg_figures.loc[area].copy()
            #plot
            fig, ([ax1, ax2, ax3, ax4], [ax5, ax6, ax7, ax8]) = plt.subplots(2, 4, figsize=(18, 10), sharex=True, sharey=True)
            fig.suptitle(f'{area} - LoS by Day of Week', fontsize=24)
            for i, ax in enumerate([ax1, ax2, ax3, ax4, ax5, ax6, ax7]):
                try:
                    data = area_data.loc[i].copy()

                except:
                    data = pd.DataFrame(columns=['min', 'q25', 'mean', 'q75', 'max'], index=hours).fillna(0)
                data = data.reset_index().merge(pd.DataFrame(hours), on='Arrival Hour', how='right').set_index('Arrival Hour').fillna(0)
                ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
                ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
                ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
                ax.set_title(days_of_week[i], fontsize=18)
                ax.tick_params(axis='both',  which='major', labelsize=18)
            #plt.legend(fontsize=18)
            fig.supxlabel('Arrival Hour of Day', fontsize=18)
            fig.supylabel('Average LoS', fontsize=18)
            fig.tight_layout()
            ax8.axis('off')
            st.pyplot(fig)
            plt.close(fig)

    with plot_tabs[3]:
        #4hr perf by day of week
        agg_figures = pat.groupby(['Run', 'Area', 'Day', 'Arrival DoW', 'Arrival Hour'], as_index=False)['not 4hr breach'].agg(['sum', 'count'])
        agg_figures['4 hour performance'] = agg_figures['sum'] / agg_figures['count']
        area_agg = agg_figures.groupby(['Area', 'Arrival DoW', 'Arrival Hour'])['4 hour performance'].agg(['min', q25,'mean', q75, 'max']) 
        for area in areas:
            area_data = area_agg.loc[area].copy()
            #plot
            fig, ([ax1, ax2, ax3, ax4], [ax5, ax6, ax7, ax8]) = plt.subplots(2, 4, figsize=(20, 10), sharex=True, sharey=True)
            fig.suptitle(f'{area} - 4 hour performance by day of week', fontsize=24)
            for i, ax in enumerate([ax1, ax2, ax3, ax4, ax5, ax6, ax7]):
                try:  
                    data = area_data.loc[i].copy()
                except:
                    data = pd.DataFrame(columns=['min', 'q25', 'mean', 'q75', 'max'], index=hours).fillna(0)
                data = data.reset_index().merge(pd.DataFrame(hours), on='Arrival Hour', how='right').set_index('Arrival Hour').fillna(0)
                ax.plot(hours, data['mean'].fillna(0), '-r', label='Mean')
                ax.fill_between(hours, data['min'].fillna(0), data['max'].fillna(0), color='grey', alpha=0.2, label='Min-Max')
                ax.fill_between(hours, data['q25'].fillna(0), data['q75'].fillna(0), color='black', alpha=0.2, label=quartile_label)
                ax.set_title(days_of_week[i], fontsize=18)
                ax.tick_params(axis='both',  which='major', labelsize=18)
            #plt.legend(fontsize=18)
            fig.supxlabel('Arrival Hour of Day', fontsize=18)
            fig.supylabel('4 hour performance', fontsize=18)
            fig.tight_layout()
            ax8.axis('off')
            st.pyplot(fig)
            plt.close(fig)
