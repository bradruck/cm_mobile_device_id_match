# Campaign Management - Pixel Creation, Mobile Device ID Match Rate

# Description -
# The Campaign Management Mobile Device ID Match is an automation for producing real time data metrics that will enable
# better decision making for the CM group in their mobile pixel id creation work.  The automation is deployed to run
# twice a month, the 1st and 15th early am.
# The automation begins by initiating an API call to the pixel builder server, this fetches a number of pixel ids
# along will relevant information on their campaigns. The returned pixel ids are then used to find the corresponding
# Jira tickets that they match up with.  The pixel numbers and campaign start dates are used to populate a hive query
# which returns count results of MAIDS (hashed and un-hashed) matched to HHIDS and cookies as they all relate to the
# pixel. The qubole calls are done concurrently, all at the same time. The count results are used to calculate match
# rates and all of the count and rate data is posted as a comment to the Jira ticket. The query count results are
# saved as a json file on zfs1 on the Operations_mounted drive for further processing if desired by the CM team.
# If a criteria matching Jira ticket cannot be found for the pixel, an alert email is sent to the CM team.
#
# Additional functionality added: Includes a search for the parent (measurement) ticket of the pixel ticket. Upon
# successful search, add the same comment to the measurement ticket as the pixel ticket.  The reporter and lead analyst
# are alerted via Jira comment post (if they exist on parent ticket). If no pixels are found to run, an alert email is
# sent to the CM team.
#
# Application Information -
# Required modules:     main.py,
#                       mobile_id_match_manager.py,
#                       pixel_name_search.py,
#                       jira_manager.py,
#                       qubole_manager.py,
#                       email_manager.py,
#                       hhid_pixel_query.py,
#                       config.ini
# Deployed Location:    //prd-use1a-pr-34-ci-operations-01/opt/app/automations/brad/Projects/
#                                                                           campaign_management_mobile_device_id_match/
# ActiveBatch Trigger:  //prd-09-abjs-01 (V11)/'Jobs, Folders & Plans'/Operations/Report/CM_MAID_to_HHID/First_of_Month
# Source Code:          //gitlab.oracledatacloud.com/odc-operations/CM_MAID_HIDD_Match/
# LogFile Location:     //zfs1/Operations_mounted/CampaignManagement/MaidsToHHIDsLogs/
# JsonFile Location:    //zfs1/Operations_mounted/CampaignManagement/MaidsToHHIDsLogs/Results/
#
# Contact Information -
# Primary Users:        Campaign Management
# Lead Customer:        Petya Mavrikov(petya.mavrikov@oracle.com)
# Lead Developer:       Bradley Ruck (bradley.ruck@oracle.com)
# Date Launched:        June, 2018
# Date Updated:         April, 2019

# main module
# Responsible for reading in the basic configurations settings, creating the log file, and creating and launching
# the Campaign Management SSID Manager (CM-SSID), finally it launches the purge_files method to remove log files that
# are older than a prescribed retention period. A console logger option is offered via keyboard input for development
# purposes when the main.py script is invoked. For production, import main as a module and launch the main function
# as main.main(), which uses 'n' as the default input to the the console logger run option.
#
from datetime import datetime, timedelta
import os
import configparser
import logging

#from VaultClient3 import VaultClient3 as VaultClient
from mobile_id_match_manager import MobileIDMatchManager


# Define a console logger for development purposes
#
def console_logger():
    # define Handler that writes DEBUG or higher messages to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    # set a simple format for console use
    formatter = logging.Formatter('%(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s')
    console.setFormatter(formatter)
    # add the Handler to the root logger
    logging.getLogger('').addHandler(console)


def main(con_opt='n'):
    today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y%m%d-%H%M%S')

    # create a configparser object and open in read mode
    config = configparser.ConfigParser()
    config.read('config.ini')

    # create a dictionary of configuration parameters
    config_params = {
        "jira_url":             config.get('Jira', 'url'),
        "jira_token":           tuple(config.get('Jira', 'authorization').split(',')),
        "jql_type":             config.get('Jira', 'type'),
        "jql_status":           config.get('Jira', 'status'),
        "qubole_token":         config.get('Qubole', 'bradruck-prod-operations-consumer'),
        "cluster_label":        config.get('Qubole', 'cluster-label'),
        "api_url":              config.get('Api', 'api_url', raw=True),
        "results_json_path":    config.get('ResultsFile', 'path'),
        "results_json_name":    config.get('Project Details', 'app_name'),
        "email_subject":        config.get('Email', 'subject'),
        "email_to":             config.get('Email', 'to'),
        "email_from":           config.get('Email', 'from'),
        "email_cc":             config.get('Email', 'cc')
    }

    # logfile path to point to the Operations_limited drive on zfs
    purge_days = config.get('LogFile', 'retention_days')
    log_file_path = config.get('LogFile', 'path')
    logfile_name = '{}{}_{}.log'.format(log_file_path, config.get('Project Details', 'app_name'), today_date)

    # check to see if log file already exits for the day to avoid duplicate execution
    if not os.path.isfile(logfile_name):
        logging.basicConfig(filename=logfile_name,
                            level=logging.INFO,
                            format='%(asctime)s: %(levelname)-7s: %(name)-30s: %(threadName)-12s: %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')

        logger = logging.getLogger(__name__)

        # checks for console logger option, default value set to 'n' to not run in production
        if con_opt and con_opt in ['y', 'Y']:
            console_logger()

        logger.info("Process Start - Weekly HHID Check, Campaign Management - {}\n".format(today_date))

        # create CM-SSID object and launch the process manager
        cm_ssid_match = MobileIDMatchManager(config_params)
        cm_ssid_match.process_manager()

        # search logfile directory for old log files to purge
        #cm_ssid_match.purge_files(purge_days, log_file_path)


if __name__ == '__main__':
    # prompt user for use of console logging -> for use in development not production
    ans = input("\nWould you like to enable a console logger for this run?\n Please enter y or n:\t")
    print()
    main(ans)
