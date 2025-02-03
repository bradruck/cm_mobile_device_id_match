# mobile_id_match_manager module
# Module holds the class => MobileIDMatchManager - manages the Mobile ID Matching Process
# Class responsible for overall program management
#
from datetime import datetime, timedelta
import time
import os
import json
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing_logging import install_mp_handler
import logging

from jira_manager import JiraManager
from qubole_manager import QuboleManager
from hhid_pixel_query import MaidHHIDMatch
from pixel_name_search import MobileSSIDSearchManager
from email_manager import EmailManager

today_date = (datetime.now() - timedelta(hours=6)).strftime('%Y%m%d-%H%M%S')


class MobileIDMatchManager(object):
    def __init__(self, config_params):
        self.jira_url = config_params['jira_url']
        self.jira_token = config_params['jira_token']
        self.jira_pars = JiraManager(self.jira_url, self.jira_token)
        self.jql_type = config_params['jql_type']
        self.jql_status = config_params['jql_status']
        self.qubole_token = config_params['qubole_token']
        self.cluster_label = config_params['cluster_label']
        self.api_url = config_params['api_url']
        self.results_json_path = config_params['results_json_path']
        self.results_json_name = config_params['results_json_name']
        self.email_subject = config_params['email_subject']
        self.email_to = config_params['email_to']
        self.email_from = config_params['email_from']
        self.email_cc = config_params['email_cc']
        self.results_file_name = '{}{}_{}.json'.format(self.results_json_path, self.results_json_name, today_date)
        self.results_dict = {}
        self.issues = []
        self.tickets = []
        self.logger = logging.getLogger(__name__)

    # Manages the overall automation
    #
    def process_manager(self):
        try:
            # creates a list of lists for each found pixel id, includes campaign - name, start and end dates
            pixel_list = self.api_manager()
        except Exception as e:
            self.logger.error("Pixel-builder api call and dictionary search failed => {}".format(e))
        else:
            # check for an empty pixel list, if not, process the list
            if len(pixel_list) != 0:
                self.logger.info("\n")
                self.logger.info("\n\nThese are the pixel ids with their corresponding Jira-tickets-ids (if found):\n")

                #  create the iterable required for the concurrency processing, jira ticket search is done here
                self.iterable_creator(pixel_list)

                # launch the qubole queries concurrently, includes results handling and dictionary addition as well as
                # jira ticket comment posting, first check for matching jira tickets, if none then bypass concurrency
                if [x for x in self.tickets if x[0] is not None]:
                    self.pixel_concurrency_manager(self.tickets)
                else:
                    self.logger.info("\n")
                    self.logger.warning("There were no matching Jira tickets for the pixels found.\n")

                # finally, if any, write the results of the entire run dictionary to zfs located json file
                if self.results_dict:
                    self.json_file_write()
                else:
                    self.logger.warning("Since there were no results from the run, no json file was created.\n")
            else:
                # if no pixels found, send email to campaign management to notify
                self.emailer(None)
                # writes log error and exits program
                self.logger.error("\n\nThere were no pixel ids returned from the api call.\n")

    # Manages the api class, instance creation and function calls
    #
    def api_manager(self):
        # create api search object
        api_manager = MobileSSIDSearchManager()
        pixel_dict = api_manager.api_call(self.api_url)

        # confirm api call returned results, search to reduce pixel list to active campaigns then log results
        if pixel_dict is not None:
            pixel_list = api_manager.mobile_ssid_search(pixel_dict)
            api_manager.log_results(pixel_dict, pixel_list)
            return pixel_list
        else:
            return None

    # Creates an iterable list of lists with pixel information and jira ticket number for the concurrency requirement
    #
    def iterable_creator(self, pixel_list):
        #  iterate through the found pixel list
        for pixel in pixel_list:
            # find the jira ticket that corresponds to the pixel
            self.issues = self.jira_pars.find_tickets(self.jql_type, self.jql_status, pixel)
            # add jira ticket key to complete the concurrency manager iterable
            if self.issues is not None:
                self.tickets.append([self.issues.key, pixel])
            else:
                # if no jira ticket found, send email to campaign management to alert a potential problem
                self.emailer(pixel)

    # Run the qubole query for each of the pixel numbers, up to 10 active threads at a time
    #
    def pixel_concurrency_manager(self, tickets):
        self.logger.info("\n")
        self.logger.info("Beginning the maid to hhid match concurrent processing")
        self.logger.info("\n")

        # activate concurrency logging handler
        install_mp_handler(logger=self.logger)
        # set the logging level of urllib3 to "ERROR" to filter out 'warning level' logging message deluge
        logging.getLogger("urllib3").setLevel(logging.ERROR)

        # launches a thread for each of the tickets
        pixel_pool = ThreadPool(processes=len(tickets))
        try:
            pixel_pool.map(self.query_manager, tickets)
            pixel_pool.close()
            pixel_pool.join()
        except Exception as e:
            self.logger.error("Pixel-HHID Concurrency run failed => {}".format(e))
        else:
            self.logger.info("\n")
            self.logger.info("Concluded the maid to hhid match concurrent processing\n")

    # Runs a twice a week match and returns results
    #
    def query_manager(self, ticket):
        # checks that the required ticket information exists, else bypasses Qubole
        if ticket:
            # set the logging level of Qubole to "WARNING" to filter out 'info level' logging message deluge
            logging.getLogger("qds_connection").setLevel(logging.WARNING)

            query = MaidHHIDMatch()
            qubole = QuboleManager((ticket[0], "".join(str(ticket[1][0]))), self.qubole_token,
                                   self.cluster_label, query.unified_impressions_query(ticket[1][0],
                                   ticket[1][2]))
            query_result = qubole.get_results()

            self.results_manager(ticket, query_result)

    # Copies the results to a dictionary and then adds this to a run level dictionary for json file creation
    #
    def results_manager(self, ticket, query_result):
        # filter out the results where there are no counts available, sets the result_dict to 'None' -
        # there is no dictionary of results saved for all-zero count searches and no results posted to ticket
        result_dict = {}
        self.logger.info("The pixel_id is {} and the campaign name is {}".format(ticket[1][0], ticket[1][1]))
        try:
            # add query results to dictionary
            result_dict['hashed_chpck'] = query_result[0]
            result_dict['hashed_hhid'] = query_result[1]
            result_dict['unhashed_chpck'] = query_result[2]
            result_dict['unhashed_hhid'] = query_result[3]
            result_dict['cookie_chpck'] = query_result[4]
            result_dict['cookie_hhid'] = query_result[5]
            result_dict['total_chpck'] = (query_result[0] + query_result[2] + query_result[4])
            result_dict['total_hhid'] = (query_result[1] + query_result[3] + query_result[5])

            # calculate the rates, filtering for zero values in denominators
            if (int(query_result[0]) + int(query_result[2])) == 0 and int(query_result[4]) == 0:
                # return 'None' for all the rates since all denominators will be zero
                result_dict['match_rate_hashes'] = 'None'
                result_dict['match_rate_cookies'] = 'None'
                result_dict['match_rate_full'] = 'None'
                self.logger.warning("The match results have zero values for both maid and cookie dlx_chpck counts "
                                    "for pixel: {}".format(ticket[1][0]))

            elif (int(query_result[0]) + int(query_result[2])) == 0:
                # return 'None' for hashes and total but return a value for the cookies
                result_dict['match_rate_hashes'] = 'None'
                result_dict['match_rate_cookies'] = float(format(float(query_result[5]) / float(query_result[4]),
                                                                 '.3f'))
                result_dict['match_rate_full'] = 'None'
                self.logger.warning("The match results have zero values for maid dlx_chpck counts "
                                    "for pixel: {}".format(ticket[1][0]))

            elif int(query_result[4]) == 0:
                # return 'None' for cookies and total but return a value for the hashes
                result_dict['match_rate_hashes'] = float(format((float(query_result[1]) + float(query_result[3])) /
                                                                (float(query_result[0]) + float(query_result[2])),
                                                                '.3f'))
                result_dict['match_rate_cookies'] = 'None'
                result_dict['match_rate_full'] = 'None'
                self.logger.warning("The match results have zero values for cookie dlx_chpck counts "
                                    "for pixel: {}".format(ticket[1][0]))

            else:
                # calculate the rates required for analysis and add to dictionary
                result_dict['match_rate_hashes'] = float(format((float(query_result[1]) + float(query_result[3])) /
                                                                (float(query_result[0]) + float(query_result[2])),
                                                                '.3f'))
                result_dict['match_rate_cookies'] = float(format(float(query_result[5]) / float(query_result[4]),
                                                                 '.3f'))
                result_dict['match_rate_full'] = float(format((float(query_result[1]) + float(query_result[3]) +
                                                               float(query_result[5])) / (float(query_result[0]) +
                                                                                          float(query_result[2]) +
                                                                                          float(query_result[4])),
                                                              '.3f'))
                self.logger.info("The match results were successfully created for pixel: {}".format(ticket[1][0]))

        except Exception as e:
            self.logger.error("Either there are no results or there is a problem with the data "
                              "for pixel {} => {}".format(ticket[1][0], e))
            result_dict = None

        else:
            # check the count results to eliminate all-zeros query results from the json run file, and trigger a
            # different comment post
            if sum({k: v for (k, v) in result_dict.items() if isinstance(v, int)}.values()) == 0:
                result_dict = None
            else:
                self.results_dict[ticket[1][0]] = result_dict

        # call the measurement ticket manager to collect measurement ticket information
        meas_ticket, reporter, lead_analyst = self.parent_ticket_manager(ticket)

        # comment out the lines below for test runs without jira ticket comment posting
        self.comments_manager(ticket, result_dict, None, None)
        self.comments_manager(meas_ticket, result_dict, reporter, lead_analyst)

        self.logger.info("End of thread\n")

    # Finds the Measurement (Parent) ticket and collects reporter and lead analyst names from this ticket
    #
    def parent_ticket_manager(self, ticket):
        # find the parent ticket and associated reporter and lead analyst
        meas_ticket = [self.jira_pars.find_parent_ticket(ticket[0]), [ticket[1][0], ticket[1][1], ticket[1][2],
                                                                      ticket[1][3]]]
        reporter, lead_analyst = self.jira_pars.ticket_info_pull(meas_ticket[0])

        return meas_ticket, reporter, lead_analyst

    # Confirms output of query, posts results to Jira ticket
    #
    def comments_manager(self, ticket, result, reporter, lead_analyst):
        # check for results, then check to see if ticket is pixel or measurement level
        if result is not None:
            if reporter is None and lead_analyst is None:
                self.jira_pars.add_match_count_comment(ticket, result, None, None)
                self.logger.info("The maid, cookie and total counts along with match rates have been added as a comment"
                                 " to Jira Ticket: " + str(ticket[0]))
            else:
                self.jira_pars.add_match_count_comment(ticket, result, reporter, lead_analyst)
                self.logger.info("The maid, cookie and total counts along with match rates have been added as a comment"
                                 " to Jira Ticket: " + str(ticket[0]))
        else:
            if reporter is None and lead_analyst is None:
                self.jira_pars.add_match_fail_comment(ticket, None, None)
                self.logger.info("The ticket alert has been added as a comment to Jira Ticket: {}".format(ticket[0]))
            else:
                self.jira_pars.add_match_fail_comment(ticket, reporter, lead_analyst)
                self.logger.info("The ticket alert has been added as a comment to Jira Ticket: {}".format(ticket[0]))

    # Creates the Email Manager instance, launches the emailer module
    #
    def emailer(self, pixel):
        cm_email = EmailManager(pixel, self.email_subject, self.email_to, self.email_from, self.email_cc)
        cm_email.cm_emailer()

    # Writes the run data to a json file as a history repository and potential further processing
    #
    def json_file_write(self):
        try:
            # create json file for results repository, to be stored on zfs1/operations_mounted drive
            with open(self.results_file_name, 'w') as fp:
                json.dump(self.results_dict, fp, indent=4)
        except Exception as e:
            self.logger.error("There was a problem creating the json data file or posting it to "
                              "/zfs1/operations_mounted => {}".format(e))
        else:
            self.logger.info("The results have been posted to: {}".format(self.results_file_name))

    # Checks the log directory for all files and removes those after a specified number of days
    #
    def purge_files(self, purge_days, purge_dir):
        try:
            self.logger.info("\n\t\tRemove {} days old files from the {} directory".format(purge_days, purge_dir))
            now = time.time()
            for file_purge in os.listdir(purge_dir):
                f_obs_path = os.path.join(purge_dir, file_purge)
                if os.stat(f_obs_path).st_mtime < now - int(purge_days) * 86400 and f_obs_path.split(".")[-1] == "log":
                    time_stamp = time.strptime(time.strftime('%Y-%m-%d %H:%M:%S',
                                                             time.localtime(os.stat(f_obs_path).st_mtime)),
                                               '%Y-%m-%d %H:%M:%S')
                    self.logger.info("Removing File [{}] with timestamp [{}]".format(f_obs_path, time_stamp))

                    os.remove(f_obs_path)

        except Exception as e:
            self.logger.error("{}".format(e))
