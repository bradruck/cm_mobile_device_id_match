# pixel_name_search module
# Module holds the class => MobileSSIDSearchManager - manages the api interface to fetch pixel data for campaigns
# Class responsible for all api related interactions with the pixel builder including the data fetch, json file read
# and search and data collection and organization
#
import json
import requests
from datetime import datetime
import logging


class MobileSSIDSearchManager(object):
    def __init__(self):
        self.key_id = 'id'
        self.key_name = 'name'
        self.key_campaigns = 'campaigns'
        self.start_date = 'startDate'
        self.end_date = 'endDate'
        self.logger = logging.getLogger(__name__)

    # Launch the api call to return a json dictionary to be searched for ssid pixel numbers and corresponding names
    #
    def api_call(self, api_url):
        try:
            response = requests.get(api_url)
        except Exception as e:
            self.logger.error("Failed to create a response object => {}".format(e))
            return None
        else:
            pixel_dict = response.json()
            return pixel_dict

    # Find the mobile ssid number from the api call return, ssid number is the first level down in dictionary, then
    # return a list of all the found ssid numbers and their corresponding names, only pixels are returned that have
    # a start date and a future end date, returns a list of lists
    #
    def mobile_ssid_search(self, pixel_dict):
        pixel_name_list = []
        for k, v in pixel_dict.items():
            # get the api returned pixel number total
            if k == 'pixels':
                # go down one level in dictionary to find campaign specific data
                for item1 in v:
                    mobile_id = []
                    for k1, v1 in item1.items():
                        # find the campaign pixel number in the first level down
                        if k1 == self.key_id:
                            mobile_id.append(str(v1))
                        # find the associated campaign name
                        if k1 == self.key_name:
                            mobile_id.append(v1)
                        # go down one more level in dictionary to find campaign dates
                        if k1 == self.key_campaigns:
                            item2 = v1[0]
                            for k2, v2 in item2.items():
                                if k2 == self.start_date:
                                    # convert start date to a string for query use
                                    start_date = v2.split('T')[0].replace('-', '').strip()
                                    mobile_id.append(start_date)
                                if k2 == self.end_date:
                                    # convert end date to a string for query use
                                    end_date = v2.split('T')[0].replace('-', '').strip()
                                    # test for a future end date, if end date already passed, don't include
                                    if datetime.now().strftime('%Y%m%d') < end_date:
                                        mobile_id.append(end_date)
                    # filter out any results that don't include a start date, it could happen
                    if len(mobile_id) == 4:
                        pixel_name_list.append(mobile_id)
        return pixel_name_list

    # Optional method to load json dictionary from a json file
    #
    @staticmethod
    def json_file_load(json_file="pixel.json"):
        # create a json file
        with open(json_file, 'r') as target:
            data = target.read()
        # load a dictionary from the json file
        pixel_dict = json.loads(data)
        return pixel_dict

    # Optional method to log file print the pixel list, also conducts a check for total number of ssid pixels found and
    # compares to the json dictionary figure for this
    #
    def log_results(self, pixel_dict, pixel_list):
        # get the api return value for number of ssids to check the pixel list results
        for k, v in pixel_dict.items():
            if k == 'totalPixels':
                self.logger.info("The api call returned a total number of {} pixels\n".format(v))

        # print the list of mobile ssid numbers
        self.logger.info("The mobile ssid criteria reduces this list to {} pixels, that have a start date with a "
                         "future end date -\n\n\tThese are listed below:\n".format(len(pixel_list)))
        self.logger.info("Pixel id\t\tStart Date\t\tEnd Date\t\t\t\t\t\t\t\t\tCampaign Name\n")

        for pixel in pixel_list:
            self.logger.info(" {pixel_id}\t\t\t{cam_name}\t\t{start_date}\t\t{end_date}"
                             .format(pixel_id=pixel[0], cam_name=pixel[2], start_date=pixel[3], end_date=pixel[1]))
