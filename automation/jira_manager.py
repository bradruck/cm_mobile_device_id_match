# jira_manager module
# Module holds the class => JiraManager - manages JIRA ticket interface
# Class responsible for all JIRA related interactions including ticket searching, data pull, file attaching, comment
# posting and field updating.
#
from jira import JIRA
from datetime import datetime, timedelta
import logging


class JiraManager(object):
    def __init__(self, url, jira_token):
        self.tickets = []
        self.jira = JIRA(url, basic_auth=jira_token)
        self.date_range = ""
        self.file_name = ""
        self.advert_field_name = ""
        self.advertiser_name = ""
        self.logger = logging.getLogger(__name__)
        self.today_date = (datetime.now() - timedelta(hours=6)).strftime('%m/%d/%Y')
        self.comment_alert = 'campaignmanagement'
        self.pixel_hhid_match_alert = 'The maid and cookie to hhid matches are now available.'
        self.match_fail_alert = 'The match query failed to return any results for this run.'

    # Searches Jira for all tickets that match the parent ticket query criteria
    #
    def find_tickets(self, jira_type, jira_status, pixel):
        # Query to find corresponding Jira Tickets from pixel id
        self.tickets = []
        jql_query = "project in (CAM) AND Type = " + jira_type + " AND Status in " + jira_status + " AND Pixels ~ " \
                    + pixel[0]
        self.tickets = self.jira.search_issues(jql_query, maxResults=500)
        if len(self.tickets) > 0:
            self.logger.info("Pixel: {pixel}, Jira ticket -> {key}"
                             .format(pixel=pixel[0], key=str(self.tickets[0].key)))
            return self.tickets[0]
        else:
            self.logger.error("Pixel: {pixel}, Jira ticket -> No Jira Ticket found"
                              .format(pixel=pixel[0]))
            return None

    # Searches Jira for parent tickets
    #
    def find_parent_ticket(self, ticket):
        jql_parent_query = "issue in parentIssuesOf(" + ticket + ")"
        parent_ticket = self.jira.search_issues(jql_parent_query, maxResults=500)
        if len(parent_ticket) > 0:
            self.logger.info("Here is the measurement ticket found for Jira pixel ticket -> {key}"
                             .format(key=str(parent_ticket[0].key)))
            return parent_ticket[0].key
        else:
            self.logger.error("There were no measurement tickets found for Jira pixel ticket {key} -> "
                              "No Jira Ticket found".format(key=ticket.key))
            return None

    # Retrieves the Reporter and Lead Analyst from Measurement Ticket
    #
    def ticket_info_pull(self, ticket_no):
        ticket = self.jira.issue(ticket_no)
        reporter = ticket.fields.reporter
        lead_analyst = ticket.fields.customfield_12325
        return reporter, lead_analyst

    # Add a comment on tickets for match creation alert with counts and rate calculations
    #
    def add_match_count_comment(self, ticket, result_dict, reporter, lead_analyst):
        message = ""
        cam_ticket = self.jira.issue(str(ticket[0]))
        if reporter:
            message += """[~{attention1}] """.format(attention1=str(reporter).replace(" ", "."))
        if lead_analyst == 'Debra Eskra':
            message += """[~{attention2}] """.format(attention2='deb.eskra')
        if lead_analyst and lead_analyst != 'Debra Eskra':
            message += """[~{attention2}] """.format(attention2=str(lead_analyst).replace(" ", "."))
        if not reporter and not lead_analyst:
            message += """[~{attention}] """.format(attention=self.comment_alert)

        message += """{match_alert}

                     Pixel =>          *{pixel_id}*
                     Campaign Name =>  *{campaign_name}*
                     
                     ||Match Basis||Rate||
                     |MAIDs|{hashes}|
                     |Cookies|{cookies}|
                     |Full|{full}|

                     ||Matches||Count||
                     |Total Imprs with Hashed Maids|Q = {hashed_chpck}|
                     |Total Imprs w/hashed Maids matched  to a  HH|Q = {hashed_hhid}|
                     |Total Imprs with unhashed MAIDS|Q = {unhashed_chpck}|
                     |Total Imprs w/unhashed MAIDs matched to a HH|Q = {unhashed_hhid}|
                     |Total Imprs with cookie IDs|Q = {cookie_chpck}|
                     |Total Imprs w/cookie IDs matched to a HH|Q = {cookie_hhid}|
                     |Total Imprs with IDs captured|Q = {total_chpck}|
                     |Total Imprs/w IDs matched to a HH|Q = {total_hhid}|

                     """.format(match_alert=self.pixel_hhid_match_alert,
                                hashes=result_dict.get('match_rate_hashes'),
                                cookies=result_dict.get('match_rate_cookies'),
                                full=result_dict.get('match_rate_full'),
                                hashed_chpck='{0:,d}'.format(result_dict.get('hashed_chpck')),
                                hashed_hhid='{0:,d}'.format(result_dict.get('hashed_hhid')),
                                unhashed_chpck='{0:,d}'.format(result_dict.get('unhashed_chpck')),
                                unhashed_hhid='{0:,d}'.format(result_dict.get('unhashed_hhid')),
                                cookie_chpck='{0:,d}'.format(result_dict.get('cookie_chpck')),
                                cookie_hhid='{0:,d}'.format(result_dict.get('cookie_hhid')),
                                total_chpck='{0:,d}'.format(result_dict.get('total_chpck')),
                                total_hhid='{0:,d}'.format(result_dict.get('total_hhid')),
                                pixel_id=ticket[1][0],
                                campaign_name=ticket[1][1]
                                )
        self.jira.add_comment(issue=cam_ticket, body=message)

    # Add a comment to ticket informing of a match fail
    #
    def add_match_fail_comment(self, ticket, reporter, lead_analyst):
        message = ""
        cam_ticket = self.jira.issue(str(ticket[0]))
        if reporter:
            message += """[~{attention1}] """.format(attention1=str(reporter).replace(" ", "."))
        if lead_analyst:
            message += """[~{attention2}] """.format(attention2=str(lead_analyst).replace(" ", "."))
        elif not reporter and not lead_analyst:
            message += """[~{attention}] """.format(attention=self.comment_alert)

        message += """{match_fail_comment}
                     
                     Pixel =>          *{pixel_id}*
                     Campaign Name =>  *{campaign_name}*
                     
                     """.format(match_fail_comment=self.match_fail_alert,
                                pixel_id=ticket[1][0],
                                campaign_name=ticket[1][1]
                                )
        self.jira.add_comment(issue=cam_ticket, body=message)

    # Change the field 'labels' in the ticket to ???
    #
    @staticmethod
    def update_field_value(ticket):
        ticket.fields.labels.append(u'???')
        ticket.update(fields={'labels': ticket.fields.labels})

    # Ends the current JIRA session
    #
    def kill_session(self):
        self.jira.kill_session()
