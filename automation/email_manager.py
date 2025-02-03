# weekly_emailer module
# Module holds the class => WeeklyEmailManager - manages the email creation and the smtp interface
# Class responsible for all email related management
#
from smtplib import SMTP
from email.message import EmailMessage
from io import StringIO
import logging


class EmailManager(object):
    def __init__(self, pixel, subject, to_address, from_address, cc):
        if pixel:
            self.pixel = pixel
            self.text = "Campaign Management,\n\n" + \
                        "There seems to be a problem locating the Jira ticket. Please find details below:\n\n" + \
                        "Pixel: " + self.pixel[0] + "\n\n" + \
                        "Campaign Name: " + self.pixel[1] + "\n\n" + \
                        "Start Date: " + self.pixel[2] + "\n\n" + \
                        "End Date: " + self.pixel[3] + "\n\n" + \
                        "Thanks,\n" + \
                        "CI Team"
        else:
            self.pixel = None
            self.text = "Campaign Management,\n\n" + \
                        "There were no pixels to run today.\n" + "\n\n" + \
                        "Thanks,\n" + \
                        "CI Team"
        self.logger = logging.getLogger(__name__)
        self.msg = ""
        self.attachment = StringIO()
        self.subj = subject
        self.to_address = to_address
        self.from_address = from_address
        self.cc = cc

    # Create the email in a text format then send via smtp, finally save the email as a StringIO file and return
    #
    def cm_emailer(self):
        try:
            # Simple Text Email
            self.msg = EmailMessage()
            self.msg['Subject'] = self.subj
            self.msg['From'] = self.from_address
            self.msg['To'] = self.to_address
            self.msg['Cc'] = self.cc

            # Message Text
            self.msg.set_content(self.text)

            # Send Email
            with SMTP('mailhost.valkyrie.net') as smtp:
                smtp.send_message(self.msg)

        except Exception as e:
            self.logger.error = ("Email failed for pixel {} => {}".format(self.pixel[0], e))

        else:
            self.logger.warning("An alert email for pixel {} has been sent.".format(self.pixel[0]))

    # Create the email in a text format then send via smtp, finally save the email as a StringIO file and return
    #
    def no_pixel_emailer(self):
        try:
            # Simple Text Email
            self.msg = EmailMessage()
            self.msg['Subject'] = self.subj
            self.msg['From'] = self.from_address
            self.msg['To'] = self.to_address
            self.msg['Cc'] = self.cc

            # Message Text
            self.msg.set_content(self.text)

            # Send Email
            with SMTP('mailhost.valkyrie.net') as smtp:
                smtp.send_message(self.msg)

        except Exception as e:
            self.logger.error = ("Email failed for no pixels => {}".format(e))

        else:
            self.logger.warning("An alert email for no pixels has been sent.")
