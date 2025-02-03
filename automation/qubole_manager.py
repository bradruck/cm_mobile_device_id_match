# qubole_manager module
# Module holds the class => QuboleManager - manages Qubole search interface
# Class responsible for all Qubole related interactions including query launch and results retrieval
#
from qds_sdk.commands import *
import io
from contextlib import redirect_stdout
import logging


class QuboleManager(object):
    def __init__(self, name, qubole_token, cluster_label, query):
        self.name = name
        self.qubole_token = qubole_token
        self.cluster_label = cluster_label
        self.query = query
        self.logger = logging.getLogger(__name__)

    # Launches query, collects,converts and returns results
    #
    def get_results(self):
        Qubole.configure(api_token=self.qubole_token)
        try:
            # launches the qubole query
            resp = self.launch_query()

        except Exception as e:
            self.logger.error("Query run failed => {}".format(e))

        else:
            clean_results = []
            output = io.BytesIO()
            with redirect_stdout(output):
                resp.get_results(fp=output, inline=True)
                output.seek(0)
                # takes output from quoble, converts to ascii, strips whitespace then splits to form a list
                results = (output.read()).decode("utf-8").strip().split('\t')
                # remove any non-numeric characters from list elements
                for item in results:
                    item = ''.join(n for n in item if n.isdigit())
                    # finally, remove any blank items left in list
                    if item is not '':
                        clean_results.append(int(item))

            return clean_results

    # Launches query and checks periodically for completion
    #
    def launch_query(self):
        done = False
        attempt = 1
        while not done and attempt <= 3:
            resp = HiveCommand.create(query=self.query, retry=3, label=self.cluster_label, name=", ".join(self.name))
            final_status = self.watch_status(resp.id)
            done = HiveCommand.is_success(final_status)
            attempt += 1
            if done:
                return resp

    # Monitors the Hive query status, returns when finished
    #
    @ staticmethod
    def watch_status(job_id):
        cmd = HiveCommand.find(job_id)
        while not HiveCommand.is_done(cmd.status):
            time.sleep(Qubole.poll_interval)
            cmd = HiveCommand.find(job_id)
        return cmd.status
