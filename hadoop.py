import json
import os
import argparse

from config_util import ConfigUtil
from time_util import TimeUtil
from urllib2 import urlopen as urlopen
from urllib2 import URLError as urlerror
from logger_util import get_logger
from file_operator import FileOperator
from pandas.core.frame import DataFrame
import datetime


PWD = os.getcwd()
CONFIG_FILE = PWD + "/conf/properties.conf"
FLAGS = None
logger = get_logger(refresh=True)
hadoop_util = None

class HadoopUtil(object):
  def __init__(self, file_path):
    self.confing_util = ConfigUtil(CONFIG_FILE)
    self.hadoop_url = self.confing_util.get_options("url", "hadoop_url")
    self.file_path = file_path
    self.application_url = self.confing_util.get_options("url", "application_url")
    self.job_metrics = self.confing_util.get_options("job", "job_metrices")
    self.job_url = self.confing_util.get_options("url", "job_url")
    self.memcpu_info = {}

  def get_cluster_information(self):
    logger.info("start get_cluster_information")
    url = self.hadoop_url + "metrics"
    write_header = True
    cluster_file = os.path.join(self.file_path, "cluster.csv")
    if FileOperator.file_exits(cluster_file):
      write_header = False
    results = urlopen(url, timeout=2000).read()
    results = [json.loads(results)["clusterMetrics"]]
    self.memcpu_info["memory"] = results[0].get('totalMB', 0)
    self.memcpu_info["vCores"] = results[0].get('totalVirtualCores', 0)
    headers = results[0].keys()
    FileOperator.write_to_csv(results, cluster_file, headers=headers, write_header=write_header, model="a+")
    self.get_applications_information()

  def get_scheduler_info(self, running_application):
    logger.info("start get_scheduler_info")
    apps = running_application.copy(deep=True)

    apps = apps.groupby('queue')['allocatedMB', 'allocatedVCores'].sum()
    apps['queueName'] = apps.index
    apps.insert(0, 'totalMemory', self.memcpu_info['memory'])
    apps.insert(0, 'totalCpu', self.memcpu_info['vCores'])
    apps.insert(0, 'memory', apps['allocatedMB'] / apps['totalMemory'])
    apps.insert(0, 'vCores', apps['allocatedVCores'] / apps['totalCpu'])

    scheduler_file = os.path.join(self.file_path, "scheduler_summary.csv")
    scheduler_file1 = os.path.join(self.file_path, "scheduler_summary_current.csv")
    write_header = True
    if FileOperator.file_exits(scheduler_file):
      write_header = False
    apps.to_csv(scheduler_file, header=write_header, index=False, mode="a+")
    apps.to_csv(scheduler_file1, header=True, index=False, mode="w")

    logger.info("start get_cluster_scheduler")
    url = self.hadoop_url + "scheduler"
    scheduler_file2 = os.path.join(self.file_path, "scheduler_metric.csv")

    results = urlopen(url, timeout=2000).read()
    results = json.loads(results)
    results = results['scheduler']['schedulerInfo']['queues']['queue']
    headers = results[0].keys()
    for j in results:
      if j.has_key('queues'):
        del j['queues']
    FileOperator.write_to_csv(results, scheduler_file2,headers=headers, model="w+")

  @staticmethod
  def request_url(url):
    try:
      result = urlopen(url, timeout=2000).read()
    except urlerror as error:
      raise urlerror("urlopen {0} error:{1}".format(url, error.reason))
    else:
      return result

  def get_applications_information(self):
    logger.info("start get_application_information")
    hadoop_rest_url = self.hadoop_url + "apps?"
    finished_app_file = os.path.join(self.file_path, "finishedapp.csv")
    running_app1_file = os.path.join(self.file_path, "runningapp1.csv")
    running_app2_file = os.path.join(self.file_path, "runningapp2.csv")

    finished_data = hadoop_util.request_url(format(hadoop_rest_url + "states=finished&finishedTimeEnd={0}".format(TIME_BEGIN - FLAGS.time_period * 1000)))
    running_data = hadoop_util.request_url(format(hadoop_rest_url + "states=running"))
    try:
      finished_data_list = json.loads(finished_data)['apps']['app']
      finished_data_frame = DataFrame(finished_data_list)
      finished_data_frame = finished_data_frame[
        (finished_data_frame['state'] == 'FINISHED') |
        (finished_data_frame['state'] == 'finished')
        ]
    except KeyError as error:
      logger.error("key error {0}".format(error))
    except TypeError:
      logger.warn('did ont get any data finished apps use:{0}'.format((hadoop_rest_url + "states=finished&finishedTimeEnd={0}".format(TIME_BEGIN - FLAGS.time_period * 1000))))
    except Exception as error:
      logger.error(error)
    else:
      finished_data_frame.to_csv(finished_app_file, index=False)
      # self.get_scheduler_info(finished_data_frame)

    try:
      running_data_list = json.loads(running_data)['apps']['app']
      running_data_frame = DataFrame(running_data_list)
      running_data_frame = running_data_frame[
        (running_data_frame['state'] == 'RUNNING')
        | (running_data_frame['state'] == 'running')
        ]
      running_data1 = running_data_frame[
        running_data_frame['startedTime'] <= (TIME_BEGIN - FLAGS.time_period * 1000)
        ]
      running_data2 = running_data_frame[
        running_data_frame['startedTime'] > (TIME_BEGIN - FLAGS.time_period * 1000)
        ]
    except KeyError as error:
      logger.error("key error {0}".format(error))
    except TypeError:
      logger.warn('did ont get any data running apps use:{0}'.format(hadoop_rest_url + "states=running"))
    except Exception as error:
      logger.error(error)
    else:
      running_data1.to_csv(running_app1_file, index=False)
      running_data2.to_csv(running_app2_file, index=False)
      self.get_scheduler_info(running_data_frame)

  def get_commonjobs_information(self):
    logger.info("start get_commonjobs_information")
    commonjob_file = os.path.join(self.file_path, "commonjob.csv")

    result = hadoop_util.request_url(self.job_url)

    result = json.loads(result)["jobs"]
    if not result:
      return
    result = result["job"]
    headers = result[0].keys()
    FileOperator.write_to_csv(result, commonjob_file, headers=headers)


def main():
  print('-----------------------', datetime.datetime.now())
  sw = {
    'w': lambda: TimeUtil.get_time_weeks(FLAGS.time_interval),
    'd': lambda: TimeUtil.get_time_days(FLAGS.time_interval),
    'h': lambda: TimeUtil.get_time_hours(FLAGS.time_interval),
    'm': lambda: TimeUtil.get_time_minutes(FLAGS.time_interval, TIME_END),
    's': lambda: TimeUtil.get_time_seconds(FLAGS.time_interval),
  }
  if FLAGS.time_interval > 0:
    global TIME_BEGIN
    global TIME_END
    TIME_BEGIN, TIME_END = sw[FLAGS.time_format]()
  print(TIME_BEGIN, TIME_BEGIN - FLAGS.time_period * 1000)

  hadoop_util.get_cluster_information()
  hadoop_util.get_commonjobs_information()
  # response = requests.get("http://127.0.0.1:5001/update/all")
  # if response.status_code == 200:
  #   print("success request http://127.0.0.1:5001/update/all")
    # response = requests.get('http://127.0.0.1:5002/train_lstm')
    # if response.status_code == 200:
    #   print("success request http://127.0.0.1:5002/train_lstm")
    # else:
    #   print("fail request http://127.0.0.1:5002/train_lstm")
  # else:
  #   print("fail request http://127.0.0.1:5001/update/all")

TIME_BEGIN = None
TIME_END = None

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.register("type", "bool", lambda v: v.lower() == "true")
  parser.add_argument(
    "--file_path",
    type=str,
    default="./output/",
    help="the output file path."
  )
  parser.add_argument(
    "--time_format",
    type=str,
    choices=['w', 'd', 'h', 'm', 's'],
    default='m',
    help="w: week, d:day, h:hour, m:minutes, s:second"
  )
  parser.add_argument(
    "--time_interval",
    type=int,
    default=5,
    help="to collector job's information which job's finished time begin "
         "before now.time_format:m , time_interval:20 means collectors "
         "job's information which finished in lasted 20 minutes, "
         "if time_interval < 0 then collecotrs all"
  )
  parser.add_argument(
    "--states",
    type=str,
    choices=["finished", "accepted", "running"],
    default='running',
    help="the job's state"
  )
  parser.add_argument(
    "--time_period",
    type=int,
    default=300,
    help="the scripts run's time period"
  )
  FLAGS = parser.parse_args()
  FileOperator.path_exits(FLAGS.file_path)

  hadoop_util = HadoopUtil(
    FLAGS.file_path,
  )
  # t = threading.Timer(
  #   FLAGS.time_period, main)
  # t.start()
  main()
