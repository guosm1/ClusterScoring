import json
import os
import requests

class Config:
  def __init__(self, path=None):
    self.config_file_path = path
    if path is not None:
      self.update_config()

  def get_cluster_metric_path(self):
    return self.cluster_metric_path

  def set_cluster_metric_path(self, path):
    self.cluster_metric_path = path

  def get_prediction_path(self):
    return self.prediction_path

  def set_prediction_path(self, path):
    self.prediction_path = path

  def get_stat_output_file(self):
    return self.stat_output_file

  def set_stat_output_file(self, path):
    self.stat_output_file = path

  def get_scheduler_summary_path(self):
    return self.scheduler_summary_path

  def set_scheduler_summary_path(self, path):
    self.scheduler_summary_path = path

  def get_scheduler_metric_path(self):
    return self.scheduler_metric_path

  def set_scheduler_metric_path(self, path):
    self.scheduler_metric_path = path

  def get_job_stopped_path(self):
    return self.job_stopped_path

  def set_job_stopped_path(self, path):
    self.job_stopped_path = path

  def get_job_started_path(self):
    return self.job_started_path

  def set_job_started_path(self, path):
    self.job_started_path = path

  def get_job_metric_path(self):
    return self.job_metric_path

  def set_job_metric_path(self, path):
    self.job_metric_path = path

  def get_stat_interval(self):
    return self.stat_interval

  def set_stat_interval(self, interval):
    self.stat_interval = interval

  def get_sys_total_memory(self):
    return self.total_sys_memory

  def set_sys_total_memory(self, size):
    self.total_sys_memory = size

  def get_valid_queue_count(self):
    return self.valid_queue_count

  def set_valid_queue_count(self, count):
    self.valid_queue_count = count

  def get_rest_port(self):
    return self.rest_port

  def set_rest_port(self, port):
    self.rest_port = port

  def get_yarn_config_path(self):
    return self.yarn_config_path

  def set_yarn_config_path(self, path):
    self.yarn_config_path = path

  def set_scheduler_summary_current_path(self, path):
    self.scheduler_summary_current_path = path

  def get_scheduler_summary_current_path(self):
    return self.scheduler_summary_current_path

  def update_es_rest_index(self):
    response1 = requests.get(str(self.es_rest_address) + str(self.es_index))
    if response1.status_code == 404:
      response2 = requests.put(str(self.es_rest_address) + str(self.es_index))
      if response2.status_code != 200:
        raise Exception("Cannot fetch elasticsearch indexes", response2.text)

  def update_config(self):
    with open(self.config_file_path) as f:
      data = json.load(f)

    self.set_job_metric_path(data['job_metric_path'])
    self.set_job_stopped_path(data['job_stopped_path'])
    self.set_job_started_path(data['job_started_path'])
    self.set_scheduler_metric_path(data['scheduler_metric_path'])
    self.set_scheduler_summary_path(data['scheduler_summary_path'])
    self.set_scheduler_summary_current_path(data['scheduler_summary_current_path'])
    self.set_stat_interval(data['stat_interval'])
    self.set_prediction_path(data['prediction_path'])
    self.set_cluster_metric_path(data['cluster_metric_path'])
    self.set_stat_output_file(data['stat_output_file'])
    self.set_sys_total_memory(data['sys_total_memory'])
    self.set_yarn_config_path(data['yarn_config_file'])
    self.es_rest_address = data['es_rest_address']
    self.es_index = data['es_index']
    self.set_rest_port(data['rest_port'])
    self.set_valid_queue_count(data['valid_queue_count'])
    self.update_es_rest_index()

  def display(self):
    print('--------------------------------')
    print('job_metric_path:       \t%s' % self.job_metric_path)
    print('job_stopped_path:      \t%s' % self.job_stopped_path)
    print('job_started_path:      \t%s' % self.job_started_path)
    print('scheduler_metric_path: \t%s' % self.scheduler_metric_path)
    print('scheduler_summary_path:\t%s' % self.scheduler_summary_path)
    print('scheduler_summary_current_path:\t%s' % self.scheduler_summary_current_path)
    print('cluster_metric_path:   \t%s' % self.cluster_metric_path)
    print('prediction_path        \t%s' % self.prediction_path)
    print('stat_interval:         \t%ld' % self.stat_interval)
    print('stat_output_file       \t%s' % self.stat_output_file)
    print('sys_total_memory       \t%ld' % self.total_sys_memory)
    print('yarn_config_path       \t%s' % self.yarn_config_path)
    print('es_rest_address        \t%s' % self.es_rest_address)
    print('es_index               \t%s' % self.es_index)
    print('rest_port              \t%d' % self.rest_port)
    print('valid_queue_count      \t%d' % self.valid_queue_count)
    print('--------------------------------')


def get_mtime(filename):
  info = os.stat(filename)
  return info.st_mtime

if __name__ == '__main__':
  conf = Config('./conf/config.json')
  conf.display()
