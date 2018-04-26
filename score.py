import argparse
import os

import conf
import datainput
import resource_manager

FLAGS = None


def update_filepath(cfg):
  cluster_file = cfg.get_cluster_metric_path()
  scheduler_file = cfg.get_scheduler_metric_path()
  app_file = cfg.get_job_metric_path()
  return cluster_file, scheduler_file, app_file


# get the last update time of the file
def get_mtime(filename):
  info = os.stat(filename)
  return info.st_mtime


# parse csv file which includes queue info gathered from yarn scheduler
def update_scheduler_info(rmq, cfg):
  scheduler_file = cfg.get_scheduler_metric_path()
  queue_configs = datainput.read_scheduler_csv(scheduler_file)
  for qc in queue_configs:
    queue = rmq.get_queue(qc.name)
    if queue is None:
      print("Unknown queue name", qc.name)
      continue
    queue.data.update_queue_config(qc)
    # print(qc.name)


# parse csv file which includes memory usage info of each queue
def update_mu_info(rmq, cfg):
  mu_file = cfg.get_scheduler_summary_path()
  count = cfg.get_valid_queue_count()
  queue_mus = datainput.read_memory_usage(mu_file, count)
  for qmu in queue_mus:
    queue = rmq.get_queue(qmu.name)
    if queue is None:
      print("Unknown queue name1", qmu.name)
      continue
    queue.data.add_queue_memory_usage(qmu)


# parse csv file which includes stopped app info gathered from yarn scheduler
def update_app_stopped_info(rmq, cfg):
  app_file = cfg.get_job_stopped_path()
  jobs = datainput.read_app_stopped_csv(app_file)
  for job in jobs:
    queue = rmq.get_queue(job.name)
    if queue is None:
      print("Unknown queue name2", job.name)
      continue
    queue.data.add_job(job)


# parse csv file which includes new started app info gathered from yarn scheduler
def update_app_started_info(rmq, cfg):
  app_file = cfg.get_job_started_path()
  jobs = datainput.read_app_started_csv(app_file)
  for job in jobs:
    queue = rmq.get_queue(job.name)
    if queue is None:
      print("Unknown queue name3", job.name)
      continue
    queue.data.add_job(job)


# parse csv file which includes app info gathered from yarn scheduler
def update_app_info(rmq, cfg):
  app_file = cfg.get_job_metric_path()
  jobs = datainput.read_app_csv(app_file)
  for job in jobs:
    queue = rmq.get_queue(job.name)
    if queue is None:
      print("Unknown queue name4", job.name)
      continue
    queue.data.add_job(job)


# parse csv file which includes prediction info
def update_prediction_info(rmq, cfg):
  prediction_file = cfg.get_prediction_file()
  wishes = datainput.read_prediction_csv(prediction_file)
  for wish in wishes:
    queue = rmq.get_queue(wish.name)
    if queue is None:
      print("Unknown queue name5", wish.name)
      continue
    queue.data.update_wish(wish)


# parse csv file which includes the total memory of root queue
def update_cluster_info(rmq, cfg):
  cluster_file = cfg.get_cluster_metric_path()
  ts = get_mtime(cluster_file)
  totalMb = datainput.read_cluster_csv(cluster_file)
  if totalMb == 0:
    return
  queue = rmq.get_queue('root')
  queue.data.add_totalMb(totalMb)


# parse csv file which includes the prediction of each leaf queue
def update_predict_info(rmq, cfg):
  prediction_file = cfg.get_prediction_path()
  queue_wishes = datainput.read_prediction_csv(prediction_file)
  for wish in queue_wishes:
    queue = rmq.get_queue(wish.name)
    if queue is None:
      print("Unknown queue name6", wish.name)
      continue
    queue.data.update_queue_wish(wish)


def update_all_info(rmq, cfg):
  update_scheduler_info(rmq, cfg)
  update_mu_info(rmq, cfg)
  update_cluster_info(rmq, cfg)
  update_app_info(rmq, cfg)
  update_app_stopped_info(rmq, cfg)
  update_app_started_info(rmq, cfg)


def score(rmq, cfg):
  rmq.score()
  rmq.display_score()
  path = cfg.get_stat_output_file()
  rmq.write_score(path)


def predict(rmq, cfg):
  update_predict_info(rmq, cfg)
  rmq.predict()
  rmq.display_prediction()
  path = cfg.get_stat_output_file()
  rmq.write_prediction(path)


def start(cfg):
  cluster_file, scheduler_file, app_file = update_filepath(cfg)
  rmq = resource_manager.parseYarnConfig(cfg.yarn_config_path)
  rmq.set_stat_interval(cfg.get_stat_interval())
  rmq.set_system_memory(cfg.get_sys_total_memory())
  print('Starting to collecting and scoring ... ')
  import restserver
  restserver.start_server(cfg.get_rest_port())

def main(config_path):
  cfg = conf.Config(config_path)
  cfg.update_config()
  start(cfg)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.register("type", "bool", lambda v: v.lower() == "true")
  parser.add_argument(
    "--config_file",
    type=str,
    default="./conf/config.json",
    help="The path of config file, in json format"
  )

  FLAGS = parser.parse_args()
  config_path = FLAGS.config_file

  main(config_path)
