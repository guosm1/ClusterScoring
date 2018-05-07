import argparse
import conf
import datainput
import resource_manager
import train_lstm
from file_operator import FileOperator

def update_scheduler_info(rmq, cfg):
  scheduler_file = cfg.get_scheduler_metric_path()
  if FileOperator.file_exits(scheduler_file):
    queue_configs = datainput.read_scheduler_csv(scheduler_file)
    for qc in queue_configs:
      queue = rmq.get_queue(qc.name)
      if queue is None:
        print("Unknown queue name", qc.name)
        continue
      else:
        queue.data.update_queue_config(qc)

def update_mu_info(rmq, cfg):
  mu_file = cfg.get_scheduler_summary_current_path()
  count = cfg.get_valid_queue_count()
  queue_mus = datainput.read_memory_usage(mu_file, count)
  for qmu in queue_mus:
    queue = rmq.get_queue(qmu.name)
    if queue is None:
      print("Unknown queue name1", qmu.name)
      continue
    queue.data.add_queue_memory_usage(qmu)

def update_app_stopped_info(rmq, cfg):
  app_file = cfg.get_job_stopped_path()
  if FileOperator.file_exits(app_file):
    jobs = datainput.read_app_stopped_csv(app_file)
    for job in jobs:
      queue = rmq.get_queue(job.name)
      if queue is None:
        print("Unknown queue name2", job.name)
        continue
      queue.data.add_job(job)

def update_app_started_info(rmq, cfg):
  app_file = cfg.get_job_started_path()
  if FileOperator.file_exits(app_file):
    jobs = datainput.read_app_started_csv(app_file)
    for job in jobs:
      queue = rmq.get_queue(job.name)
      if queue is None:
        print("Unknown queue name3", job.name)
        continue
      queue.data.add_job(job)

def update_app_info(rmq, cfg):
  app_file = cfg.get_job_metric_path()
  if FileOperator.file_exits(app_file):
    jobs = datainput.read_app_csv(app_file)
    for job in jobs:
      queue = rmq.get_queue(job.name)
      if queue is None:
        print("Unknown queue name4", job.name)
        continue
      queue.data.add_job(job)

def update_cluster_info(rmq, cfg):
  cluster_file = cfg.get_cluster_metric_path()
  if FileOperator.file_exits(cluster_file):
    total_mb = datainput.read_cluster_csv(cluster_file)
    if total_mb == 0:
      return
    queue = rmq.get_queue('root')
    queue.data.add_totalMb(total_mb)

def update_predict_info(rmq, cfg):
  prediction_file = cfg.get_prediction_path()
  if FileOperator.file_exits(prediction_file):
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
  update_predict_info(rmq, cfg)

def score(rmq, cfg):
  update_all_info(rmq, cfg)
  rmq.score()
  rmq.display_score()
  path = cfg.get_stat_output_file()
  rmq.write_score(path)
  rmq.request_score()

def predict(rmq, cfg):
  try:
    train_lstm.main()
  except Exception:
    pass
  update_all_info(rmq, cfg)
  rmq.predict()
  rmq.display_prediction()
  path = cfg.get_stat_output_file()
  rmq.write_prediction(path)
  rmq.request_prediction()

def start(cfg):
  rmq = resource_manager.parseYarnConfig(cfg.yarn_config_path)
  rmq.set_stat_interval(cfg.get_stat_interval())
  rmq.set_system_memory(cfg.get_sys_total_memory())
  print('Starting to collecting and scoring ... ')
  import restserver
  restserver.start_server(cfg.get_rest_port())

def main(config_path):
  cfg = conf.Config(config_path)
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
