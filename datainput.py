import pandas as pd
from utils import Job, QueueConfig, QueueWish, QueueMemoryUsage

def read_scheduler_csv(path):
  df = pd.read_csv(path)
  confs = []
  for index, row in df.iterrows():
    queue_config = QueueConfig()
    queue_config.capacity = float(row['capacity'])
    queue_config.max_capacity = float(row['maxCapacity'])
    queue_config.abs_capacity = float(row['absoluteCapacity'])
    queue_config.pending = row['numPendingApplications']
    queue_config.name = row['queueName']
    queue_config.state = row['state']
    confs.append(queue_config)
  return confs


def read_memory_usage(path, count):
  print path
  df = pd.read_csv(path)
  mus = []
  for i in range(count):
    mu = QueueMemoryUsage()
    row = df.iloc[i - count]
    mu.name = row['queueName']
    mu.mu = row['memory']
    mus.append(mu)
  return mus


def read_cluster_csv(path):
  obj = pd.read_csv(path)
  total_mb = obj.iloc[0]['totalMB']  # the csv file should contain only one data line
  return total_mb


def read_app_csv(path):
  df = pd.read_csv(path)
  jobs = []
  for index, row in df.iterrows():
    job = Job()
    job.name = row['queue']
    job.run_time = row['elapsedTime'] * 0.001
    job.memory_seconds = row['allocatedMB'] * 300  # five minute per sampling
    jobs.append(job)
  return jobs


def read_app_stopped_csv(path):
  df = pd.read_csv(path)
  jobs = []
  for index, row in df.iterrows():
    job = Job()
    job.name = row['queue']
    job.run_time = row['elapsedTime'] * 0.001
    job.memory_seconds = row['memorySeconds']
    if job.run_time > 150:
      job.memory_seconds = job.memory_seconds * 150 / job.run_time
    jobs.append(job)
  return jobs


def read_app_started_csv(path):
  df = pd.read_csv(path)
  jobs = []
  for index, row in df.iterrows():
    job = Job()
    job.name = row['queue']
    job.run_time = row['elapsedTime'] * 0.001
    job.memory_seconds = row['memorySeconds']
    jobs.append(job)
  return jobs


def read_prediction_csv(path):
  df = pd.read_csv(path, header=None)
  wishes = []
  for index, row in df.iterrows():
    wish = QueueWish()
    wish.vmem = row[0]
    wish.name = row[1]
    wishes.append(wish)
  return wishes
