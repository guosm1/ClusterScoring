#!/usr/bin/python2.7  
# -*- coding: utf-8 -*- 

# 数据 输入与预处理相关功能
import pandas as pd
import numpy as np
from utils import Job, QueueConfig, QueueWish, QueueMemoryUsage


def read_scheduler_csv(path):
  df = pd.read_csv(path)
  cols = df.columns.tolist()
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
    """
    for i in range(df.shape[1]):
        print(i, cols[i], row[i])
    print('-----------------------------------')
    """
  return confs


def read_memory_usage(path, count):
  df = pd.read_csv(path)
  mus = []
  for i in range(count):
    mu = QueueMemoryUsage()
    row = df.iloc[i - count]
    mu.name = row['queueName']
    mu.mu = row['memory']
    # mu.display()
    mus.append(mu)
  return mus


# TOTOL_MB_INDEX = 15
def read_cluster_csv(path):
  obj = pd.read_csv(path)
  # cols = obj.columns.tolist()
  # print(cols)
  total_mb = 0
  total_mb = obj.iloc[0]['totalMB']  # the csv file should contain only one data line
  return total_mb


def read_app_csv(path):
  df = pd.read_csv(path)
  # cols = df.columns.tolist()
  jobs = []
  for index, row in df.iterrows():
    job = Job()
    job.name = row['queue']
    job.run_time = row['elapsedTime'] * 0.001
    job.memory_seconds = row['allocatedMB'] * 300  # five minute per sampling
    jobs.append(job)
    """
    for i in range(df.shape[1]):
        print(i, cols[i], row[i])
    print( '-----------------------------------')
    """
  return jobs


def read_app_stopped_csv(path):
  df = pd.read_csv(path)
  # cols = df.columns.tolist()
  # print(cols)
  jobs = []
  for index, row in df.iterrows():
    job = Job()
    job.name = row['queue']
    # job.wait_time = np.random.randint(50) #暂时用随机数模拟
    job.run_time = row['elapsedTime'] * 0.001
    job.memory_seconds = row['memorySeconds']
    if job.run_time > 150:
      job.memory_seconds = job.memory_seconds * 150 / job.run_time
      # print("STOPPED: ", job.memory_seconds)
    jobs.append(job)
    """
    for i in range(df.shape[1]):
        print(i, cols[i], row[i])
    print( '-----------------------------------')
    """
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
    """
    for i in range(df.shape[1]):
        print(i, cols[i], row[i])
    print( '-----------------------------------')
    """
  return jobs


# 0, time_step
# 1, mem_predicion
# 2, cpu_prediction
# 3, queue_name
def read_prediction_csv(path):
  df = pd.read_csv(path)
  cols = df.columns.tolist()
  wishes = []
  for index, row in df.iterrows():
    wish = QueueWish()
    wish.vmem = row[1]
    wish.name = row[2]
    wishes.append(wish)
  return wishes
