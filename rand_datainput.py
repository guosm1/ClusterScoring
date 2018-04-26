#!/usr/bin/python2.7  
# -*- coding: utf-8 -*- 

# 数据 输入与预处理相关功能

import pandas as pd
import numpy as np
from utils import Job, QueueConfig, QueueWish


# CAPACITY_INDEX = 1 
# MAX_CAPACITY_INDEX = 3
# ABS_CAPACITY = 4
# QUEUE_NAME_INDEX = 8
# QUEUE_STATE_INDEX = 9
def read_scheduler_csv(path):
  confs = []
  queue = ['spark', 'hive', 'ProgrammerAlliance']
  state = ['RUNNING', 'RUNNING', 'FIXED']
  capacity = [25, 35, 40]
  for i in range(len(queue)):
    queue_config = QueueConfig()
    queue_config.capacity = capacity[i]
    queue_config.max_capacity = capacity[i]
    queue_config.abs_capacity = capacity[i]
    queue_config.pending = np.random.randint(0, 5)
    queue_config.name = queue[i]
    queue_config.state = state[i]
    confs.append(queue_config)
    """
    queue_config.display()
    """
  return confs


def read_prediction_csv(path):
  wishes = []
  count = np.random.randint(30, 50)
  queue = ['spark', 'hive', 'ProgrammerAlliance']
  for i in range(count):
    wish = QueueWish()
    wish.name = queue[np.random.randint(0, 3)]
    wish.vmem = np.random.randint(20, 100)
    wish.vcpu = np.random.randint(20, 100)
    wishes.append(wish)
    """
    queue_config.display()
    """
  return wishes


# TOTOL_MB_INDEX = 15
def read_cluster_csv(path):
  total_mb = 16 * 1024
  return total_mb


# QUEUE_INDEX = 3
# START_TIME_INDEX = 14
# FINISHED_TIME_INDEX = 15
# RUN_TIME_INDEX = 16
# MEMORY_SECONDS_INDEX = 22
def read_app_csv(path):
  job_count = np.random.randint(10, 50)
  queue = ['spark', 'hive', 'ProgrammerAlliance']
  # queue = ['spark', 'hive']
  jobs = []
  for i in range(job_count):
    job = Job()
    job.name = queue[np.random.randint(0, 3)]
    job.wait_time = np.random.randint(0, 25)
    job.run_time = np.random.randint(10, 40)
    job.memory_seconds = 1024 * job.run_time * 0.05
    jobs.append(job)
    """
    print '%d: queue: %s, wait time: %d, run time: %d, memory seconds: %d' %(i, job.name, job.wait_time, job.run_time, job.memory_seconds)
    """
  print('%d jobs finished during this interval' % job_count)
  return jobs


if __name__ == '__main__':
  jobs = read_app_csv('./hadoop/app.csv')
  """
  """
  # read_scheduler_csv('./hadoop/scheduler2.csv')
  # print read_cluster_csv('./hadoop/cluster2.csv')
