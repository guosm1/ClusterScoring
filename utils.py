import time
import numpy as np

class Singleton(type):
  def __init__(self, *args, **kwargs):
    super(Singleton,self).__init__(*args, **kwargs)
    self.__instance = None

  def __call__(self, *args, **kwargs):
    if self.__instance is None:
      self.__instance = super(Singleton,self).__call__(*args, **kwargs)
    return self.__instance

class QueueWish(object):
  def __init__(self):
    self.name = ''
    self.abs_capacity = 0.0
    self.capacity = 0.0
    self.vmem = 0.0
    self.vcpu = 0.0

  def display(self):
    print('queue name: %s, mem: %.2f' % (self.name, self.vmem))

class QueueConfig(object):
  def __init__(self):
    self.capacity = 0
    self.max_capacity = 0
    self.abs_capacity = 0
    self.abs_memory = 0  # absolute memory capacity size in M
    self.pending = 0  # pending containers/applications in queue
    self.name = ""
    self.state = ""
    self.fixed = False

  def display(self):
    print('queue name: %s, state: %s' % (self.name, self.state))

class QueueMemoryUsage(object):
  def __init__(self):
    self.name = ""
    self.mu = 0.0

  def display(self):
    print('queue name: %s, memory usage: %.3f' % (self.name, self.mu))

class Job(object):
  def __init__(self, waitTime=0, runTime=0, vcore=0, memorySeconds=0):
    self.wait_time = float(waitTime)
    self.run_time = float(runTime)
    self.memory_seconds = memorySeconds
    self.name = ""

  def display(self):
    print('queue: %s, \t run_time: %.3f, \t memory seconds: %ld' % (self.name, self.run_time, self.memory_seconds))

class QueueMetric(object):
  def __init__(self):
    self.job_count = 0
    self.abs_used_memory = 0
    self.slowdown = 0.0
    self.slowdown_div = 0.0
    self.mem_usage = 0.0
    self.abs_mem_usage = 0.0
    self.mem_usage_div = 0.0
    self.pending = 0.0
    self.pending_div = 0.0

class QueueData(object):
  def __init__(self):
    self.jobs = []
    self.pendings = []
    self.config = QueueConfig()
    self.mus = []
    self.cur_metric = QueueMetric()
    self.metrics = []
    self.totalMbs = []
    self.wish = QueueWish()

  def add_job(self, job):
    self.jobs.append(job)

  def add_pending(self, cnt):
    self.pendings.append(cnt)

  def add_totalMb(self, localMb):
    self.totalMbs.append(localMb)

  def clear_totalMb(self):
    self.totalMbs = []

  def cal_totalMb_mean(self):
    l = len(self.totalMbs)
    if l > 0:
      self.config.abs_memory = np.mean(self.totalMbs)

  def cal_queue_memory_usage(self):
    l = len(self.mus)
    if l > 0:
      return np.mean(self.mus)
    return 0

  def clear_queue_memory_usage(self):
    self.mus = []

  def add_metric(self, metric):
    self.metrics.append(metric)

  def clear_jobs(self):
    self.jobs = []

  def clear_pendings(self):
    self.pendings = []

  def cal_leaf_mem_second(self):
    total_memory_second = 0
    for job in self.jobs:
      total_memory_second += job.memory_seconds
    return total_memory_second

  def cal_leaf_pending(self):
    if len(self.pendings) > 0:
      self.cur_metric.pending = np.mean(self.pendings)
    else:
      self.cur_metric.pending = 0

  def get_capacity(self):
    return self.config.capacity

  def set_fixed(self, fixed):
    self.config.fixed = fixed

  def set_capacity(self, capacity):
    self.config.capacity = float(capacity)

  def set_max_capacity(self, max_capacity):
    self.config.max_capacity = float(max_capacity)

  def set_abs_capacity(self, abs_capacity):
    self.config.abs_capacity = float(abs_capacity)

  def get_abs_capacity(self):
    return self.config.abs_capacity

  def get_abs_memory_usage(self):
    return self.cur_metric.abs_mem_usage

  def set_abs_memory_usage(self, abs_memory_usage):
    self.cur_metric.abs_mem_usage = float(abs_memory_usage)

  def cal_abs_used_memory(self):
    self.cur_metric.abs_used_memory = self.config.abs_memory * self.cur_metric.mem_usage

  def set_abs_used_memory(self, abs_used_memory):
    self.cur_metric.abs_used_memory = float(abs_used_memory)

  def get_abs_used_memory(self):
    return self.cur_metric.abs_used_memory

  def set_abs_memory(self, abs_memory):
    self.config.abs_memory = float(abs_memory)

  def get_abs_memory(self):
    return self.config.abs_memory

  def get_slowdown(self):
    return self.cur_metric.slowdown

  def get_pending(self):
    return self.cur_metric.pending

  def get_slowdown_div(self):
    return self.cur_metric.slowdown_div

  def get_pending_div(self):
    return self.cur_metric.pending_div

  def get_mem_usage_div(self):
    return self.cur_metric.mem_usage_div

  def update_queue_config(self, queue_config):
    self.config.capacity = float(queue_config.capacity)
    self.config.max_capacity = float(queue_config.max_capacity)
    self.config.abs_capacity = float(queue_config.abs_capacity)
    self.add_pending(queue_config.pending)
    self.config.state = queue_config.state

  def add_queue_memory_usage(self, queue_memory_usage):
    self.mus.append(queue_memory_usage.mu)

  def update_queue_wish(self, queue_wish):
    self.wish.vmem += float(queue_wish.vmem)
    self.wish.vcpu += float(queue_wish.vcpu)
    self.wish.abs_capacity += queue_wish.vmem

  def get_mem_usage(self):
    return self.cur_metric.mem_usage

  def set_mem_usage(self, mem_usage):
    self.cur_metric.mem_usage = mem_usage

  def set_state(self, state):
    self.config.state = state

  def set_job_count(self, job_count):
    self.cur_metric.job_count = job_count

  def get_job_count(self):
    return self.cur_metric.job_count

def get_str_time():
  return time.strftime('%Y-%m-%d:%H:%M:%S', time.localtime(time.time()))

if __name__ == '__main__':
  print(get_str_time())
