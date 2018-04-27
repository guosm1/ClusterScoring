import numpy as np
import xml.etree.ElementTree as ET
import utils
from file_operator import FileOperator
from treelib import Tree
from utils import QueueConfig, QueueWish, Singleton


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
      # print(self.config.abs_memory)

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
    if (len(self.pendings)):
      self.cur_metric.pending = np.mean(self.pendings)

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
    # To be fixed
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

class RMQueue(object):
  __metaclass__ = Singleton
  def __init__(self):
    self.tree = Tree()
    self.MAX_METRIC_COUNT = 12
    self.CAL_INTERVAL_IN_SECOND = 2 * 60 * 60

  def set_stat_interval(self, interval):
    self.CAL_INTERVAL_IN_SECOND = interval

  def set_system_memory(self, size):
    root = self.get_root()
    root.data.set_abs_memory(float(size))

  def create_queue(self, name=None, parent=None):
    data = QueueData()
    self.tree.create_node(name, name, parent, data)

  def display(self):
    self.tree.show()

  def display_score(self, queue=None, depth=0):
    if queue is None:
      queue = self.get_root()
      print('------------' + utils.get_str_time() + ' SCORE ----------')
      print('\tPENDING\tMEMORY USAGE')
      print('QUEUE NAME\tAVG\tDIV\tAVG\tDIV')
    print(queue.tag + '\t%8.3f' % queue.data.get_pending() + '\t%8.3f' % queue.data.get_pending_div() + '\t%8.3f' % queue.data.get_mem_usage() + '\t%8.3f' % queue.data.get_mem_usage_div())

    if not self.is_leaf(queue.tag):
      children = self.tree.children(queue.tag)
      for child in children:
        self.display_score(child, depth + 2)

  def display_prediction(self, queue=None, depth=0):
    if queue is None:
      queue = self.get_root()
      print('------------' + utils.get_str_time() + ' PREDICTION ----------')
      print('QUEUE NAME            DESIRED CAPACITY')

    print(queue.tag + '\t%8.3f' % queue.data.wish.capacity)
      # print(queue.tag, 'desired capacity: %.3f' % queue.data.wish.capacity)

    if not self.is_leaf(queue.tag):
      children = self.tree.children(queue.tag)
      for child in children:
        self.display_prediction(child, depth + 2)

  def write_score(self, path):
    with open(path, 'a') as f:
      self.write_score_top_down(output=f)

  def write_score_top_down(self, queue=None, depth=0, output=None):
    if queue is None:
      queue = self.get_root()
      output.writelines('------------' + utils.get_str_time() + ' SCORE ----------\n')
      output.writelines('\tPENDING\tMEMORY USAGE\n')
      output.writelines('QUEUE NAME\tAVG\tDIV\tAVG\tDIV\n')

    output.writelines(queue.tag + '\t%8.3f' % queue.data.get_pending() + '\t%8.3f' % queue.data.get_pending_div() + '\t%8.3f' % queue.data.get_mem_usage() + '\t%8.3f' % queue.data.get_mem_usage_div() + '\n')

    if not self.is_leaf(queue.tag):
      children = self.tree.children(queue.tag)
      for child in children:
        self.write_score_top_down(child, depth + 2, output)

  def write_prediction(self, path):
    if not FileOperator.file_exits(path):
      with open(path, 'a'):
        pass
    with open(path, 'w') as f:
      self.write_prediction_top_down(output=f)

  def write_prediction_top_down(self, queue=None, depth=0, output=None):
    if queue is None:
      queue = self.get_root()

    output.writelines(queue.tag + ',%.3f' % queue.data.wish.capacity + '\n')
    if not self.is_leaf(queue.tag):
      children = self.tree.children(queue.tag)
      for child in children:
        self.write_prediction_top_down(child, depth + 2, output)

  def add_job(self, job, qname):
    queue = self.tree.get_node(qname)
    if queue.is_leaf():
      queue.data.add_job(job)
    else:
      print("Canot add jobs to parent queue", queue.tag, queue.identifier)

  def add_metric(self, qname):
    queue = self.tree.get_node(qname)
    queue.data.add_metric(queue.cur_metric)
    if len(queue.data.metrics) > RMQueue.MAX_METRIC_COUNT:
      del queue.data.metrics[0]

  def remove_queue(self, qname):
    self.tree.remove_node(qname)

  def move_queue(self, src, dest):
    self.tree.move_node(src, dest)

  def get_queue(self, qname):
    return self.tree.get_node(qname)

  def get_root(self):
    return self.get_queue('root')

  def is_leaf(self, qname):
    queue = self.tree.get_node(qname)
    return queue.is_leaf()

  def cal_slowdown(self, queue=None):
    if queue is None:
      queue = self.get_root()

    avg_slowdown = 0.0
    if queue.is_leaf():
      job_count = len(queue.data.jobs)
      for i in list(range(job_count)):
        job = queue.data.jobs[i]
        slowdown = (job.wait_time + job.run_time) / job.run_time
        avg_slowdown += slowdown / job_count
      queue.data.set_job_count(job_count)
      queue.data.cur_metric.slowdown = avg_slowdown
    else:  # parent queue
      # First, get its all chilren queue, and call each child's cal_slowdown function
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_slowdown(child)

      # Second, get the job count
      job_count = 0
      for child in children:
        job_count += child.data.get_job_count()
      queue.data.set_job_count(job_count)

      # Finally, calculate the average slowdown of the queue
      if job_count == 0:
        queue.data.cur_metric.slowdown = avg_slowdown
        return avg_slowdown

      for child in children:
        avg_slowdown += child.data.get_job_count() * child.data.get_slowdown() / job_count
      queue.data.cur_metric.slowdown = avg_slowdown
    return queue.data.get_slowdown()

  def cal_pending(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if queue.is_leaf():
      queue.data.cal_leaf_pending()
    else:  # parent queue
      # First, get its all chilren queue, and call each child's cal_pending function
      # Second, get the sum of all its children pending
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_pending(child)
        queue.data.cur_metric.pending += child.data.get_pending()

    return queue.data.get_pending()

  def cal_pending_division(self, queue=None):
    if queue is None:
      queue = self.get_root()

    division = 0.0
    if self.is_leaf(queue.tag):
      return division
    else:  # parent queue
      children = self.tree.children(queue.tag)
      # First, get its all chilren queue, and call each child's calSlowDown function
      for child in children:
        self.cal_pending_division(child)

      # Second, calculate the square sum of division
      count = len(children)
      avg_pending = queue.data.get_pending() * 1.0 / count
      squareSum = 0.0
      for child in children:
        squareSum += np.square(child.data.get_pending() - avg_pending)

      # Finally, calculate the standard division of the queue
      # if count == 0:
      #    queue.data.cur_metric.slowdown_div = division
      #    return division
      division = np.sqrt(squareSum / count)
      queue.data.cur_metric.pending_div = division
      return division

  def cal_slowdown_division(self, queue=None):
    if queue is None:
      queue = self.get_root()

    division = 0.0
    if self.is_leaf(queue.tag):
      return division
    else:  # parent queue
      children = self.tree.children(queue.tag)
      # First, get its all chilren queue, and call each child's calSlowDown function
      for child in children:
        self.cal_slowdown_division(child)

      # Second, calculate the square sum of division
      squareSum = 0.0
      count = len(children)
      for child in children:
        squareSum += np.square(child.data.get_slowdown() - queue.data.get_slowdown())

      # Finally, calculate the standard division of the queue
      # if count == 0:
      #    queue.data.cur_metric.slowdown_div = division
      #    return division
      division = np.sqrt(squareSum / count)
      queue.data.cur_metric.slowdown_div = division
      return division

  def cal_memory_usage_old(self, queue=None):
    if queue is None:
      queue = self.get_root()

    memory_usage = 0.0
    if queue.is_leaf():
      total_memory_seconds = queue.data.cal_leaf_mem_second()
      total_memory_capacity = queue.data.get_abs_memory() * RMQueue.CAL_INTERVAL_IN_SECOND
      memory_usage = 1.0 * total_memory_seconds / total_memory_capacity
      queue.data.set_mem_usage(memory_usage)
      queue.data.cal_abs_used_memory()
    else:  # parent queue
      # First, get its all chilren queue, and call each child's calMemoryUsage function
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_memory_usage_old(child)

      # Second, calculate the absUsedMemory of current queue
      abs_used_memory = 0
      for child in children:
        abs_used_memory += child.data.get_abs_used_memory()
      queue.data.set_abs_used_memory(abs_used_memory)

      # Finally, calculate the memory usage of the queue
      queue.data.set_mem_usage(1.0 * queue.data.get_abs_used_memory() / queue.data.get_abs_memory())
    return queue.data.get_mem_usage()

  def cal_memory_usage(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if queue.is_leaf():
      abs_memory_usage = queue.data.cal_queue_memory_usage()
      abs_memory_capacity = queue.data.get_abs_capacity()
      memory_usage = None
      if abs_memory_capacity == 0:
        memory_usage = 0
      else:
        memory_usage = 100.0 * abs_memory_usage / abs_memory_capacity
      queue.data.set_mem_usage(memory_usage)
      queue.data.set_abs_memory_usage(abs_memory_usage)
    else:  # parent queue
      # First, get its all chilren queue, and call each child's calMemoryUsage function
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_memory_usage(child)

      # Second, calculate the absUsedMemoryUsage of current queue
      abs_memory_usage = 0
      for child in children:
        abs_memory_usage += child.data.get_abs_memory_usage()
      queue.data.set_abs_memory_usage(abs_memory_usage)

      # Finally, calculate the memory usage of the queue
      queue.data.set_mem_usage(100.0 * queue.data.get_abs_memory_usage() / queue.data.get_abs_capacity())
    return queue.data.get_mem_usage()

  def cal_mem_usage_division(self, queue=None):
    if queue is None:
      queue = self.get_root()

    std_division = 0.0
    if self.is_leaf(queue.tag):
      queue.data.cur_metric.mem_usage_div = std_division
      return std_division
    else:  # parent queue
      # First, get its all chilren queue, and call each child's calSlowDown function
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_mem_usage_division(child)

      # Second, calculate the average memory usage of all its children
      count = len(children)
      total_mem_usage = 0
      for child in children:
        total_mem_usage += child.data.get_mem_usage()
        # print(child.data.get_mem_usage())
      avg_mem_usage = total_mem_usage / count

      # Finally, calculate the standard division of the queue
      squareSum = 0
      for child in children:
        squareSum += np.square(child.data.get_mem_usage() - avg_mem_usage)
      std_division = np.sqrt(squareSum / count)
      queue.data.cur_metric.mem_usage_div = std_division
      return std_division

  def cal_abs_capacity_bottom_up(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if self.is_leaf(queue.tag):
      return
    else:
      children = self.tree.children(queue.tag)
      abs_capacity = 0
      for child in children:
        # print("Queue name: %s, abs_capacity: %.2f" %(child.tag, child.data.get_abs_capacity()))
        self.cal_abs_capacity_bottom_up(child)
        abs_capacity += child.data.get_abs_capacity()
      queue.data.set_abs_capacity(abs_capacity)

  def cal_desired_abs_capacity_bottom_up(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if self.is_leaf(queue.tag):
      return
    else:
      children = self.tree.children(queue.tag)
      abs_capacity = 0.0
      fixed_capacity = 0.0
      for child in children:
        self.cal_desired_abs_capacity_bottom_up(child)
        if child.data.config.fixed:
          # print("FIXED")
          # print(child.data.config.capacity)
          # print(child.data.config.abs_capacity)
          fixed_capacity += child.data.config.capacity
        else:
          abs_capacity += child.data.wish.abs_capacity

      for child in children:
        if child.data.config.fixed:
          child.data.wish.abs_capacity = abs_capacity / (100.0 - fixed_capacity) * child.data.config.capacity
      queue.data.wish.abs_capacity = abs_capacity * 100.0 / (100.0 - fixed_capacity)

  def clear_desired_abs_capacity(self, queue=None):
    if queue is None:
      queue = self.get_root()

    queue.data.wish.abs_capacity = 0
    if self.is_leaf(queue.tag):
      return
    else:
      queue.data.cur_metric.pending = 0.0
      children = self.tree.children(queue.tag)
      for child in children:
        self.clear_desired_abs_capacity(child)

  def cal_abs_capacity_top_down(self, queue=None):
    if queue is None:
      queue = self.get_root()
      queue.data.set_abs_capacity(100.0)

    if self.is_leaf(queue.tag):
      return
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        child.data.set_abs_capacity(queue.data.get_abs_capacity() * child.data.get_capacity() / 100.0)
        self.cal_abs_capacity_top_down(child)

  def cal_desired_capacity_top_down(self, queue=None):
    if queue is None:
      queue = self.get_root()
      queue.data.wish.capacity = 100.0

    if self.is_leaf(queue.tag):
      return
    else:
      children = self.tree.children(queue.tag)
      abs_capacity = queue.data.wish.abs_capacity
      for child in children:
        if child.data.config.fixed:
          child.data.wish.capacity = child.data.config.capacity
        elif abs_capacity == 0:
          child.data.wish.capacity = 0
        else:
          child.data.wish.capacity = child.data.wish.abs_capacity / abs_capacity * 100.0
        self.cal_desired_capacity_top_down(child)

  def cal_capacity_top_down(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if self.is_leaf(queue.tag):
      return
    else:
      children = self.tree.children(queue.tag)
      abs_capacity = queue.data.get_abs_capacity()
      for child in children:
        if abs_capacity == 0:
          child.data.set_capacity(0)
        else:
          child.data.set_capacity(child.data.get_abs_capacity() / abs_capacity * 100)
        self.cal_capacity_top_down(child)

  def cal_abs_memory_top_down(self, queue=None):
    if queue is None:
      queue = self.get_root()
      queue.data.cal_totalMb_mean()
      queue.data.clear_totalMb()

    if self.is_leaf(queue.tag):
      return
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        child.data.set_abs_memory(queue.data.get_abs_memory() * child.data.get_capacity() / 100)
        self.cal_abs_memory_top_down(child)

  def clear_mus_top_down(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if self.is_leaf(queue.tag):
      queue.data.clear_queue_memory_usage()
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.clear_mus_top_down(child)

  def clear_jobs_top_down(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if self.is_leaf(queue.tag):
      queue.data.clear_jobs()
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.clear_jobs_top_down(child)

  def clear_pendings_top_down(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if self.is_leaf(queue.tag):
      queue.data.clear_pendings()
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.clear_pendings_top_down(child)

  def before_scoring(self):
    self.cal_abs_capacity_bottom_up()
    self.cal_capacity_top_down()
    self.cal_abs_memory_top_down()

  def after_scoreing(self):
    self.clear_jobs_top_down()
    self.clear_pendings_top_down()
    self.clear_mus_top_down()

  def score(self):
    self.before_scoring()
    # self.cal_slowdown()
    # self.cal_slowdown_division()
    self.cal_pending()
    self.cal_pending_division()
    self.cal_memory_usage()
    self.cal_mem_usage_division()
    self.after_scoreing()

  def before_predict(self):
    self.cal_desired_abs_capacity_bottom_up()

  def after_predict(self):
    self.clear_desired_abs_capacity()

  def predict(self):
    self.before_predict()
    self.cal_desired_capacity_top_down()
    self.after_predict()


def parseYarnConfig(conf):
  # YARN_CONFIG_HEAD = ["yarn", "scheduler", "capacity", "root"]
  YARN_PROPERTY_STATE = "state"
  YARN_PROPERTY_CAPACTIY = "capacity"
  YARN_PROPERTY_MAX_CAPACITY = "maximum-capacity"
  YARN_PROPERTY_QUEUE = "queues"
  # YARN_QUEUE_STATE = ["STOPPED", "RUNNING"]

  rmq = RMQueue()
  tree = ET.parse(conf)
  root = tree.getroot()
  for p in root:
    if p.tag != 'property':
      pass
    name = p.find('name')
    value = p.find('value')
    if name is None or value is None:
      continue
    names = name.text.split('.')
    if len(names) < 5:
      continue
    if names[-1] == YARN_PROPERTY_QUEUE:
      qname = names[-2]
      queue = rmq.get_queue(qname)
      if queue is None:
        rmq.create_queue(name=qname)
      children = value.text.split(',')
      for child in children:
        child = child.strip()
        if rmq.get_queue(child) is None:
          rmq.create_queue(name=child, parent=qname)
        else:
          rmq.move_queue(src=child, dest=queue)

  # the second loop set the configuration of each queue
  for p in root:
    if p.tag != 'property':
      pass
    name = p.find('name')
    value = p.find('value')
    if name is None or value is None:
      continue
    names = name.text.split('.')
    if len(names) < 5:
      continue
    if names[-1] == YARN_PROPERTY_CAPACTIY:
      qname = names[-2]
      queue = rmq.get_queue(qname)
      if queue is not None:
        capacity = float(value.text)
        queue.data.set_capacity(capacity)
    elif names[-1] == YARN_PROPERTY_MAX_CAPACITY:
      qname = names[-2]
      queue = rmq.get_queue(qname)
      if queue is not None:
        max_capacity = float(value.text)
        queue.data.set_max_capacity(max_capacity)
    elif names[-1] == YARN_PROPERTY_STATE:
      qname = names[-2]
      queue = rmq.get_queue(qname)
      if queue is not None:
        queue.data.set_state(value.text)

  rmq.cal_abs_capacity_top_down()
  return rmq

if __name__ == '__main__':
  rmq = parseYarnConfig('./conf/capacity-scheduler61.xml')
  root = rmq.get_queue("root")
  default = rmq.get_queue("default")
  ocsp = rmq.get_queue("ocsp")
  default.data.wish.abs_capacity = 26.0
  ocsp.data.wish.abs_capacity = 156.0

  rmq.display_score()
  rmq.predict()
  rmq.display_prediction()
  rmq.write_prediction('test.txt')
