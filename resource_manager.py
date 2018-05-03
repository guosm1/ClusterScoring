import numpy as np
import xml.etree.ElementTree as ET
import utils
from file_operator import FileOperator
from treelib import Tree
from utils import QueueData, Singleton
from prettytable import PrettyTable

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

  def display_score(self, queue = None, depth = 0, table = None, printer = None):
    flag = False
    if queue is None:
      queue = self.get_root()
      flag = True
      table = PrettyTable(["QUEUE", "PENDING AVG", "PENDING DIV", "MEMORY USAGE AVG(Q)", "MEMORY USAGE AVG(C)", "MEMORY USAGE DIV"])
    if table is not None:
      table.add_row([queue.tag, 0 if queue.data.get_pending() == 0 else "%.3f"%queue.data.get_pending(),
                     0 if queue.data.get_pending_div() == 0 else "%.3f"%queue.data.get_pending_div(),
                     0 if queue.data.get_mem_usage() == 0 else "%.3f"%queue.data.get_mem_usage(),
                     0 if queue.data.cal_queue_memory_usage() == 0 else "%.3f"%queue.data.cal_queue_memory_usage(),
                     0 if queue.data.get_mem_usage_div() == 0 else "%.3f"%queue.data.get_mem_usage_div()])
    if not self.is_leaf(queue.tag):
      children = self.tree.children(queue.tag)
      for child in children:
        self.display_score(child, depth + 1, table)
    if flag:
      if printer is None:
        print('------------' + utils.get_str_time() + ' SCORE ----------')
        print table
      else:
        printer.write('\n------------' + utils.get_str_time() + ' SCORE ----------\n')
        printer.write(str(table))

  def display_prediction(self, queue = None, depth = 0, table = None, printer = None):
    flag = False
    if queue is None:
      queue = self.get_root()
      flag = True
      table = PrettyTable(["QUEUE", "DESIRED CAPACITY(Q)", "DESIRED CAPACITY(C)", "CONFIG CAPACITY"])
    if table is not None:
      table.add_row([queue.tag, str(0 if queue.data.wish.capacity == 0 else "%.3f"%(100 * queue.data.wish.capacity)) + " %",
                     0 if queue.data.wish.abs_capacity == 0 else "%.3f"%queue.data.wish.abs_capacity,
                     str(0 if queue.data.config.capacity == 0 else "%.3f"%queue.data.config.capacity) + " %"])
    if not self.is_leaf(queue.tag):
      children = self.tree.children(queue.tag)
      for child in children:
        self.display_prediction(child, depth + 1, table)
    if flag:
      if printer is None:
        print('------------' + utils.get_str_time() + ' PREDICTION ----------')
        print table
      else:
        printer.write('\n------------' + utils.get_str_time() + ' PREDICTION ----------\n')
        printer.write(str(table))

  def write_score(self, path):
    FileOperator.touch(path)
    with open(path, 'a') as f:
      self.display_score(printer=f)

  def write_prediction(self, path):
    FileOperator.touch(path)
    with open(path, 'a') as f:
      self.display_prediction(printer=f)

  def add_job(self, job, qname):
    queue = self.tree.get_node(qname)
    if queue.is_leaf():
      queue.data.add_job(job)
    else:
      print("Cannot add jobs to parent queue", queue.tag, queue.identifier)

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
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_slowdown(child)

      job_count = 0
      for child in children:
        job_count += child.data.get_job_count()
      queue.data.set_job_count(job_count)

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
      if len(queue.data.pendings) > 0:
        queue.data.cur_metric.pending = np.mean(queue.data.pendings)
    else:
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
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_pending_division(child)

      count = len(children)
      avg_pending = queue.data.get_pending() * 1.0 / count
      square_sum = 0.0
      for child in children:
        square_sum += np.square(child.data.get_pending() - avg_pending)

      division = np.sqrt(square_sum / count)
      queue.data.cur_metric.pending_div = division
      return division

  def cal_slowdown_division(self, queue=None):
    if queue is None:
      queue = self.get_root()

    division = 0.0
    if self.is_leaf(queue.tag):
      return division
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_slowdown_division(child)

      square_sum = 0.0
      count = len(children)
      for child in children:
        square_sum += np.square(child.data.get_slowdown() - queue.data.get_slowdown())

      division = np.sqrt(square_sum / count)
      queue.data.cur_metric.slowdown_div = division
      return division

  def cal_memory_usage(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if queue.is_leaf():
      capacity = queue.data.get_abs_capacity()
      memory_usage = 0.0
      if capacity != 0:
        memory_usage = 100.0 * queue.data.cal_queue_memory_usage() / capacity
      queue.data.set_mem_usage(memory_usage)
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_memory_usage(child)

      abs_memory_usage = 0
      for child in children:
        abs_memory_usage += child.data.get_abs_memory_usage()

      queue.data.set_mem_usage(100.0 * abs_memory_usage / queue.data.get_abs_capacity())
    return queue.data.get_mem_usage()

  def cal_mem_usage_division(self, queue=None):
    if queue is None:
      queue = self.get_root()

    std_division = 0.0
    if self.is_leaf(queue.tag):
      queue.data.cur_metric.mem_usage_div = std_division
      return std_division
    else:
      children = self.tree.children(queue.tag)
      for child in children:
        self.cal_mem_usage_division(child)

      count = len(children)
      total_mem_usage = 0
      for child in children:
        total_mem_usage += child.data.get_mem_usage()
      avg_mem_usage = total_mem_usage / count

      square_sum = 0
      for child in children:
        square_sum += np.square(child.data.get_mem_usage() - avg_mem_usage)
      std_division = np.sqrt(square_sum / count)
      queue.data.cur_metric.mem_usage_div = std_division
      return std_division

  def cal_abs_capacity_bottom_up(self, queue=None):
    if queue is None:
      queue = self.get_root()

    if self.is_leaf(queue.tag):
      return
    else:
      children = self.tree.children(queue.tag)
      abs_capacity = 0.0
      for child in children:
        self.cal_abs_capacity_bottom_up(child)
        abs_capacity += child.data.get_abs_capacity()
      queue.data.set_abs_capacity(abs_capacity)

  def cal_desired_abs_capacity_bottom_up(self, queue=None, delim = None):
    if queue is None:
      queue = self.get_root()
      delim = 1
    if self.is_leaf(queue.tag):
      queue.data.wish.capacity = queue.data.wish.abs_capacity / delim
    else:
      children = self.tree.children(queue.tag)
      abs_capacity = 0.0
      for child in children:
        self.cal_desired_abs_capacity_bottom_up(child, queue.data.config.capacity * delim / 100)
        abs_capacity += child.data.wish.abs_capacity

      queue.data.wish.capacity = abs_capacity / delim

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
        child.data.wish.capacity = child.data.config.capacity
        if abs_capacity == 0:
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

  def score(self):
    # self.cal_abs_capacity_bottom_up()
    # self.cal_capacity_top_down()
    # self.cal_abs_memory_top_down()
    # self.cal_slowdown()
    # self.cal_slowdown_division()
    self.cal_pending()
    self.cal_pending_division()
    self.cal_memory_usage()
    self.cal_mem_usage_division()
    # self.clear_jobs_top_down()
    self.clear_pendings_top_down()
    self.clear_mus_top_down()

  def predict(self):
    self.cal_desired_abs_capacity_bottom_up()
    # self.cal_desired_capacity_top_down()
    # self.clear_desired_abs_capacity()

def parseYarnConfig(conf):
  YARN_PROPERTY_STATE = "state"
  YARN_PROPERTY_CAPACTIY = "capacity"
  YARN_PROPERTY_MAX_CAPACITY = "maximum-capacity"
  YARN_PROPERTY_QUEUE = "queues"

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
  rmq.display()
