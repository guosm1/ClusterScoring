"""
"""
import time

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


class MyException(Exception):
  def __init__(self, message):
    Exception.__init__(self)
    self.message = message


def get_str_time():
  return time.strftime('%Y-%m-%d:%H:%M:%S', time.localtime(time.time()))

if __name__ == '__main__':
  print(get_str_time())
