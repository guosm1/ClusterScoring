# encoding=utf8
"""
Created on:17-11-1 上午11:18
@author:chenxf@chinaskycloud.com
"""
import datetime
import time


class TimeUtil(object):
  """
  获取时间工具，获取当前时间和当前时间之前的周，天，小时，秒的时间
  返回13位时间戳
  """

  @staticmethod
  def __get_time_now():
    end_time = datetime.datetime.now()
    return end_time

  @staticmethod
  def __change_time(times):
    mic = str(times).split('.')[1][0:3]
    times = int((time.mktime(times.timetuple()) * 1000)) + int(mic)
    return times

  @staticmethod
  def get_time_weeks(weeks=1, start_time=None):
    if not start_time:
      start_time = TimeUtil.__get_time_now()
    else:
      start_time = TimeUtil.change_to_time(start_time)

    end_time = start_time + datetime.timedelta(weeks=1)
    end_time = TimeUtil.__change_time(end_time)
    start_time = TimeUtil.__change_time(start_time)
    return start_time, end_time

  @staticmethod
  def get_time_days(days=1, start_time=None):
    if not start_time:
      start_time = TimeUtil.__get_time_now()
    else:
      start_time = TimeUtil.change_to_time(start_time)
    end_time = start_time + datetime.timedelta(days=days)

    end_time = TimeUtil.__change_time(end_time)
    start_time = TimeUtil.__change_time(start_time)
    return start_time, end_time

  @staticmethod
  def get_time_hours(hours=1, start_time=None):
    if not start_time:
      start_time = TimeUtil.__get_time_now()
    else:
      start_time = TimeUtil.change_to_time(start_time)
    end_time = start_time + datetime.timedelta(hours=hours)

    end_time = TimeUtil.__chang_time(end_time)
    start_time = TimeUtil.__change_time(start_time)
    return start_time, end_time

  @staticmethod
  def get_time_minutes(minutes=5, start_time=None):
    print('start_time', start_time)
    if not start_time:
      start_time = TimeUtil.__get_time_now()
    else:
      start_time = TimeUtil.change_to_time(start_time)

    end_time = start_time + datetime.timedelta(minutes=minutes)
    print(start_time)
    print(end_time)
    end_time = TimeUtil.__change_time(end_time)
    start_time = TimeUtil.__change_time(start_time)

    return start_time, end_time

  @staticmethod
  def get_time_seconds(seconds=10, start_time=None):
    if not start_time:
      start_time = TimeUtil.__get_time_now()
    else:
      start_time = TimeUtil.change_to_time(start_time)
    end_time = start_time + datetime.timedelta(seconds=seconds)

    print('start_time', start_time)
    print('end_time', end_time)
    end_time = TimeUtil.__change_time(end_time)
    start_time = TimeUtil.__change_time(start_time)
    print(start_time, end_time)
    return start_time, end_time

  @staticmethod
  def change_to_time(time_num):
    time_stamp = time_num / 1000
    print(time_stamp)
    style_time = datetime.datetime.fromtimestamp(time_stamp)
    return style_time


if __name__ == "__main__":
  print(TimeUtil.change_to_time(1514529808397))
