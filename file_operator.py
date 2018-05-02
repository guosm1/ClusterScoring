import os
import csv

class FileOperator(object):
  @staticmethod
  def write_to_csv(data, file, headers, write_header=True, model="w+"):
    from logger_util import get_logger
    logger = get_logger()
    with open(file, model) as f:
      try:
        f_csv = csv.DictWriter(f, headers)
        if write_header:
          f_csv.writeheader()
        f_csv.writerows(data)
      except Exception as error:
        raise error
      else:
        logger.info("write data to {0} success".format(file))

  @staticmethod
  def write_list_tocsv(data, file, model="w+"):
    with open(file, mode=model) as csv_file:
      csv_writer = csv.writer(csv_file)
      for data in data:
        csv_writer.writerow(data)

  @staticmethod
  def file_exits(file):
    if os.path.isfile(file):
      return True
    return False

  @staticmethod
  def touch(file):
    if os.path.isfile(file):
      return False
    else:
      FileOperator.path_exits(os.path.dirname(file))
      fd = open(file,'w')
      fd.close()

  @staticmethod
  def path_exits(file):
    if not os.path.exists(file):
      os.mkdir(file)
