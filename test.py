import pandas as pd
from pandas.core.frame import DataFrame

if __name__ == '__main__':
  head = ["1" , "2" , "3"]
  l = [[1 , 2 , 3],[4,5,6] , [8 , 7 , 9]]
  df = DataFrame (l , columns = head)
  df.to_csv ("testfoo.csv" , encoding = "utf-8")
  df2 = pd.read_csv ("testfoo.csv" , encoding = "utf-8")
  print (df2)
