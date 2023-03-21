from plugins.read_json.read_json import read_json_file
import pandas as pd
from datetime import datetime

class load_to_staging:
    ui_color = '#FFF4E0'
    def __init__(self, path, table_name, engine):
      self.path = path
      self.table_name= table_name
      self.engine = engine

    def load_staging_tip(self):
      data = read_json_file(self.path)
      data = data.read_json_tip()
      data.to_sql(self.table_name, self.engine, if_exists='replace')

    def load_staging_review(self):
      data = read_json_file(self.path)
      data = data.read_json_review()
      data.to_sql(self.table_name, self.engine, if_exists='replace')
      
    def load_staging_precipitacion(self):
      dateparse = lambda x: datetime.strptime(x, '%Y%m%d')
      data = pd.read_csv(self.path, parse_dates=['date'], date_parser=dateparse)
      data.to_sql(self.table_name, self.engine, if_exists='replace')

    def load_staging_temperature(self):
      dateparse = lambda x: datetime.strptime(x, '%Y%m%d')
      data = pd.read_csv(self.path, parse_dates=['date'], date_parser=dateparse)
      data.to_sql(self.table_name, self.engine, if_exists='replace')
    
    def load_staging_business(self):
      data = read_json_file(self.path)
      data = data.read_json_business()
      data.to_sql(self.table_name, self.engine, if_exists='replace')

    def load_staging_user(self):
      data = read_json_file(self.path)
      data = data.read_json_user()
      data.to_sql(self.table_name, self.engine, if_exists='replace')
      


