import pandas as pd
import json

class read_json_file:
    def __init__(self, path):
      self.path = path

    def read_json_tip(self):
      data = pd.read_json(self.path, lines=True)
      return data
    
    def read_json_checkin(self):
      data = pd.read_json(self.path, lines=True)
      data['date'] = data['date'].apply(lambda x: x.split(','))
      data = data.explode('date')
      return data
    
    def read_json_review(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          data.append(json.loads(f.readline()))
        data = pd.DataFrame(data)
      return data
    
    def read_json_business(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          data.append(json.loads(f.readline()))
        data = pd.DataFrame(data)
      return data
    
    def read_json_user(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          data.append(json.loads(f.readline()))
        data = pd.DataFrame(data)
      return data
    
    