import pandas as pd
import json

class read_json_file:
    def __init__(self, path):
      self.path = path

    def read_json_tip(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          try:
            data.append(json.loads(f.readline()))
          except Exception as e:
            pass
        data = pd.DataFrame(data)
      return data
    
    def read_json_checkin(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          try:
            data.append(json.loads(f.readline()))
          except Exception as e:
            pass
        data = pd.DataFrame(data)
      data['date'] = data['date'].apply(lambda x: x.split(','))
      data = data.explode('date')
      return data
    
    def read_json_review(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          try:
            data.append(json.loads(f.readline()))
          except Exception as e:
            pass
        data = pd.DataFrame(data)
      return data
    
    def read_json_business(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          try:
            data.append(json.loads(f.readline()))
          except Exception as e:
            pass
        data = pd.DataFrame(data)
        data['attributes']=str(data['attributes'])
        data['hours']=str(data['hours'])
      return data
    
    def read_json_user(self):
      data =[]
      with open(self.path,"r",encoding='utf-8') as f:
        for line in f:
          try:
            data.append(json.loads(f.readline()))
          except Exception as e:
            pass
      data = pd.DataFrame(data)
      return data
    
    