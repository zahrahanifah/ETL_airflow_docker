from sqlalchemy.sql import text

class load_to_fact:
    ui_color = '#80BD9E'
    def __init__(self, create_queries, insert_queries, engine):
      self.create_queries = create_queries
      self.insert_queries = insert_queries
      self.engine = engine

    def load_to_fact(self):
       with self.engine.connect().execution_options(autocommit=True) as conn:
          conn.execute(text(self.create_queries)) 
          conn.execute(text(self.insert_queries)) 
