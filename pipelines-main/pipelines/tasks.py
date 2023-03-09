import pyodbc
import pandas as pd
import os

#conn = pyodbc.connect('Driver={PostgreSQL Unicode};'
#                      'Server=localhost;'
#                      'Port=5432;'
#                      'Database=pipelines;'
#                      'User Id=postgres;'
#                      'Password=123;'
#                      'Trusted_Connection=no;')
conn = pyodbc.connect("DSN=pgadmin4")

conn.autocommit = True


class BaseTask:
    """Base Pipeline Task"""

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        sql_query = pd.read_sql_query(f'''
                                      select * from {self.table}
                                      '''
                                      ,
                                      conn)

        df = pd.DataFrame(sql_query)
        df.to_csv(self.output_file + '.csv.gz', index=False)
        print(f"Copy table `{self.table}` to file `{self.output_file}`")


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        cursor = conn.cursor()

        sql = f'''CREATE TABLE {self.table}(id int NOT NULL,\
        name varchar(30),\
        url varchar(30));'''

        cursor.execute(sql)

        sql2 = f'''COPY {self.table}(id,name,url)
        FROM 'C:/original.csv' 
        DELIMITER ','
        CSV HEADER;'''
        # C:/original.csv - заглушка, так как с моим путем почему-то не работает
        cursor.execute(sql2)

        conn.commit()
        print(f"Load file `{self.input_file}` to table `{self.table}`")


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        cursor = conn.cursor()

        cursor.execute(self.sql_query)

        conn.commit()
        print(f"Run SQL ({self.title}):\n{self.sql_query}")



class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):
        sql_query = pd.read_sql_query(f'''
                                              select * from original
                                              '''
                                      ,
                                      conn)

        df = pd.DataFrame(sql_query)

        new_col = []
        for x in df['url']:
            new_col.append(x.split("/", )[2])
        df['domain_of_url'] = new_col
        tempname = 'C:/temp.csv'
        df.to_csv(tempname, index=False)

        cursor = conn.cursor()

        sql = f'''CREATE TABLE {self.table}(id int NOT NULL,\
                name varchar(30),\
                url varchar(30),
                domain_of_url varchar(30));'''
        cursor.execute(sql)

        sql2 = f'''COPY {self.table}(id,name,url,domain_of_url)
                FROM '{tempname}'
                DELIMITER ','
                CSV HEADER;'''

        cursor.execute(sql2)

        conn.commit()
        os.remove(tempname)
        print(f"Create table `{self.table}` as SELECT:\n{self.sql_query}")
