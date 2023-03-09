from pipelines import tasks, Pipeline
import pandas as pd
import pyodbc


def test_load_file():
    tasks.LoadFile(input_file='/example_pipeline/original/original.csv', table='original')
    df1 = pd.read_csv('C:/original.csv')
    conn = pyodbc.connect("DSN=pgadmin4")
    cursor = conn.cursor()
    sql = f'''CREATE TABLE original(id int NOT NULL,\
           name varchar(30),\
           url varchar(30));'''
    cursor.execute(sql)
    sql2 = f'''COPY original(id,name,url)
           FROM 'C:/original.csv' 
           DELIMITER ','
           CSV HEADER;'''
    cursor.execute(sql2)
    conn.commit()
    sql_query = pd.read_sql_query(f'''select * from original''', conn)
    df2 = pd.DataFrame(sql_query)
    cursor.execute('drop table original')
    conn.commit()
    conn.close()
    assert df1.equals(df2), "The data in the two dataframes do not match"


def test_ctas():
    TASKS = [
        tasks.LoadFile(input_file='/example_pipeline/original/original.csv', table='original'),
        tasks.CTAS(
            table='norm',
            sql_query='''
                    select *, domain_of_url(url)
                    from original;
                '''
        ),
        ]
    pipeline = Pipeline(
        name="test",
        version="test",
        tasks=TASKS
    )
    pipeline.run()
    d = {'id': [1, 2], 'name': ['hello', 'world'], 'url': ['http://hello.com/home', 'https://world.org/'], 'domain_of_url': ['hello.com', 'world.org']}
    df1 = pd.DataFrame(data=d)
    conn = pyodbc.connect("DSN=pgadmin4")
    sql_query = pd.read_sql_query(f'''select * from norm''', conn)
    df2 = pd.DataFrame(sql_query)
    cursor = conn.cursor()
    cursor.execute('drop table original')
    cursor.execute('drop table norm')
    conn.commit()
    conn.close()
    assert df1.equals(df2), "The data in the two dataframes do not match"


if __name__ == "__main__":
    test_load_file()
    test_ctas()
    print("Everything passed")