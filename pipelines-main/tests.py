from pipelines import tasks
import pandas as pd
import pyodbc


def test_load_file():
    tasks.LoadFile(input_file='/example_pipeline/original/original.csv', table='original')
    df1 = pd.read_csv('/example_pipeline/original/original.csv')
    conn = pyodbc.connect("DSN=pgadmin4")
    sql_query = pd.read_sql_query(f'''select * from original''', conn)
    df2 = pd.DataFrame(sql_query)
    conn.close()
    tasks.RunSQL('drop table original')
    assert df1 == df2, "The data in the two dataframes do not match"


if __name__ == "__main__":
    test_load_file
    print("Everything passed")