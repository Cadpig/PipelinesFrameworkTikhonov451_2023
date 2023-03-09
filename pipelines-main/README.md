# Pipelines — ETL Framework

## Quickstart

### Initialization

Create a new folder and run `pipelines init` inside of it.

It will create a file named `pipeline.py` with the following content:

```python
from pipelines import tasks, Pipeline
from pipelines.tasks import sql_create_table_as, load_file_to_db, save_table_to_file

NAME = 'test_project'
SCHEMA = 'public'
YEAR_SUFFIX = '2023'

pipeline = Pipeline(
    name=NAME,
    schema=SCHEMA,
    version=VERSION,
    tasks=[
        load_file_to_db(
            input='original/original.csv',
            output='original',
        ),
        sql_create_table_as(
            table='norm',
            query='''
                select *, domain_of_url(url)
                from {original};
            '''
        ),
        tasks.CopyToFile(
            input='norm',
            output='norm',
        ),

        # clean up:
        sql('drop table {original}'),
        sql('drop table {norm}'),
    ]
)
```

The idea of this pipeline is to load the existing file with URLs and normalize them — extract domain name for each url, and finally save the result back to CSV-file.

### List available tasks


```shell
> pipelines tasks
Tasks:
 1: load_file_to_db [original]: original/original.csv -> original
 2: ctas [norm]: norm
 3: copy_to_file [norm]: norm -> norm.csv.gz
```

### Add files

Let's create file `original.csv` somewhere with the following content:

```csv
id,name,url
1,hello,http://hello.com/home
2,world,https://world.org/
```

Formatted:

id |  name | url
-- | ----- | ---
 1 | hello | http://hello.com/home
 2 | world | https://world.org/

Now you can add it to your data sources using following command:

```shell
> pipelines add original.csv
data/original/original.csv
```

### Running the pipeline

```shell
> pipeline list
Error: No pipeline found in the current directory!

> cd test_project
> pipeline list
...
> pipeline run
...
```

Now, when we have all the dependencies in place, we can run the pipeline using `pipelines run`.

```shell
> pipelines run
Running task 1: load_file_to_db [original]: original/original.csv -> original
0 original/original.csv original
Loading file original/original.csv
Dropping table 'public.test_project_2023_original'
drop true
'COPY 2'
Task took 0.385 seconds

Running task 2: ctas [norm]: norm
drop table if exists public.test_project_2023_norm;
create table public.test_project_2023_norm as
select *, domain_of_url(url)
from public.test_project_2023_original;

SELECT 2
Task took 0.812 seconds

--------------------------------------------------------------------------------
Running task 3: copy_to_file [norm]: norm -> norm.csv.gz
Writing data to file: norm.csv.gz
COPY 2
Task took 0.027 seconds
```

### Results

You can see the result of you work in `data/norm.csv.gz`.

```csv
id,name,url,domain_of_url
1,hello,http://hello.com/home,hello.com
2,world,https://world.org/,world.org
```

Formatted:

id |  name |                   url | domain_of_url
-- | ----- | --------------------- | -------------
 1 | hello | http://hello.com/home | hello.com
 2 | world |    https://world.org/ | world.org
