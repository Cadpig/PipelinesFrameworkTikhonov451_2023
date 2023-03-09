Goals:
- Base knowledge: Python + Data (DB, Pandas, Web)
- Understand role of data pipelines
- CI/CD (GitHub Actions, Docker)
- Open Source Project

ToDo (Jan 27):
- [ ] Meet Dagster & Prefect
- [ ] Implement test project with both Dagster & Prefect
- [ ] Create and share GitHub repo with the results
- [ ] Begin to implement "Pipelines"


Recommendations:
- Use Click as a CLI framework — https://click.palletsprojects.com/en/8.1.x/setuptools/
- Use `entry_points` to make `pipelines` available in as global command line utility.
- `domain_of_url` can be temporarily replaced with a builtin SQL function like `upper(str)` (`upper('http://google.com') -> `HTTP://GOOGLE.COM')


ToDo (Feb 3):
- [ ] finish Dagster/Prefect (1):
    - [ ] add poetry support (Dagster/Prefect)
    - [ ] add README.txt
    - [ ] add CSV files
- [ ] Pipelines:
    - [ ] fork this repo
    - [ ] add poetry support (deps: click)
    - [ ] > pipeline list
    - [ ] > pipeline run (fake)
    - [ ] > poetry run pipeline run  # should work!
    - [ ] configuration in .env file


Recommendations:
- cd pipelines
- poetry run pip install -e .
