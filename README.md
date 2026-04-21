# Movie Score Data Pipeline

ETL pipeline that ingests, cleans, standardises, and merges movie data from three providers into a unified PySpark DataFrame.

# Target

This project processes provider's files and produces a unified view of the data.
The final result is a data structure that allows the data science team to access all information for a given movie easily.

# Project Structure

```
movie-pipeline/
├── data/                          # raw provider's files
├── src/
│   └── movie_pipeline/
│       ├── config.py              # file paths, Spark settings, column lists
│       ├── schema.py              # model schema and each provider StructTypes
│       ├── session.py             # SparkSession factory
│       ├── pipeline.py            # Main file. Merge_providers() orchestrator
│       ├── providers/
│       │   ├── base.py            # BaseProvider abstract class
│       │   ├── critic.py      	   # CriticProvider (1st provider .csv file)
│       │   ├── audience.py        # AudienceProvider (2nd provider .json file)
│       │   └── box_office.py      # BoxOfficeMetricsProvider (3rd provider 3 different csv files)
│       └── transformations/
│           ├── helpers.py         # normalise_title(), enforce_schema()
│           └── derived.py         # derived columns whose values are calculated from the unified dataframe
├── tests/
│   ├── conftest.py                # shared SparkSession fixture + temp file helpers
│   ├── test_helpers.py
│   ├── test_critic_agg.py
│   ├── test_audience_pulse.py
│   ├── test_box_office.py
│   └── test_pipeline.py
└── output/                        # pipeline results (gitignored)
```


## Starting file pipeline

```
movie_pipeline.pipeline
```


## Adding a new provider

1. Create `src/movie_pipeline/providers/my_provider.py` — subclass `BaseProvider`, implement `extract_transform()`.
2. Add it to the `providers` list in `pipeline.py`.
3. Add a `tests/test_my_provider.py` file.

No other files need to change — `enforce_schema()` handles missing columns automatically.
