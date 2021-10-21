# Featurestore Tester

The objective of this app is to 
1) Show how to load and use FeatureStore via the Vertex SDK
2) Provide a performance test with the `run_experiment.py` application


## Load FS

### [`00_load_data_to_bq.ipynb`](00_load_data_to_bq.ipynb) 
This notebook downloads the movielens and imbdb datasets then creates a simple testing dataset of over 150k movies.

### [`01_feature_store.ipynb`](01_feature_store.ipynb)
This notebook creates a movies featurestore and loads movies and user review entities into a featurestore. The notebook ends with making streaming calls to the featurestore.

## Test FS

### [`run_experiment.py`](run_experiment.py)

This (along with `Factorial-Design.ipynb`) tests featurestore's load and request latency. `doepy` is used to construct the tests. Recommended to load via terminal background process or `nohup` call.
