# set up the featurestore client
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
import datetime as datetime_class
import json
from google.cloud import bigquery
import time
from helper_fns.helpers import *
from absl import app
from absl import flags
from absl import logging

### 159 bytes per row of 7 features (plus one id), so ~135 bytes per row
## 6250 rows per 1 mb
# python run_experiment.py --N_ITERATIONS 15 --N_WORKERS 8 \
# --N_MEASURES 100 --N_REPEAT_MEASURES --N_PREDICTIONS 2000 \
# --PROJECT_ID matching-engine-blog --BUCKET matching-eingine-demo-blog \
# --BQ_DATASET movielens --REGION us-central1 --FEATURESTORE_ID perf_testing



FLAGS = flags.FLAGS
flags.DEFINE_integer("N_ITERATIONS", 4, "Number of group of 7 features")
flags.DEFINE_integer("N_WORKERS", 1, "Number of workers")
flags.DEFINE_integer("N_MEASURES", 30, "Number of repeat mesures per point")
flags.DEFINE_integer("N_PREDICTIONS", 30, "Number of entities or predictions to return")
flags.DEFINE_integer("N_REPEAT_MEASURES", 500, "Number of entities or predictions to return")

#initialize bq client for building benchmark datasets for FS
client = bigquery.Client()


def repeat_measure(n_iterations, n_predictions, n_workers, n_repeats=30):
    data = {'create_stats': [],
            'n_features': [],
            'n_predictions': [],
            'n_workers': [],
            'total_seconds': [],
            'n_features': []
           }
    stats = create_a_fs_run(n_iterations, n_predictions, n_workers)
    
    
    for run in range(n_repeats):
        total_time, n_features = measure_fs(n_iterations, n_predictions)
        data['create_stats'].append(stats)
        data['n_features'].append(n_features)
        data['n_predictions'].append(n_predictions)
        data['n_workers'].append(n_workers)
        data['total_seconds'].append(total_time)

    print(total_time)
    return data


def main(argv):
    N_ITERATIONS = FLAGS.N_ITERATIONS
    N_PREDICTIONS = FLAGS.N_PREDICTIONS
    N_WORKERS = FLAGS.N_WORKERS
    N_MEASURES = FLAGS.N_MEASURES
    
    print(N_ITERATIONS, N_PREDICTIONS, N_WORKERS, N_MEASURES)
    
    logging.set_verbosity(logging.INFO)
    
    from doepy import build
    import pandas as pd
    
    
    admin_client = FeaturestoreServiceClient(client_options={"api_endpoint": FLAGS.API_ENDPOINT})

    BASE_RESOURCE_PATH = admin_client.common_location_path(FLAGS.PROJECT_ID, FLAGS.REGION)

    design_data = build.lhs(
        {'Nodes':[1,N_WORKERS],
         'N_Rows':[1, N_PREDICTIONS],
         'N_Iterations':[1,N_ITERATIONS],
        }, num_samples=N_MEASURES)

    design_data = design_data[['Nodes', 'N_Rows', 'N_Iterations']].astype(int)
    
    # run the experiment and store data

    cols = ['create_stats',
        'n_features',
        'n_predictions',
        'n_workers',
        'total_seconds']


    data = pd.DataFrame([], columns=cols)
    
    for index, row in design_data.iterrows():
        print(f"Testing for following row: \n{row}")
        try:
            repeat_run_data = repeat_measure(row['N_Iterations']
                                             , row['N_Rows']
                                             , row['Nodes'], n_repeats=FLAGS.N_REPEAT_MEASURES)
            append_frame = pd.DataFrame(repeat_run_data, columns=cols)
            data = data.append(append_frame, ignore_index=True)
            ts = datetime.now()
            data.to_csv(f'data/experiment-{ts}.csv')
            time.sleep(120*2)
        except:
            time.sleep(120*2)
            pass

    #save to csv
    ts = datetime.now()
    data.to_csv(f'data/experiment-{ts}.csv')
    
    
if __name__ == '__main__':
    app.run(main)


