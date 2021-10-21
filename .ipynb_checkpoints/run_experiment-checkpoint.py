# set up the featurestore client
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
import datetime as datetime_class
import json
from google.cloud import bigquery
import time
from helper_fns.helpers import *


BUCKET = "matching-engine-demo-blog"

#variables change to your liking
BUCKET = "matching-engine-demo-blog"
BQ_DATASET = 'movielens'
PROJECT_ID = 'matching-engine-blog'
API_ENDPOINT = "us-central1-aiplatform.googleapis.com"  # @param {type:"string"}
FEATURESTORE_ID = "performance_testing"
REGION = 'us-central1'

n_iterations = 2
n_predictions = 100
n_workers = 1

admin_client = FeaturestoreServiceClient(client_options={"api_endpoint": API_ENDPOINT})

data_client = FeaturestoreOnlineServingServiceClient(
    client_options={"api_endpoint": API_ENDPOINT}
)

BASE_RESOURCE_PATH = admin_client.common_location_path(PROJECT_ID, REGION)

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

def main():
    from doepy import build
    import pandas as pd

    design_data = build.lhs(
        {'Nodes':[2,8],
         'N_Rows':[1, 6],
         'N_Iterations':[1,7],
        }, num_samples=30)

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
        repeat_run_data = repeat_measure(row['N_Iterations']
                                         , row['N_Rows']
                                         , row['Nodes'], n_repeats=30)
        append_frame = pd.DataFrame(repeat_run_data, columns=cols)
        data = data.append(append_frame, ignore_index=True)
        ts = datetime.now()
        data.to_csv(f'data/experiment-{ts}.csv')

    #save to csv
    ts = datetime.now()
    data.to_csv(f'data/experiment-{ts}.csv')
    
    
if __name__ == '__main__':
    main()


