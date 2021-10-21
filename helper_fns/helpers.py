# set up the featurestore client
import pandas as pd
from google.cloud.aiplatform_v1beta1 import (
    FeaturestoreOnlineServingServiceClient, FeaturestoreServiceClient)
from google.cloud.aiplatform_v1beta1 import (
    FeaturestoreOnlineServingServiceClient, FeaturestoreServiceClient)
from google.cloud.aiplatform_v1beta1.types import FeatureSelector, IdMatcher
from google.cloud.aiplatform_v1beta1.types import \
    entity_type as entity_type_pb2
from google.cloud.aiplatform_v1beta1.types import feature as feature_pb2
from google.cloud.aiplatform_v1beta1.types import \
    featurestore as featurestore_pb2
from google.cloud.aiplatform_v1beta1.types import \
    featurestore_monitoring as featurestore_monitoring_pb2
from google.cloud.aiplatform_v1beta1.types import \
    featurestore_online_service as featurestore_online_service_pb2
from google.cloud.aiplatform_v1beta1.types import \
    featurestore_service as featurestore_service_pb2
from google.cloud.aiplatform_v1beta1.types import io as io_pb2
from google.protobuf.duration_pb2 import Duration
from google.cloud import bigquery
from datetime import datetime
from dateutil.parser import parse
import datetime as datetime_class
import json
from google.cloud import bigquery
import time



#variables change to your liking
BUCKET = "matching-engine-demo-blog"
BQ_DATASET = 'movielens'
PROJECT_ID = 'matching-engine-blog'
API_ENDPOINT = "us-central1-aiplatform.googleapis.com"  # @param {type:"string"}
FEATURESTORE_ID = "performance_testing"
REGION = 'us-central1'

n_iterations = 2

admin_client = FeaturestoreServiceClient(client_options={"api_endpoint": API_ENDPOINT})

data_client = FeaturestoreOnlineServingServiceClient(
    client_options={"api_endpoint": API_ENDPOINT}
)

BASE_RESOURCE_PATH = admin_client.common_location_path(PROJECT_ID, REGION)

#initialize bq client for building benchmark datasets for FS
client = bigquery.Client()




def format_time(): #need time precision to not include microsecond info - this is a featurestore requirement
    t = datetime.now()
    if t.microsecond % 1000 >= 500:  # check if there will be rounding up
        t = t + datetime_class.timedelta(milliseconds=1)  # manually round up
    string = t.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    return parse(string)

def feature_store_spec(iteration):
    feature_specs=[
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"title_{iteration}"),
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"is_adult_{iteration}"),
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"average_rating_{iteration}"),
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"num_votes_{iteration}"),
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"director_{iteration}"),
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"d_profession_{iteration}"),
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"actors_{iteration}"),
        featurestore_service_pb2.ImportFeatureValuesRequest.FeatureSpec(id=f"actor_profession_{iteration}"),
    ]
    
    requests=[
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.STRING,
            description="The title of the movie", 
        ),
        feature_id=f"title_{iteration}",
    ),
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.INT64,
            description="Adult movie flag",
        ),
        feature_id=f"is_adult_{iteration}",
    ),
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.DOUBLE,
            description="The average rating for the movie, range is [1.0-10.0]",
        ),
        feature_id=f"average_rating_{iteration}",
    ),
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.INT64,
            description="Review Count",
        ),
        feature_id=f"num_votes_{iteration}",
    ),
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.STRING,
            description="Movie director(s) id - this is a key that matches to IMDB",
        ),
        feature_id=f"director_{iteration}",
    ),
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.STRING,
            description="Director(s) profession - a concat space seperated string of ids",
        ),
        feature_id=f"d_profession_{iteration}",
    ),
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.STRING,
            description=f"Actors - id these are keys that match to IMDB",
        ),
        feature_id=f"actors_{iteration}",
    ),
    featurestore_service_pb2.CreateFeatureRequest(
        feature=feature_pb2.Feature(
            value_type=feature_pb2.Feature.ValueType.STRING,
            description="Actors professions - a concat space seperated string of ids",
        ),
        feature_id=f"actor_profession_{iteration}",
    ),
    ]
    return feature_specs, requests
        
def create_fs():
    create_lro = admin_client.create_featurestore(
    featurestore_service_pb2.CreateFeaturestoreRequest(
        parent=BASE_RESOURCE_PATH,
        featurestore_id=FEATURESTORE_ID,
        featurestore=featurestore_pb2.Featurestore(
            online_serving_config=featurestore_pb2.Featurestore.OnlineServingConfig(
                fixed_node_count=1
                ),
            ),
        )
    )
    return create_lro
    
def create_entity_collection():
    movies_entity_type_lro = admin_client.create_entity_type(
    featurestore_service_pb2.CreateEntityTypeRequest(
        parent=admin_client.featurestore_path(PROJECT_ID, REGION, FEATURESTORE_ID),
        entity_type_id="movies",
        entity_type=entity_type_pb2.EntityType(description="Movies entity"),
        )
    )
    return movies_entity_type_lro


def load_fs(iterations, n_workers):
    
    feature_specs_list = []
    requests_list = []
    for iteration in range(iterations):
        fs, req = feature_store_spec(iteration)
        feature_specs_list.extend(fs)
        requests_list.extend(req)
    
    # declare new features
    admin_client.batch_create_features(
    parent=admin_client.entity_type_path(PROJECT_ID, REGION, FEATURESTORE_ID, "movies"),
    requests=requests_list
    ).result()
    
    #load the data
    
    import_movies_request = featurestore_service_pb2.ImportFeatureValuesRequest(
    entity_type=admin_client.entity_type_path(
        PROJECT_ID, REGION, FEATURESTORE_ID, "movies"
    ),
    # Source
    bigquery_source=io_pb2.BigQuerySource(
        input_uri=f"bq://{PROJECT_ID}.{BQ_DATASET}.movie_small"
    ),
    entity_id_field="movie_id",
    feature_specs=feature_specs_list,
    feature_time=format_time(),
    worker_count=n_workers,
    disable_online_serving = False # set to backfill historic features
    )
    ingestion_lro = admin_client.import_feature_values(import_movies_request)
    return ingestion_lro

def delete_featurestore():
    admin_client.delete_featurestore(
    request=featurestore_service_pb2.DeleteFeaturestoreRequest(
        name=admin_client.featurestore_path(PROJECT_ID, REGION, FEATURESTORE_ID),
        force=True,
        )
    ).result()

    return print("Deleted featurestore '{}'.".format(FEATURESTORE_ID ))

def poll_loading_data(ingestion_lro):
    start_time = datetime.now()
    mins = 0 
    while True: #polling to keep the session alive every x minutes
        if ingestion_lro.done():
            break
        #print(f"Running for {mins} minutes")
        #mins+=1
        #time.sleep(60) # one minute
    runtime_mins = (datetime.now() - start_time)
    return print(f"Ran for a total of {runtime_mins}") 
    
    
def create_a_fs_run(n_iterations, n_predictions, n_workers):
    
    #delete FS and dataset if exists (try except)
    try:
        delete_featurestore()
    except:
        pass
    
    try:
        client.query("DROP TABLE movielens.movie_small")
    except:
        pass
    
    # create the table for features
    select_stmnt = 'DELETE TABLE movielens.movie_small; '
    select_stmnt = (f'CREATE TABLE IF NOT EXISTS {BQ_DATASET}.movie_small as (SELECT movie_id, ')
    selects = []
    for iteration in range(n_iterations):
        appending = f'''
        title as title_{iteration},
        is_adult as is_adult_{iteration},
        average_rating as average_rating_{iteration},
        num_votes as num_votes_{iteration},
        director as director_{iteration},
        d_profession as d_profession_{iteration},
        actors as actors_{iteration},
        actor_profession as actor_profession_{iteration}
        '''
        selects.append(appending)

    select_stmnt += ','.join(selects)

    select_stmnt += f'FROM `{BQ_DATASET}.movie_view` LIMIT {n_predictions})'
    query_job = client.query(select_stmnt)
    time.sleep(20*10) #prevent throttling

    create_lro = create_fs()
    print(create_lro.result())
    
    movies_entity_type_lro = create_entity_collection()
    print(movies_entity_type_lro.result())
    time.sleep(30*10) #prevent throttling
    start_time = time.time()
    #update featurestore two itertions
    ingestion_lro = load_fs(n_iterations, n_workers=n_workers)
    poll_loading_data(ingestion_lro)
    load_time = time.time() - start_time
    lro_creation_metrics = print(ingestion_lro.result())
    return load_time #includes n features and runtime to be used in future analysis


def measure_fs(n_iterations, n_predictions):
    query = f"SELECT movie_id FROM `matching-engine-blog.movielens.movie_small` ORDER BY num_votes_0 DESC LIMIT {n_predictions}"
    query_job = client.query(query)
    top_movie_ids = query_job.result().to_dataframe()
    
    #grab ids for prediction
    
    ids = []
    for iteration in range(n_iterations):
        appender = [ f"title_{iteration}",
                    f"is_adult_{iteration}",
                    f"average_rating_{iteration}",
                    f"num_votes_{iteration}",
                    f"director_{iteration}",
                    f"d_profession_{iteration}",
                    f"actors_{iteration}",
                    f"actor_profession_{iteration}"]
        ids.extend(appender)

    feature_selector = FeatureSelector(
        id_matcher=IdMatcher(ids=ids)
    )
    
    n_features = len(ids)

    start_time = time.time()

    response_stream = data_client.streaming_read_feature_values(
        featurestore_online_service_pb2.StreamingReadFeatureValuesRequest(
            # Fetch from the following feature store/entity type
            entity_type=admin_client.entity_type_path(
                PROJECT_ID, REGION, FEATURESTORE_ID, "movies"
            ),
            entity_ids=top_movie_ids['movie_id'],
            feature_selector=feature_selector,
        )
    )

    for response in response_stream:
        _ = response

    total_time = time.time() - start_time
    ms_per = total_time / n_predictions 

    print(f"Ran for a total of: {total_time} seconds for {n_predictions} streaming predictions \n Per prediction seconds: {ms_per}")

    query_job.result()
    return total_time, n_features