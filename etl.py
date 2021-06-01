import configparser
import psycopg2
import boto3
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries
import logging

logger = logging.getLogger()
logging.basicConfig(format="[%(levelname)s] [%(asctime)s] %(message)s")
logger.setLevel(logging.INFO)

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def gather_events():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=config.get('AWS','KEY'),
                        aws_secret_access_key=config.get('AWS','SECRET')
                        )
    
    sampleDbBucket =  s3.Bucket("udacity-dend")
    dfs_ = []
    for obj in sampleDbBucket.objects.filter(Prefix="log-data"):
        tmp_json = obj.get()['Body'].read().decode('utf-8')
        edata = pd.read_json(tmp_json, lines=True)
        dfs_.append(edata)
    df_events_ = pd.concat(dfs_, ignore_index=True) # (8056, 18)
    df_events = df_events_.loc[df_events_.page == "NextSong"].reset_index(drop=True) # (6820, 18) -> (6820, 25)

    df_events["start_time"] = df_events["ts"]
    df_events["hour"] = df_events.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').hour)
    df_events["day"] = df_events.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').day)
    df_events["week"] = df_events.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').week)
    df_events["month"] = df_events.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').month)
    df_events["year"] = df_events.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').year)
    df_events["weekday"] = df_events.start_time.apply(lambda x: pd.Timestamp(x, unit='ms').weekday()) # Monday == 0 … Sunday == 6
    return df_events


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_DB                 = config.get("DWH","DWH_DB")
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=config.get('AWS','KEY'),
                            aws_secret_access_key=config.get('AWS','SECRET'))

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)

    df_events = gather_events() 
    logger.info("Worning for staging time! Staging event table could take about 17 min.")
    df_events.to_sql("staging_events_table", conn_string, index=False, if_exists='replace')

    logger.info("Staging songs table could take about 5 min.")
    load_staging_tables(cur, conn)
    
    insert_tables(cur, conn)
    

    conn.close()


if __name__ == "__main__":
    main()