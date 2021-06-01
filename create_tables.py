import configparser
import psycopg2
import boto3
from sql_queries import create_table_queries, drop_table_queries
from sql_queries import create_schemas_queries, drop_schemas_queries


def create_schemas(cur, conn):
    '''
    Function to create schemas. This function uses the variable 'create_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in create_schemas_queries:
        cur.execute(query)
        conn.commit()        

def drop_schemas(cur, conn):
    '''
    Function to drop schemas. This function uses the variable 'drop_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in drop_schemas_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    iam = boto3.client('iam',aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET'),
                       region_name='us-west-2')
    roleArn = iam.get_role(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"))['Role']['Arn']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_schemas(cur, conn)
    create_schemas(cur, conn)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()