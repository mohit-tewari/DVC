import pandas as pd
import numpy as np
from sys import argv
import psycopg2
from sqlalchemy import create_engine
import dvc.api as dvc_api

def csv_to_parquet(df):
    print(df.shape)
    df["week"] = pd.to_datetime(df["week"])
    df["price_diff"] = df["total_price"]-df["base_price"]
    print("df dtypes : ",df.dtypes)
    print(df.head())
    df=df.loc[(df["sku_id"].isin([216418,219009]))&(df["store_id"]==8091)]
    input_features = df.loc[:,df.columns!="units_sold"]
    target = df[["sku_id","week","units_sold"]]
    input_features.to_parquet("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/Feast/my_features/feature_repo/data/small_features.parquet")
    target.to_parquet("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/Feast/my_features/feature_repo/data/small_target.parquet")

def test_to_parquet(df):
    df["week"] = pd.to_datetime(df["week"])
    df["price_diff"] = df["total_price"]-df["base_price"]
    df=df.loc[(df["sku_id"].isin([216418,219009]))&(df["store_id"]==8091)]
    existing_data = pd.read_parquet("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/Feast/my_features/feature_repo/data/small_features.parquet")
    combined_data = pd.concat([existing_data,df], ignore_index=True)
    combined_data.to_parquet("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/Feast/my_features/feature_repo/data/small_features.parquet")

product_mapping = {
    'Loreal-109': 'Urban Decay Naked Skin Weightless Ultra Definition Liquid Makeup',
    'Loreal-110': 'NYX Professional Makeup Epic Ink Liner (liquid eyeliner)',
    'Loreal-114': 'Emporio Armani Stronger With You',
    'Loreal-117': 'SoftSheen-Carson Dark and Lovely Beautiful Beginnings Baby Powder',
    'Loreal-119': 'Anthelios Melt-In Milk Sunscreen SPF 60'
}    
def csv_to_postgres(df, table_name):
    print(df.shape)
    for index, row in df.iterrows():
        df.at[index, 'product'] = product_mapping.get(row['product_id'])
    
    df["week"] = pd.to_datetime(df["week"])
    df['total_price'] = round(df["base_price"] - (df["base_price"] * df["discount"])/100 , 2)
    df["price_diff"] = df["total_price"]-df["base_price"]
    print("df dtypes : ",df.dtypes)
    print(df.head())
    #df=df.loc[(df["sku_id"].isin([216418,219009]))&(df["store_id"]==8091)]
    df["timestamp"] =  pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    engine = create_engine('postgresql://postgres:docker@localhost:5492/postgres')
    df.to_sql(table_name, engine, if_exists='append', index=False, schema='offline_schema')

def csv_to_postgres_demo(df, table_name):
    print(df.shape)
    df["date"] = pd.to_datetime(df["date"])
    print("df dtypes : ",df.dtypes)
    print(df.head())
    #df=df.loc[(df["sku_id"].isin([216418,219009]))&(df["store_id"]==8091)]
    #df["timestamp"] =  pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
    engine = create_engine('postgresql://postgres:docker@localhost:5492/postgres')
    df.to_sql(table_name, engine, if_exists='append', index=False, schema='offline_schema')

def dvc_git_versioning(parquet_filename):
    import os 
    from datetime import datetime
    import git
    

    dvc_add_command = f'dvc add --all'
    dvc_commit_command = 'dvc commit -f'
    dvc_push_command = f'dvc push'
    os.system(dvc_add_command)
    os.system(dvc_push_command)
    #os.system(dvc_commit_command)

    git_commit_message = "Update code"
    git_add_command = f"git add --all"
    git_commit_command = f'git commit -m "{git_commit_message}"'
    os.system(git_add_command)
    os.system(git_commit_command)


    current_datetime = datetime.now()
    formatted_date_time = current_datetime.strftime("%b_%d_%H_%M")

    git_tag_name = formatted_date_time  # Replace with your desired version number
    #git_tag_command = f'git tag -a {git_tag_name} -m "Tagging version {git_tag_name}"'
    git_tag_command = f'git tag {git_tag_name}'
    os.system(git_tag_command)

    # Step 4: Push both Git and DVC commits to their respective remotes
    # git_push_command = 'git push origin master'  # Replace 'master' with your branch name
    # git_push_tags_command = 'git push origin --tags'
    git_push_command = 'git push -u origin master'  # Replace 'master' with your branch name
    git_push_tags_command = 'git push --tag'
    remote_repo_name = 'remote'
    #dvc_push_command = f'dvc push -r {remote_repo_name}'  # Replace 'remote-repo' with your DVC remote name
    os.system(git_push_command)
    os.system(git_push_tags_command)
    
    return git_tag_name

def dvc_git_versioning_bkp(parquet_file_name):
    from dvc.repo import Repo
    import git
    from datetime import datetime
    #repo_root = dvc_api.get_repo_root()
    dvc_repo = Repo()
    dvc_repo.stage.add(parquet_file_name)
    dvc_repo.commit('adding changed files in dvc')
    dvc_repo.push()

    # dvc_api.add(repo_root, recursive=True)
    # dvc_api.commit(message="Updated data file")
    # dvc_api.push()

    repo = git.Repo(search_parent_directories=True)
    repo.git.add("--all")
    repo.git.commit('-m', 'adding dvc files to git')
    
    current_datetime = datetime.now()
    formatted_date_time = current_datetime.strftime("%b_%d_%H_%M")
    git_tag_name = formatted_date_time
    repo.create_tag(git_tag_name)
    repo.git.push('-u', 'origin', 'main')
    repo.git.push('--tags')
    return git_tag_name


def postgres_to_dvc_versioning(schema_name, table_name, parquet_file_name = 'historical_data_versioned.parquet'):
    import pandas as pd
    engine = create_engine('postgresql://postgres:docker@localhost:5492/postgres')
    query = f'SELECT * FROM {schema_name}.{table_name}'
    df = pd.read_sql_query(query, con=engine)
    print(df.head())
    df.to_parquet(parquet_file_name, index=False)
    start_time = df.week.min()
    end_time = df.week.max()
    version_name = dvc_git_versioning(parquet_file_name)
    fetch_feature_view_query = f"SELECT * from misc.dbsource_feature_store_mapping where db_source_table_name='{table_name}'"
    print("fetch_feature_view_query : ",fetch_feature_view_query)
    df = pd.read_sql_query(fetch_feature_view_query, con=engine)
    print("printing feature view query output")
    print(df.head())
    feature_view_name = df['feature_view_name'][0]
    print("feature view name : ", feature_view_name)
    df = pd.DataFrame({'file_name': [parquet_file_name], 'version': [version_name], 
                        'feature_view_name':[feature_view_name],
                        'start_time':start_time, 'end_time':end_time, 
                        'creation_time':pd.Timestamp.now(), 'modification_time':pd.Timestamp.now()})
    df.to_sql('dvc_feature_store_mapping', engine, if_exists='append', index=False, schema='misc')




if __name__=="__main__":
    flag = argv[1]
    print(flag)
    if flag=="historical_data":
        print("historical data feeded")
        #train_df = pd.read_csv("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/data/demo_train.csv")
        #train_df = pd.read_csv("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/data/train_small_sanjay.csv")
        val_df = pd.read_csv("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/data/validate_small_sanjay.csv")
        #combined_df = pd.concat([train_df, val_df], ignore_index=True)
        combined_df = pd.concat([val_df], ignore_index=True)
        csv_to_postgres(combined_df,"history")
        postgres_to_dvc_versioning("offline_schema","history","historical_data_versioned.parquet")
    elif flag=="real_time":
        print("real time data feeded")
        #test_data = pd.read_csv("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/data/demo_test.csv")
        test_data = pd.read_csv("C:/Users/mohittewari/OneDrive - Nagarro/backup/Desktop/Workspace/Forecasting pipeline - Dec Demo/data/test_small_sanjay.csv")
        csv_to_postgres(test_data,"real_batch")


   
