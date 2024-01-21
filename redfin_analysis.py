from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
from airflow.operators.bash_operator import BashOperator


s3_client = boto3.client('s3')

# s3 buckets
target_bucket_name = 'redfin-transform-area'

# url link from - https://www.redfin.com/news/data-center/
city_url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz'

def extract_data(**kwargs):
    url = kwargs['url']
    df = pd.read_csv(url, compression='gzip', sep='\t')
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = 'redfin_data_' + date_now_string
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    output_list = [output_file_path, file_str]
    return output_list

def transform_data(task_instance):
    data = task_instance.xcom_pull(task_ids="tsk_extract_city_data")[0]
    object_key = task_instance.xcom_pull(task_ids="tsk_extract_city_data")[1]
    df = pd.read_csv(data)

    #Remove commas from the 'city' column
    df['city'] = df['city'].str.replace(',', '')

    #Establish all columns that will be used
    cols = ['period_begin','period_end','period_duration', 'region_type', 'region_type_id', 'table_id',
    'is_seasonally_adjusted', 'city', 'state', 'state_code', 'property_type', 'property_type_id',
    'median_sale_price', 'median_list_price', 'median_ppsf', 'median_list_ppsf', 'homes_sold', 'pending_sales', 'new_listings',
    'inventory', 'months_of_supply', 'price_drops', 'off_market_in_two_weeks', 'median_dom', 'avg_sale_to_list', 'sold_above_list', 'parent_metro_region', 'parent_metro_region_metro_code', 'last_updated']
   
    df = df[cols]
    #Fill in off_market_in_two_weeks and price_drops
    df['off_market_in_two_weeks'] = df['off_market_in_two_weeks'].fillna(0)
    df['price_drops'] = df['price_drops'].fillna(0)
    df = df.dropna()

    #Change the period_begin and period_end to a date time object
    df['period_begin'] = pd.to_datetime(df['period_begin'])
    df['period_end'] = pd.to_datetime(df['period_end'])

    #Extract Years
    df["period_begin_in_years"] = df['period_begin'].dt.year
    df["period_end_in_years"] = df['period_end'].dt.year

    #Extract Month
    df["period_begin_in_months"] = df['period_begin'].dt.month
    df["period_end_in_months"] = df['period_end'].dt.month

    #Trim Metro Region
    parent_metro_region = df['parent_metro_region'].str.split(',', expand= True)[0]
    df['parent_metro_region'] = parent_metro_region
    
    #Map the number of the month to its respective name
    month_dict = {
        "period_begin_in_months": {
            1 : "Jan",
            2 : "Feb",
            3 : "Mar",
            4 : "Apr",
            5 : "May",
            6 : "Jun",
            7 : "Jul",
            8 : "Aug",
            9 : "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec",
            },
        "period_end_in_months": {
            1 : "Jan",
            2 : "Feb",
            3 : "Mar",
            4 : "Apr",
            5 : "May",
            6 : "Jun",
            7 : "Jul",
            8 : "Aug",
            9 : "Sep",
            10: "Oct",
            11: "Nov",
            12: "Dec"
            }}
    
    df = df.replace(month_dict)
        
    # Convert DataFrame to CSV format
    csv_data = df.to_csv(index=False)
        
    # Upload CSV to S3
    object_key = f"{object_key}.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=object_key, Body=csv_data)


default_args = {
    'owner': 'MichaelRzadki',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 21),
    'email': ['mikerzad@hotmail.com.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('redfin_analytics_dag',
        default_args=default_args,
        # schedule_interval = '@weekly', ###disabling this as I will not be running this on an reoccuring basis
        catchup=False) as dag:

        extract_city_data = PythonOperator(
        task_id= 'tsk_extract_city_data',
        python_callable=extract_data,
        op_kwargs={'url': city_url}
        )


        transform_city_data = PythonOperator(
        task_id= 'tsk_transform_city_data',
        python_callable=transform_data
        )

        load_to_s3 = BashOperator(
            task_id = 'tsk_load_to_s3',
            bash_command = 'aws s3 mv {{ ti.xcom_pull("tsk_extract_city_data")[0]}} s3://raw-redfin-data',
        )

        extract_city_data >> transform_city_data >> load_to_s3