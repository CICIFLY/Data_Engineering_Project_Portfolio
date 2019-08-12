from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    This airflow operator class load data from AWS S3 bucket to redshift staging tables
    
    keyword arguments:
     redshift_conn_id,
     aws_credentials_id ,
     target_table ,
     s3_bucket ,
     s3_key ,
     json_path ,
     file_type ,
     delimiter ,
     ignore_headers
    
    output: populate data into staging tables 
    """
    ui_color = '#358140'
    
    # copy sql statement would be different for csv files and json files 
    copy_sql_json = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    COMPUPDATE OFF
    """

    copy_sql_csv = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    IGNOREHEADER {}
    DELIMITER '{}'
   
    """    
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 target_table = "",
                 s3_bucket = "",
                 s3_key = "",
                 json_path = "" ,
                 file_type = "" ,
                 delimiter = "," , 
                 ignore_headers = 1,                 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table 
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path 
        self.file_type = file_type 
        self.delimiter = delimiter 
        self.ignore_headers = ignore_headers    

    def execute(self, context):
        aws_hook = AwsHook( self.aws_credentials_id )
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook( postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('copy data from s3 to redshift staging table')
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format( self.s3_bucket, rendered_key)
        
        if self.file_type == "json":
            formatted_sql = StageToRedshiftOperator.copy_sql_json.format(
                self.target_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path                
            )
            redshift.run(formatted_sql)
            
        if self.file_type == "cvs":
            formatted_sql = StageToRedshiftOperator.copy_sql_csv.format(
                self.target_table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
                
            )
            redshift.run(formatted_sql)




