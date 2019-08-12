from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    This airflow operator check the numbers of records in each table 
    """
    
    ui_color = '#89DA59'  
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = "",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook( self.redshift_conn_id )
        for table in self.tables:            
            self.log.info(f"Data Quality checking for {table} table")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) <1 or len(records[0]) <1 :
                raise ValueError(f"Data quality check failed. {table} has no results")
                
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} has zero records")              
            self.log.info( f"Yeah, {table} has loaded successfully with {num_records} records!")
                          
                          
            
 
            
            
