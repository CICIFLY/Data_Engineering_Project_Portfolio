from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This airflow operator class load data from AWS redshift staging tables to fact table 
    
    keyword arguments:
    redshift_conn_id
    target_table
    query : query name from SqlQueries
    
    output: populate data into fact table
    """
    ui_color = '#F98866'
    insert_sql = """
    INSERT INTO {} 
    {};
    COMMIT;
    """

    @apply_defaults
    # Define operators params (with defaults) here
    def __init__(self,
                 redshift_conn_id = "",
                 target_table = "",
                 query = "",
                 delete_load = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.query = query
        self.delete_load = delete_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info('Clear data from fact table')
        redshift.run("DELETE FROM {}".format(self.table))   
                         
        self.log.info('Load data from staging table to fact table...')
        formatted_sql = LoadFactOperator.insert_sql.format(self.target_table,
                                                           self.query)
        redshift.run(formatted_sql)
