from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    ui_color = '#228140'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 *args, **kwargs):
        
        super(CreateTableOperator, self).__init__(*args,**kwargs)
       
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating A table!!")
        redshift.run(self.sql)

        self.log.info("A Table Created!!")
        