from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    """
    StageToRedshiftOperator loads staging data from target S3 account into Redshift

    Class object takes in the below parameters:
        reshift_conn_id: Redshift Connection ID
        aws_credentials_id: AWS Credentials ID
        table: name of table to be copied from S3
        s3_bucket: Target S3 bucket holding project data
        s3_key: AWS Access and Secret Access Key
        region: Region S3 data is being held
        _format:Data format (CSV, JSON, Text)
        delimiter: Delimiting character if _format not used

    """

    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """       
        COPY {}
        FROM '{}'
        credentials 'aws_iam_role={}'   
        FORMAT as {}
        
        
               
    """

    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 iam_cred = "",
                 region="",
                 _format="auto",
                 csv="",
                 delimiter=",",
                 extra_params = "",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.iam_cred = iam_cred
        self.region = region
        self._format = _format
        self.csv    =csv
        self.delimiter = delimiter
        self.extra_params = extra_params

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        self.log.info("Loading Data To Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(

            self.table,
            s3_path,
            self.iam_cred,
            self._format,           
        )
        redshift.run(formatted_sql)
        self.log.info("{} Data Loaded".format(self.table))
