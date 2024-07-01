import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    tags=["ETL_project1"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
)
def clean_flat_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
   
    @task()
    def create_table():
        from sqlalchemy import Table, Column, BigInteger, Float, Integer, MetaData, String, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        db_engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        churn_table = Table(
            'clean_flat_data',
            metadata,
            Column('flat_id', String, primary_key=True),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Integer),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Integer),
            Column('studio', Integer),
            Column('total_area', Float),
            Column('price', BigInteger),
            UniqueConstraint('flat_id', name='unique_clean_flat_id_constraint')
        )
        if not inspect(db_engine).has_table(churn_table.name):
            metadata.create_all(db_engine)

    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = "select * from flat_initial_data"
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        
        def remove_duplicates(data):
            cols = ['floor', 'kitchen_area', 'living_area', 'rooms', 'is_apartment', 'studio', 'total_area', 'price']
            is_duplicated_features = data.duplicated(subset=cols, keep=False)
            data = data[~is_duplicated_features].reset_index(drop=True)
            return data

        def fill_missing_values(data):
            cols_with_nans = data.isnull().sum()
            if cols_with_nans.sum() > 0:
                cols_with_nans = cols_with_nans[cols_with_nans > 0].index
                for col in cols_with_nans:
                    if data[col].dtype in [float, int]:
                        fill_value = data[col].median()
                    elif data[col].dtype == 'object':
                        fill_value = data[col].mode().iloc[0]
                    data[col] = data[col].fillna(fill_value)
            return data 

        def remove_outliers(data):
            cat_cols = data.loc[:, data.nunique() < 10].columns
            num_cols = data.select_dtypes(['float', 'int']).columns.drop(cat_cols)
            threshold = 1.5
            potential_outliers = pd.DataFrame()

            for col in num_cols:
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                margin = threshold * IQR
                lower = Q1 - margin
                upper = Q3 + margin
                potential_outliers[col] = ~data[col].between(lower, upper)

            outliers = potential_outliers.any(axis=1)
            data = data[~outliers].reset_index(drop=True)
            return data

        remove_duplicates_data = remove_duplicates(data)
        fill_missing_values_data = fill_missing_values(remove_duplicates_data)
        without_outliers_data = remove_outliers(fill_missing_values_data)
        return without_outliers_data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table= 'clean_flat_data',
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['flat_id'],
            rows=data.values.tolist()
    )
    
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

clean_flat_dataset()