import os
import yaml
import psycopg2
import pandas.io.sql as sqlio

from settings import DATABASE_CONNECTION

class DatabaseController:

    def __init__(self):
        with open(os.path.join(os.getcwd(), 'sql', 'dashboard_queries.yml'), 'r') as fp:
            self._select_queries = yaml.safe_load(fp)

    def create_connection(self):
        connector = psycopg2.connect(host=DATABASE_CONNECTION['host'],
                            dbname=DATABASE_CONNECTION['dbname'],
                            user=DATABASE_CONNECTION['user'],
                            password=DATABASE_CONNECTION['password']
                            )

        return connector
    
    def _get_kpi_values(self):
        sentiment_count_query = self._select_queries['get_sentiment_count']
        sentiment_count_dict = dict()
        try:
            connector = self.create_connection()

            with connector.cursor() as cur:
                cur.execute(sentiment_count_query)
                records = cur.fetchall()
                for row in records:
                    sentiment_count_dict[row[0]] = row[1]
        except (Exception, psycopg2.Error) as error:
            print("Error while retrieving KPI values from DB: ", error)

        finally:
            if connector:
                cur.close()
                connector.close()

        # print(sentiment_count_dict.keys())

        if 'POS' not in sentiment_count_dict.keys():
            sentiment_count_dict['POS'] = 0
        if 'NEU' not in sentiment_count_dict.keys():
            sentiment_count_dict['NEU'] = 0
        if 'NEG' not in sentiment_count_dict.keys():
            sentiment_count_dict['NEG'] = 0

        # print(sentiment_count_dict.keys())

        return sentiment_count_dict

    def _get_score_distribution_per_label(self):
        connection = self.create_connection()
        scores_query = self._select_queries['get_label_and_score_data']
        data = sqlio.read_sql_query(scores_query, connection)
        unique_keys = data.label.unique().tolist()
        score_class_dict = {key:data.query("label==@key") for key in unique_keys}
        return score_class_dict
