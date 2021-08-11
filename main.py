import concurrent.futures
import datetime
import json
import logging
import os

import boto3 as boto3
import pandas as pd
import requests

logger = logging.getLogger('news_aggregator')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('logs.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class News:
    """Class responsible for quering and preprocessing data from external source (https://newsapi.org/docs)

    Attributes:
        bucketname = a string indicating name of the S3 bucket.
        apikey = a string, newsapi.org API key.
        news_sources = list of all english news sources. 
        news_sources_ids = list of unique identifiers of the news sources.
        endpoints = list of endpoints to be called in order to gather data.

    """
    def __init__(self, apikey: str):
        self.bucketname = 'news-bucket-antonvls'
        self.apikey = apikey
        self.news_sources = None
        self.news_sources_ids = None
        self.endpoints = list()
        self.top_headlines_json = list()
        self.resulting_dataframes = map

    def _get_news_sources(self) -> list:
        """Gathering all the en news sources in a single list"""
        try:
            response = requests.get(
                f'https://newsapi.org/v2/sources?language=en&apiKey={self.apikey}',
                timeout=5)
            response.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            logger.error(errh)
        except requests.exceptions.ConnectionError as errc:
            logger.error(errc)
        except requests.exceptions.Timeout as errt:
            logger.error(errt)
        except requests.exceptions.RequestException as err:
            logger.error(err)
        self.news_sources = response.json()
        self.news_sources_ids = self._get_ids(self.news_sources)
        return self.news_sources_ids

    def _get_ids(self, sources_json: json) -> list:
        """Helper function. Gets unique id of news sources """
        sources = sources_json['sources']
        sources_ids = [s.get('id', None) for s in sources]
        return sources_ids

    def _load_url(self, url: str, timeout: int) -> json:
        """Helper function in order to call endpoints in parallel"""
        headline_responses = requests.get(url, timeout=timeout)
        return headline_responses.json()

    def _topheadlines_endpoints(self) -> list:
        """Generating list of headline endpoints to be called"""
        self._get_news_sources()
        for _id in self.news_sources_ids:
            self.endpoints.append(f'https://newsapi.org/v2/top-headlines?sources={_id}&apiKey={self.apikey}')
        return self.endpoints

    def _get_top_headlines(self) -> map:
        """Retrieve headlines data filtered by news source"""
        self._get_news_sources()
        max_workers = 100
        timeout = 5
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = (executor.submit(self._load_url, url, timeout) for url in self._topheadlines_endpoints())
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    data = future.result()
                except Exception as exc:
                    data = str(type(exc))
                finally:
                    self.top_headlines_json.append(data)
        self.top_headlines_json = map(lambda x: x.get('articles'), self.top_headlines_json)
        return self.top_headlines_json

    def _clean_source(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Cleaning the source series in resulting DataFrames"""
        try:
            source_series = dataframe.loc[:, 'source'].apply(lambda x: x.get('name', None))
            dataframe.loc[:, 'source'] = source_series
        except KeyError as errk:
            logger.error(errk)
            pass
        return dataframe

    def _get_headlines_dataframes(self) -> map:
        """Getting the list of dataframes with clean and preprocessed data"""
        self._get_top_headlines()
        dataframes = [pd.DataFrame(response) for response in self.top_headlines_json]
        self.resulting_dataframes = map(self._clean_source, dataframes)
        return self.resulting_dataframes

    def _save_data(self):
        """Saving .csv files locally in data/ folder"""
        self._get_headlines_dataframes()
        for dataframe in self.resulting_dataframes:
            if dataframe.shape[0] > 1:
                file_name = f'{datetime.datetime.now()}_headlines.csv'
                news_source_folder = f'data/{dataframe.loc[1, "source"].replace("/", "")}'
                if not os.path.exists(news_source_folder):
                    os.mkdir(news_source_folder)
                f_path = os.path.join(news_source_folder, file_name)
                dataframe.to_csv(f_path)


    def upload_data(self):
        """Uploading data to S3 with the same folder structure"""
        self._save_data()
        session = boto3.Session(
            # In real case access key will be sitting within a cofig file and being read by configparser:
            aws_access_key_id='AKIAVK7PZNHLQBGR7TGL',
            aws_secret_access_key='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',
            region_name='eu-central-1'
        )
        s3 = session.resource('s3')
        path = 'data/'
        bucket = s3.Bucket(self.bucketname)
        for subdir, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    bucket.put_object(Key=full_path[len(path):], Body=data)
                logger.info(f'{full_path} has been uploaded')


if __name__=='__main__':
    news_object = News('b13583d49ad244a899b04634c2bafab2')
    news_object.upload_data()
