# ee8041c722fb4fae8e367c5218deadf0
# 331db66686a7483c93316a671c4d3630
from pprint import pprint
import requests
import pandas as pd
import logging
import json

import concurrent.futures
import requests
import time


logger = logging.getLogger('news_aggregator')
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler('spam.log')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


class News:
    """

    """
    def __init__(self, apikey: str):
        self.apikey = apikey
        self.news_sources = None
        self.news_sources_ids = None
        self.endpoints = list()
        self.out = list()

    def _get_ids(self, sources_json: json) -> list:
        sources = sources_json['sources']
        sources_ids = [s.get('id', None) for s in sources]
        return sources_ids

    def _get_news_sources(self) -> list:
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

    def _load_url(self, url, timeout):
        ans = requests.get(url, timeout=timeout)
        return ans.json()

    def _topheadlines_endpoints(self):
        self._get_news_sources()
        for _id in self.news_sources_ids:
            self.endpoints.append(f'https://newsapi.org/v2/top-headlines?sources={_id}&apiKey={self.apikey}')
        print(self.endpoints)
        return self.endpoints

    def get_top_headlines(self) -> map:
        self._get_news_sources()

        CONNECTIONS = 100
        TIMEOUT = 5

        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            future_to_url = (executor.submit(self._load_url, url, TIMEOUT) for url in self._topheadlines_endpoints())
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    data = future.result()
                except Exception as exc:
                    data = str(type(exc))
                finally:
                    self.out.append(data)
                    print(str(len(self.out)), end="\r")

        self.out = map(lambda x: x.get('articles'), self.out)
        return self.out

    def _clean_source(self, dataframe: pd.DataFrame):
        source_series = dataframe.loc[:, 'source'].apply(lambda x: x.get('name', None))
        dataframe.loc[:, 'source'] = source_series
        return dataframe

    def _get_headlines_dataframes(self) -> map:
        self.get_top_headlines()
        pprint(self.out)
        # resulting_df = pd.DataFrame()
        dataframes = [pd.DataFrame(response) for response in self.out]
        resulting_dataframes = map(self._clean_source, dataframes)
        return resulting_dataframes




if __name__=='__main__':
    news_object = News('9e3d610b20cd4b02be0c7a7bbb1a32fc')
    pprint(news_object._get_headlines_dataframes())
