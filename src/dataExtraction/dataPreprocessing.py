import os
import pickle
import re
from datetime import date
from typing import List

import pandas as pd
import numpy as np

#-------------------------
# Global Variables


#-------------------------
# Compiled Regex
REMOVE_DUPLICATES_RE = re.compile(r"\b([\w\s]+)\1\b")

#-------------------------

class AnimePreprocessor:

    def __init__(self, data_path: str):

        self.PATH = data_path

    def _load_files(self, all_files: List) -> List:

        all_dicts = []
        for file in all_files:
            with open(self.PATH + file, 'rb') as f:
                anime_dict = pickle.load(f)
                all_dicts.append(anime_dict)

        return all_dicts

    def aggregate(self) -> pd.DataFrame:

        # Getting all raw files
        all_files = os.listdir(self.PATH)

        # Loading all files
        all_dicts = self._load_files(all_files)
        
        # Fill missing values
        all_keys = []
        for element in all_dicts:
            all_keys.extend(list(element.keys()))
        all_keys = list(set(all_keys))

        for element in all_dicts:
            for k in all_keys:
                if k not in element or not element[k]:
                    element[k] = 'Not found'

        return pd.DataFrame(all_dicts)
    
    def _remove_comma_from_ints(self, text):
        return int(text.replace(',', ''))
    
    def _process_ranked(self, text):
        return int(text.replace('#', ''))
    
    def _treat_duplicate_words(self, input):

        if not input or input == np.nan:
            return None

        if type(input) == list:
            return [REMOVE_DUPLICATES_RE.sub(r"\1", i.strip()) for i in input]
        return [REMOVE_DUPLICATES_RE.sub(r"\1", input)]

    def clean(self, data: pd.DataFrame) -> pd.DataFrame:

        # Replacing not found with none
        data = data.replace('Not found', None)

        # Processing ranking variables
        data['ranked'] = data.ranked.apply(self._process_ranked)
        data['popularity'] = data.popularity.apply(self._process_ranked)

        # Processing numeric values
        data['favorites'] = data.favorites.apply(self._remove_comma_from_ints)
        data['members'] = data.members.apply(self._remove_comma_from_ints)
        data['score'] = data.score.apply(lambda x: float(x))

        # Filling columns with the same data and removing unnecessary repetitions
        data['genres'] = data.genres.fillna(data.genre)
        data = data.drop('genre', axis=1)

        data['demographic'] = data.demographic.fillna(data.demographics)
        data = data.drop('demographics', axis=1)

        data['themes'] = data.themes.fillna(data.theme)
        data = data.drop('theme', axis=1)

        # Treating strings with repeated words
        data['genres'] = data.genres.apply(self._treat_duplicate_words)
        data['themes'] = data.themes.apply(lambda x: x.split(',') if x else x).apply(self._treat_duplicate_words)

        return data

    def _process_aired(self, text: str):

        months_dict = {
            'jan': 1,
            'feb': 2,
            'mar': 3,
            'apr': 4,
            'may': 5,
            'jun': 6,
            'jul': 7,
            'aug': 8,
            'sep': 9,
            'oct': 10,
            'nov': 11,
            'dec': 12
        }

        def process_date(text: str):

            try:
                text_list = text.strip().split(' ')
                if len(text_list) == 3:
                    month = months_dict[text_list[0]]
                    day = int(text_list[1])
                    year = int(text_list[2])
                elif len(text_list) == 2:
                    month = months_dict[text_list[0]]
                    day = 1
                    year = int(text_list[1])
                else:
                    month = 1
                    day = 1
                    year = int(text)
            except (KeyError, ValueError, AttributeError):
                return None


            return date(year, month, day)

        txt_list = text.split(' to ')
        txt_list = [i.replace(',', '') for i in txt_list]

        return [process_date(i) for i in txt_list]
    
    def _calculate_anime_duration(self, data_list: List):

        if not data_list or len(data_list) < 2:
            return np.nan

        if not data_list[0] or not data_list[1]:
            return np.nan

        return (data_list[1] - data_list[0]).days

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data['aired'] = data.aired.apply(self._process_aired)
        data['run_length_days'] = data.aired.apply(self._calculate_anime_duration)

        return data
