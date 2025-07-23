from typing import List, Dict, Any
import random
import pickle
import re
from time import sleep
import datetime

import requests
import pandas as pd
from tqdm import tqdm
import bs4

#-------------------------
# Global Variables

DATAPATH = "../../data/"

#-------------------------

class MyAnimeListExtractor:

    """
    A simple webscraper for data on MyAnimeList.
    """
    
    def __init__(
            self
        ) -> None:

        self.base_url = "https://myanimelist.net/topanime.php"
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en,en-GB;q=0.9,en-US;q=0.8,pt;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
            }


    def _get_all_links_from_page(
            self, pageResults = int
        ) -> List[str]:
        """
        For a given results page, extract the 50 links relative to MyAnimeList anime pages.
        _________________
            pageResults: (int) - Multiple of 50.
        """
        
        assert pageResults % 50 == 0

        try:
            payloads = {'limit':pageResults}
            response = requests.post(self.base_url, params=payloads, headers=self.headers, timeout=random.randint(2, 5))
        except Exception as e:
            print(f'ERROR while extracting results in range {pageResults}', str(e.value))

        # Extract links from page html
        soup = bs4.BeautifulSoup(response.text, 'html.parser')
        links = [element.a.get('href') for element in soup.find_all('td', class_='title al va-t word-break')]

        return links
    
    def get_top_anime_links(
            self, startingPoint: int = 0, totalResults: int = 1000
        ) -> List[str]:
        """
        Extract the top 'totalResults' links for anime pages on MyAnimeList.
        _________________
            totalResults: (int) - Number of links to extract.
                Default = 1000
        """

        results = []
        endPoint = startingPoint + totalResults + 1
        for i in tqdm(range(startingPoint, endPoint, 50), desc='Collecting Links', total=(totalResults+1)//50):
            # Extract results from page and add them to total results
            pageResults = self._get_all_links_from_page(pageResults=i)
            results.extend(pageResults)
        
        return results

    def _get_image_bytes(self, soup):
        """
        Extracts the image from an anime page's BeautifulSoup object and returns its byte content.
        _________________
            soup: (bs4.BeautifulSoup) - Parsed HTML of the anime page.
        """
        link = soup.find_all('img', class_='ac')[0].get('data-src')

        try:
            response = requests.get(link, headers=self.headers, timeout=random.uniform(0.5, 1.5))
        except:
            print('Could not extract image from link:', link)

        return response.content

    def _extract_elements_anime_page(self, html_text, link):
        """
        Extracts relevant information from an anime page's HTML content.
        Returns a dictionary with title, synopsis, genres, demographic, score, rank, and image.
        _________________
            html_text: (str) - Raw HTML of the anime page.
        """

        soup = bs4.BeautifulSoup(html_text, 'html.parser')
        resultDict = {}
        
        # Title
        try:
          title = soup.find('h1', class_="title-name h1_bold_none").text
          title_english = soup.find('p', class_="title-english title-inherit").text
          resultDict['title'] = title
          resultDict['title_english'] = title_english
        except:
          title_from_link = link.split('/')[-1]
          resultDict['title'] = title_from_link
          resultDict['title_english'] = title_from_link

        # Synopsis
        synopsis = [item.text for item in soup.find_all('p') if item.get('itemprop') == 'description'][0]
        resultDict['synopsis'] = synopsis

        # Genre and rating, demographic and ranking
        for element in soup.find_all('div', class_='spaceit_pad'):

            text_element = element.get_text()

            if not element.span:
              continue
          
            if ':' in text_element:
            
                text_list = text_element.lower().split(':')
                field = text_list[0].replace('\n', '').strip()
                value = text_list[1].replace('\n', '').strip()

                if field == 'genres':
                  regex_pattern = r"(\w+)\1"
                  value = re.sub(regex_pattern, r"\1", value)
                  value = re.sub(r'\s', "", value)
                  value = value.split(',')

                resultDict[field] = value
  
                if field == 'score':
                  regex_pattern = r"(\d+(?:[.,]\d+)?)\D*(\d+(?:,\d+)?)"
                  match = re.search(regex_pattern, value)
  
                  if match:
                      first_number = match.group(1)
                      second_number = match.group(2)
                      resultDict['score'] = first_number
                      resultDict['score_count'] = second_number
                  else:
                      resultDict['score'] = "Not found"
                      resultDict['score_count'] = "Not found"

                if field == 'demographic':
                  resultDict[field] = value

                if field == 'ranked':
                  ranking = value.replace("22    based on the top anime page. please note that 'not yet aired' and 'r18+' titles are excluded.", "")
                  resultDict[field] = ranking

        if 'genres' not in resultDict:
            resultDict['genres'] = "Not found"
        if 'score' not in resultDict:
            resultDict['score'] = "Not found"
            resultDict['score_count'] = "Not found"
        if 'demographic' not in resultDict:
            resultDict['demographic'] = "Not found"
        if 'ranked' not in resultDict:
            resultDict['ranked'] = "Not found"
                      

        # Getting image
        resultDict['image'] = self._get_image_bytes(soup)

        return resultDict

    def fetch_anime_data(
            self, link: str
        ) -> Dict[str, Any]:
        """
        Fetches and extracts relevant data for a given anime page link.
        _________________
            link: (str) - URL to the anime's MyAnimeList page.
        """
        try:
            response = requests.get(link, headers=self.headers, timeout=random.randint(1, 3))
        except:
            print('Unable to get results for link', link)

        # Extract elements we are interested in from html
        elements_dict = self._extract_elements_anime_page(response.text, link)

        return elements_dict

    def fetch_all_anime_data(
            self, totalResults: int = 1000
        ):
        """
        Iterates over top anime links and collects detailed data for each.
        Saves the result as a .parquet file.
        _________________
            totalResults: (int) - Number of anime entries to collect.
                Must be a multiple of 50.
        """
        assert totalResults % 50 == 0

        print('######### STARTED COLLECTING ALL LINKS #########')
        anime_links = self.get_top_anime_links(totalResults)
        print('######### FINISHED COLLECTING ALL LINKS #########')

        all_animes = []

        print('######### COLLECTING DATA FROM EACH LINK #########')
        for link in tqdm(anime_links, desc="Links Processed", total=totalResults):

            animeDict = self.fetch_anime_data(link)
            all_animes.append(animeDict)
            file_name = animeDict['title'] + datetime.date.today().strftime('%Y%m%d') + '.pkl'
            with open(file_name, 'wb') as handler:
                pickle.dump(animeDict, handler)
            sleep(random.uniform(1.5, 3))
        print('######### DATA COLLECTED #########')

if __name__ == "__main__":
    
    scraper = MyAnimeListExtractor()
    scraper.fetch_all_anime_data(totalResults=1000)


