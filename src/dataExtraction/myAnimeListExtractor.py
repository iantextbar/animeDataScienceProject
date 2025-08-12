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

DATAPATH = "../../data/raw/"

#-------------------------
# Compiled Regex
GENRE_DUPLICATE_RE = re.compile(r"\b(\w+)\b\s*\1\b", re.IGNORECASE)
SCORE_RE = re.compile(r"(\d+(?:[.,]\d+)?)\D*(\d+(?:,\d+)?)")

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
            return "No image"

        return response.content

    def _parse_titles(self, soup: bs4.BeautifulSoup, link: str) -> dict:
        try:
            return {
                "title": soup.find("h1", class_="title-name h1_bold_none").text.strip(),
                "title_english": soup.find("p", class_="title-english title-inherit").text.strip()
            }
        except AttributeError:
            title_from_link = link.split("/")[-1]
            return {"title": title_from_link, "title_english": title_from_link}
        
    def _parse_synopsis(self, soup: bs4.BeautifulSoup) -> str:
        synopsis_tag = soup.find("p", itemprop="description")
        return synopsis_tag.text.strip() if synopsis_tag else "Not found"

    def _parse_metadata(self, soup: bs4.BeautifulSoup) -> dict:
        data = {}
        for element in soup.find_all("div", class_="spaceit_pad"):
            if not element.span:
                continue

            text_element = element.get_text(separator=" ", strip=True)
            if ":" not in text_element:
                continue

            field, value = [part.strip() for part in text_element.split(":", 1)]
            field = field.lower()

            if field == "genres":
                value = [GENRE_DUPLICATE_RE.sub(r"\1", v).strip() for v in value.split(",")]
            elif field == "score":
                match = SCORE_RE.search(value)
                data["score"] = match.group(1) if match else "Not found"
                data["score_count"] = match.group(2) if match else "Not found"
            elif field == "ranked":
                value = value.replace("22    based on the top anime page. please note that 'not yet aired' and 'r18+' titles are excluded.", "").strip()

            data[field] = value
        return data

    def _extract_elements_anime_page(self, html_text: str, link: str) -> dict:
        """
        Extract relevant information from an anime page's HTML content.

        Args:
            html_text (str): Raw HTML of the anime page.
            link (str): URL to the anime page.

        Returns:
            dict: Extracted fields including:
                - title
                - title_english
                - synopsis
                - genres
                - demographic
                - score
                - score_count
                - rank
                - image (bytes)
        """
        soup = bs4.BeautifulSoup(html_text, "html.parser")
        result = {}

        result.update(self._parse_titles(soup, link))
        result["synopsis"] = self._parse_synopsis(soup)
        result.update(self._parse_metadata(soup))
        result["image"] = self._get_image_bytes(soup)

        return result

    def fetch_anime_data(self, link: str) -> Dict[str, Any]:
        """
        Fetches and extracts relevant data for a given anime page link with exponential backoff.
        _________________
            link: (str) - URL to the anime's MyAnimeList page.
        """
        max_retries = 3
        backoff = 2  
        for attempt in range(max_retries):
            if attempt == 2:
                sleep(random.randint(10, 15))
            try:
                response = requests.get(
                    link,
                    headers=self.headers,
                    timeout=random.randint(1, 3)
                )

                if response.status_code == 200:
                    return self._extract_elements_anime_page(response.text, link)
                elif response.status_code in [403, 429, 503]:
                    print(f"Blocked or rate-limited on attempt {attempt+1}, retrying after {backoff} seconds...")
                    sleep(backoff)
                    backoff *= 2  # exponential backoff
                else:
                    print(f"Unexpected status code {response.status_code} for {link}")
                    break

            except requests.exceptions.RequestException as e:
                print(f"Request failed on attempt {attempt+1}: {e}")
                sleep(backoff)
                backoff *= 2

    def fetch_all_anime_data(
            self, startingPoint: int = 0, totalResults: int = 1000
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
        anime_links = self.get_top_anime_links(startingPoint, totalResults)
        print('######### FINISHED COLLECTING ALL LINKS #########')

        all_animes = []

        print('######### COLLECTING DATA FROM EACH LINK #########')
        for link in tqdm(anime_links, desc="Links Processed", total=totalResults):

            animeDict = self.fetch_anime_data(link)
            if not animeDict:
                print('Lets wait a little...')
                sleep(random.randint(15, 30))
                continue

            all_animes.append(animeDict)
            file_name = DATAPATH + animeDict['title'].replace('/', '') + datetime.date.today().strftime('%Y%m%d') + '.pkl'
            with open(file_name, 'wb') as handler:
                pickle.dump(animeDict, handler)
            print('Collected data for:', animeDict['title'].replace('/', ''))
            sleep(random.uniform(1.5, 3))
        print('######### DATA COLLECTED #########')

if __name__ == "__main__":
    
    scraper = MyAnimeListExtractor()
    scraper.fetch_all_anime_data(startingPoint=50, totalResults=150)


