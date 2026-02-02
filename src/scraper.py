from apify import Actor
from bs4 import BeautifulSoup
import json, httpx, random, asyncio

class Scraper:
    def __init__(self, max_concurrent=10):
        self.semaphore = asyncio.Semaphore(max_concurrent)

         # Define realistic profiles
        profiles = [
            {
                "os": "Windows", 
                "platform": '"Windows"', 
                "ua_base": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "ch_platform": '"Windows"'
            },
            {
                "os": "macOS", 
                "platform": '"macOS"', 
                "ua_base": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "ch_platform": '"macOS"'
            }
        ]
        profile = random.choice(profiles)

        # Rotate Chrome versions (staying within the latest stable ranges)
        major_ver = random.randint(140, 142)
        build_ver = f"{major_ver}.0.{random.randint(6000, 7000)}.{random.randint(100, 200)}"

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': f'{profile["ua_base"]} AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{build_ver} Safari/537.36',
            'sec-ch-ua': f'"Chromium";v="{major_ver}", "Google Chrome";v="{major_ver}", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': profile["ch_platform"],
        }

    async def get_proxy_url(self):
        if Actor.get_env().get('is_at_home'):
            proxy_cfg = await Actor.create_proxy_configuration(
                )
            
        else:
            proxy_cfg = await Actor.create_proxy_configuration(
                proxy_urls=['http://pcuvlqkouk-res-uk:PC_3TVf5LAKhkG6W8L55@proxy-eu.proxy-cheap.com:5959']
            )

        if not proxy_cfg:
            raise RuntimeError('No proxy configuration available.')
        
        return await proxy_cfg.new_url()
    

    async def get_league_details(self, league:dict):
        async with self.semaphore:
            proxy_url = await self.get_proxy_url()
            # Actor.log.info(f'Using proxy URL: {proxy_url}')

            async with httpx.AsyncClient(proxy=proxy_url, headers=self.headers, follow_redirects=True) as client:
                try:
                    Actor.log.info(f'Getting more details for league {league["id"]}')
                    params = {
                        'league': league['id'],
                    }
                    response = await client.get(
                        'https://fulltime.thefa.com/index.html',
                        params=params,
                        timeout=60.0
                    )

                    if response.status_code != 200: 
                        print(f'Error getting league details for league {league['id']}: {response.text}')
                        return league
                    
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # Get all seasons
                    season_select = soup.select_one('#form1_selectedSeason')
                    all_seasons = []
                    for option in season_select.find_all('option'):
                        all_seasons.append({
                            'id': option['value'],
                            'name': option.text.strip(),
                            'selected': 'selected' in option.attrs
                        })

                    # Get all divisions
                    division_select = soup.select_one('#form1_selectedDivision')
                    all_divisions = []
                    for option in division_select.find_all('option'):
                        all_divisions.append({
                            'id': option['value'],
                            'name': option.text.strip(),
                            'selected': 'selected' in option.attrs
                        })


                    league.update({
                        "seasons": all_seasons,
                        "divisions": all_divisions
                    })

                    return league
                except Exception as e:
                    print(f'Error getting league details for league {league["id"]}: {e}')
                    return league

    async def get_leagues(self, group: str) -> list[dict]:
        proxy_url = await self.get_proxy_url()
        # Actor.log.info(f'Using proxy URL: {proxy_url}')

        async with httpx.AsyncClient(proxy=proxy_url, headers=self.headers, follow_redirects=True) as client:
            try:
                url = f'https://fulltime.thefa.com/home/leagues/{group}.html'
                Actor.log.info(f'Fetching page 1 for group {group}')

                response = await client.get(url, timeout=60.0)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    league_links = soup.select('div.search-results div.results-container')

                    more_pages = soup.select_one('div.paging-container ul')
                    more_pages = more_pages.find_all('li') if more_pages else []

                    if more_pages:
                        for page in more_pages:
                            page = page.find('a').text.strip()
                            if page.isdigit() and int(page) > 1:
                                Actor.log.info(f'Fetching next page {page} for group {group}')
                            
                                response = await client.get(
                                    f'https://fulltime.thefa.com/home/leagues/{group}/{page}.html', 
                                        
                                )

                                if response.status_code != 200:
                                    Actor.log.warning(f'Failed to fetch page {page} for group {group}')
                                    continue

                                soup = BeautifulSoup(response.text, 'html.parser')
                                league_links.extend(soup.select('div.search-results div.results-container'))

                    leagues = []
                    for container in league_links:
                        for link in container.select('a'):
                            league_name = link.text.strip()
                            league_url = link['href']
                            league_id = league_url.split('=')[-1]
                            leagues.append({
                                'id': league_id,
                                'name': league_name,
                                'url': f'https://fulltime.thefa.com{league_url}',
                                'group': group
                            })

                    # Launch all requests at once
                    tasks = [self.get_league_details(lg) for lg in leagues]
                    
                    # This is where the magic happens
                    updated_leagues = await asyncio.gather(*tasks)

                    return updated_leagues

                print(f"[ERROR] THEFA returned status {response.status_code}")
            
            except Exception as e:
                print(f"[ERROR] Request failed: {e}")
        return None
    
    async def get_teams_by_division(self,league_id:str, league_name:str, season:dict, division:dict):
         async with self.semaphore:
            proxy_url = await self.get_proxy_url()
            # Actor.log.info(f'Using proxy URL: {proxy_url}')

            season_id = season.get('id')
            season_name = season.get('name')
            division_id = division.get('id')
            division_name = division.get('name')

            async with httpx.AsyncClient(proxy=proxy_url, headers=self.headers, follow_redirects=True) as client:
                try:

                    params = {
                        'selectedSeason': season_id,
                        'selectedFixtureGroupAgeGroup': '0',
                        'selectedDivision': division_id,
                        'selectedCompetition': '0',
                    }

                    response = await client.get(
                        'https://fulltime.thefa.com/table.html',
                        params=params,
                        timeout=60.0
                    )

                    if response.status_code != 200: 
                        print(f'Error getting teams for league {league_id}, division {division_id}: {e}')
                        return None
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    table = soup.select_one('.tab-1 table')
                    if 'Team' not in table.select_one('thead').text:
                        print(f'No teams data for league {league_id}, division {division_id}')
                        return None
                    
                    team_list = []
                    teams = table.select('tbody tr td[class="left"] a')
                    if teams:
                        for team in teams:
                            if team:
                                team_name = team.text.strip()
                                team_link = team['href']
                                team_id = team_link.split('teamID=')[-1]

                                team_list.append({
                                    'id': team_id,
                                    'name': team_name,
                                    'link': f'https://fulltime.thefa.com{team_link}',
                                    'league_id': league_id,
                                    'league': league_name,
                                    'division_id': division_id,
                                    'division': division_name,
                                    'season_id': season_id,
                                    'season': season_name
                                })
                    return team_list
                except Exception as e:
                    print(f'Error getting teams for league {league_id}, division {division_id}: {e}')
                    return None

    async def get_teams(self, league:dict) -> list[dict]:
        try:
            divisions = league.get('divisions')
            selected_season = [season for season in league.get('seasons') if season.get('selected')][-1]

            league_id = league.get('id')
            league_name = league.get('name')

            teams_list = []

            # Launch all requests at once
            tasks = [self.get_teams_by_division(league_id,league_name,selected_season,division) for division in divisions]
            
            # This is where the magic happens
            results = await asyncio.gather(*tasks)
            for teams in results:
                if teams:
                    teams_list.extend(teams)

            return teams_list
            
        except Exception as e:
            print(f"[ERROR] Could not get teams for league {league.get('id')}: {e}")