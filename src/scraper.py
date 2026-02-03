from apify import Actor
from bs4 import BeautifulSoup
import json, httpx, random, asyncio, os, hashlib
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

class Scraper:
    def __init__(self, max_concurrent=10):
        self.semaphore = asyncio.Semaphore(max_concurrent)

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
        major_ver = random.randint(120, 122)
        build_ver = f"{major_ver}.0.{random.randint(6000, 7000)}.{random.randint(100, 200)}"

        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'User-Agent': f'{profile["ua_base"]} AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{build_ver} Safari/537.36',
            'sec-ch-ua': f'"Chromium";v="{major_ver}", "Google Chrome";v="{major_ver}", "Not_A Brand";v="99"',
            'sec-ch-ua-platform': profile["ch_platform"],
            'Upgrade-Insecure-Requests': '1',
        }

    async def get_proxy_url(self, session_id: str = None):
        proxy_url_env = os.getenv('PROXY_URL')
        proxy_cfg = await Actor.create_proxy_configuration(proxy_urls=[proxy_url_env])
        
        if not proxy_cfg:
            raise RuntimeError('No proxy configuration available.')

        return await proxy_cfg.new_url(session_id=session_id) if session_id else await proxy_cfg.new_url()

    async def process_league_batch(self, leagues_batch: list, group: str):
        batch_id = hashlib.md5(str([lg['id'] for lg in leagues_batch]).encode()).hexdigest()[:8]
        session_name = f"batch_leagues_{batch_id}"
        proxy_url = await self.get_proxy_url(session_id=session_name)
        
        Actor.log.info(f"🚀 Starting League Batch [{batch_id}] | Size: {len(leagues_batch)} | Session: {session_name}")
        
        async with httpx.AsyncClient(proxy=proxy_url, headers=self.headers, http2=True, follow_redirects=True) as client:
            tasks = [self.get_league_details(client, lg) for lg in leagues_batch]
            results = await asyncio.gather(*tasks)
            Actor.log.info(f"✅ Finished League Batch [{batch_id}]")
            return results

    async def get_league_details(self, client: httpx.AsyncClient, league: dict):
        async with self.semaphore:
            try:
                Actor.log.info(f"🔍 Fetching details for League: {league.get('name')} ({league.get('id')})")
                response = await client.get(
                    'https://fulltime.thefa.com/index.html',
                    params={'league': league['id']},
                    timeout=30.0
                )
                if response.status_code != 200: 
                    Actor.log.error(f"❌ Failed to get details for {league['id']} - Status: {response.status_code}")
                    return league
                
                soup = BeautifulSoup(response.text, 'lxml')
                seasons = [{'id': o['value'], 'name': o.text.strip(), 'selected': 'selected' in o.attrs} for o in soup.select('#form1_selectedSeason option')]
                divisions = [{'id': o['value'], 'name': o.text.strip(), 'selected': 'selected' in o.attrs} for o in soup.select('#form1_selectedDivision option')]
                
                league.update({"seasons": seasons, "divisions": divisions})
                Actor.log.info(f"📊 Found {len(seasons)} seasons and {len(divisions)} divisions for {league.get('name')}")
                return league
            except Exception as e:
                Actor.log.error(f"💥 Error in league details {league['id']}: {repr(e)}")
                return league

    async def get_leagues(self, group: str, batch_size: int = 20) -> list[dict]:
        """Scrapes league directory including pagination, then processes details in batches."""
        proxy_url = await self.get_proxy_url(session_id=f"dir_{group}")
        Actor.log.info(f"📂 Scraping League Directory for Group: {group}")
        
        async with httpx.AsyncClient(proxy=proxy_url, headers=self.headers, http2=True) as client:
            try:
                # 1. Fetch the first page
                url = f'https://fulltime.thefa.com/home/leagues/{group}.html'
                res = await client.get(url, timeout=30.0)
                if res.status_code != 200: return []
                
                soup = BeautifulSoup(res.text, 'lxml')
                # Start with the containers from page 1
                containers = soup.select('div.search-results div.results-container')
                
                # --- Pagination Logic Restored ---
                paging_ul = soup.select_one('div.paging-container ul')
                more_pages = paging_ul.find_all('li') if paging_ul else []

                if more_pages:
                    for li in more_pages:
                        link_tag = li.find('a')
                        if not link_tag: continue
                        
                        page_num = link_tag.text.strip()
                        if page_num.isdigit() and int(page_num) > 1:
                            Actor.log.info(f'📄 Fetching next page {page_num} for group {group}')
                            
                            p_res = await client.get(
                                f'https://fulltime.thefa.com/home/leagues/{group}/{page_num}.html',
                                timeout=30.0
                            )

                            if p_res.status_code != 200:
                                Actor.log.warning(f'⚠️ Failed to fetch page {page_num} for group {group}')
                                continue

                            p_soup = BeautifulSoup(p_res.text, 'lxml')
                            containers.extend(p_soup.select('div.search-results div.results-container'))
                # --- End Pagination Logic ---

                # Extract league IDs and Names
                leagues = []
                for container in containers:
                    for link in container.select('a'):
                        leagues.append({
                            'id': link['href'].split('=')[-1], 
                            'name': link.text.strip(), 
                            'group': group
                        })

                Actor.log.info(f"💡 Found {len(leagues)} total leagues in Group {group}. Processing details in IP-batches...")

                # 2. Process league details (seasons/divisions) in batches
                all_updated = []
                for i in range(0, len(leagues), batch_size):
                    batch = leagues[i : i + batch_size]
                    all_updated.extend(await self.process_league_batch(batch, group))
                
                return all_updated

            except Exception as e:
                Actor.log.error(f"💥 Group {group} overall failure: {repr(e)}")
                return []

    async def process_division_batch(self, division_batch: list, league_id: str, league_name: str, season: dict):
        batch_id = hashlib.md5(str([d['id'] for d in division_batch]).encode()).hexdigest()[:8]
        session_name = f"batch_teams_{batch_id}"

        proxy_url = await self.get_proxy_url(session_id=session_name)
        Actor.log.info(f"📦 Starting Division Batch [{batch_id}] for League: {league_name} | Size: {len(division_batch)}")
        
        async with httpx.AsyncClient(proxy=proxy_url, headers=self.headers, http2=True) as client:
            tasks = [self.get_teams_by_division(client, league_id, league_name, season, div) for div in division_batch]
            results = await asyncio.gather(*tasks)
            flattened = [team for sublist in results if sublist for team in sublist]
            Actor.log.info(f"✅ Finished Division Batch [{batch_id}] | Total Teams Found: {len(flattened)}")
            return flattened

    async def get_teams_by_division(self, client: httpx.AsyncClient, league_id: str, league_name: str, season: dict, division: dict):
        async with self.semaphore:
            try:
                Actor.log.info(f"⚽ Scraping Teams: {league_name} > {division['name']}")

                params = {'selectedSeason': season['id'], 'selectedDivision': division['id']}
                res = await client.get('https://fulltime.thefa.com/table.html', params=params, timeout=30.0)
                # Actor.log.info(res.status_code)

                if res.status_code != 200: 
                    Actor.log.error(f"❌ Failed teams for {division['name']} - Status: {res.status_code}")
                    return None
                
                soup = BeautifulSoup(res.text, 'lxml')
                teams = soup.select('.tab-1 table tbody tr td.left a')
                
                parsed_teams = [{
                    'id': t['href'].split('teamID=')[-1], 
                    'name': t.text.strip(), 
                    'league_id': league_id, 
                    'division': division['name']
                } for t in teams]
                
                Actor.log.info(f"✔️ Parsed {len(parsed_teams)} teams from {division['name']}")
                return parsed_teams
            except Exception as e:
                Actor.log.error(f"💥 Error scraping division {division['id']}: {repr(e)}")
                return None

    async def get_teams(self, league: dict, batch_size: int = 20) -> list[dict]:
        divisions = league.get('divisions', [])
        seasons = league.get('seasons', [])
        
        # --- DEFENSIVE CHECK START ---
        if not seasons:
            Actor.log.warning(f"⚠️ Skipping league {league.get('name')} ({league.get('id')}): No seasons found.")
            return []
            
        selected_seasons = [s for s in seasons if s.get('selected')]
        if not selected_seasons:
            # Fallback: if none are marked 'selected', take the first one available
            Actor.log.info(f"ℹ️ No 'selected' season for {league.get('name')}, defaulting to latest.")
            season = seasons[-1] 
        else:
            season = selected_seasons[-1]
        # --- DEFENSIVE CHECK END ---
        
        if not divisions:
            Actor.log.info(f"ℹ️ No divisions found for league {league.get('name')}. Skipping teams.")
            return []
        
        Actor.log.info(f"🛠️ Processing League: {league['name']} | Total Divisions: {len(divisions)}")
        
        all_teams = []
        for i in range(0, len(divisions), batch_size):
            batch = divisions[i : i + batch_size]
            all_teams.extend(await self.process_division_batch(batch, league['id'], league['name'], season))
        
        Actor.log.info(f"🏁 Completed League: {league['name']} | Total Teams Gathered: {len(all_teams)}")
        return all_teams