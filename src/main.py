from __future__ import annotations
import uuid, hashlib, json, asyncio
from datetime import datetime, timedelta, timezone
from apify import Actor
from .scraper import Scraper

async def main() -> None:
    async with Actor:
        # --- Helper: Check Cache Expiry (7 Days) ---
        def is_expired(stored_obj: dict | None) -> bool:
            if not stored_obj or 'timestamp' not in stored_obj or not stored_obj:
                return True
            try:
                stored_at = datetime.fromisoformat(stored_obj['timestamp'])
                # Current UTC time vs stored UTC time
                return datetime.now(timezone.utc) - stored_at > timedelta(days=7)
            except (ValueError, TypeError):
                return True

        # Initialize Scraper with UK Datacenter Proxy settings
        actor_input = await Actor.get_input() or {}
        
        scraper: Scraper = Scraper()
        
        action = actor_input.get('action', 'get-leagues')
        force_refresh = actor_input.get('force_refresh', False)

        # ---------------------------------------------------------
        # ACTION: GET LEAGUES
        # ---------------------------------------------------------
        if action == 'get-leagues':
            input_group = actor_input.get('group', None)
            kv = await Actor.open_key_value_store(name='leagues')
            cache_key = str(input_group).strip().upper() if input_group else 'ALL'
            
            # Check Cache
            cached_leagues = await kv.get_value(cache_key)
            if not is_expired(cached_leagues) and not force_refresh:
                Actor.log.info(f"Leagues for {cache_key} are fresh (< 7 days). Serving from cache.")
                all_leagues = cached_leagues['data']
                await Actor.push_data(all_leagues)
            else:
                Actor.log.info(f"Leagues for {cache_key} expired or missing. Scraping fresh...")
                if not input_group:
                    groups = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 
                              'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
                else:
                    groups = [input_group]

                group_semaphore = asyncio.Semaphore(10)
                async def limited_group_scrape(group_code):
                    async with group_semaphore:
                        return await scraper.get_leagues(group_code)

                all_leagues = []
                tasks = [limited_group_scrape(str(g).strip().upper()) for g in groups]
                Actor.log.info(f"Launching {len(tasks)} concurrent group scrapers...")

                for task in asyncio.as_completed(tasks):
                    leagues = await task
                    if leagues:
                        all_leagues.extend(leagues)
                        await Actor.push_data(leagues)
                        Actor.log.info(f"Pushed {len(leagues)} leagues to dataset.")

                # Save to KV with Timestamp
                await kv.set_value(cache_key, {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': all_leagues
                })

            await Actor.set_value('OUTPUT', all_leagues)
            await Actor.set_status_message(f'Processed {len(all_leagues)} leagues.')

        # ---------------------------------------------------------
        # ACTION: GET TEAMS
        # ---------------------------------------------------------
        elif action == 'get-teams':
            league_id = actor_input.get('league_id')
            kv_leagues = await Actor.open_key_value_store(name='leagues')
            kv_teams = await Actor.open_key_value_store(name='teams')
            
            # 1. Load League Structure (Cache for leagues must exist)
            leagues_data_obj = await kv_leagues.get_value('ALL')
            if not leagues_data_obj:
                await Actor.fail("No league directory found. Please run 'get-leagues' first.")
                return
            
            target_leagues = leagues_data_obj['data'] if leagues_data_obj else []
            if league_id:
                target_leagues = [lg for lg in target_leagues if lg.get('id') == league_id]

            if not target_leagues:
                await Actor.fail(f"League ID {league_id} not found in directory.")
                return

            # 2. Check Team Cache
            team_cache_key = league_id if league_id else 'ALL'
            cached_teams = await kv_teams.get_value(team_cache_key)

            if not is_expired(cached_teams) and not force_refresh:
                Actor.log.info(f"Teams for {team_cache_key} are fresh. Serving from cache.")
                all_teams = cached_teams['data']
                await Actor.push_data(all_teams)
            else:
                Actor.log.info(f"Teams for {team_cache_key} expired or missing. Scraping...")
                league_semaphore = asyncio.Semaphore(10)
                async def limited_league_scrape(league):
                    async with league_semaphore:
                        return await scraper.get_teams(league)

                tasks = [limited_league_scrape(lg) for lg in target_leagues]
                all_teams = []

                for task in asyncio.as_completed(tasks):
                    result = await task
                    if result:
                        await Actor.push_data(result)
                        all_teams.extend(result)

                # Save Teams to KV with Timestamp
                await kv_teams.set_value(team_cache_key, {
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': all_teams
                })

            await Actor.set_value('OUTPUT', all_teams)
            await Actor.set_status_message(f'Processed {len(all_teams)} teams.')

        else:
            await Actor.fail(f"Unknown action: {action}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())