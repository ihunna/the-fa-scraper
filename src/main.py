from __future__ import annotations
import uuid, hashlib, json, asyncio
from apify import Actor
from .scraper import Scraper


async def main() -> None:
    async with Actor:
        scraper: Scraper = Scraper()
        
        actor_input = await Actor.get_input() or {}
        action = actor_input.get('action', 'get-leagues')

        if action == 'get-leagues':
            input_group = actor_input.get('group', None)
            
            if not input_group:
                groups = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 
                          'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
            else:
                groups = [input_group]

            group_semaphore = asyncio.Semaphore(3)
            async def limited_group_scrape(group_code):
                async with group_semaphore:
                    return await scraper.get_leagues(group_code)

            # 1. Create a list of tasks for each group
            # We strip/upper here so the scraper gets clean strings
            tasks = [limited_group_scrape(str(g).strip().upper()) for g in groups]

            # 2. Run all groups concurrently
            # This will return a list of lists (each list containing leagues for that letter)
            Actor.log.info(f"Launching {len(tasks)} concurrent group scrapers...")

            for task in asyncio.as_completed(tasks):
                leagues = await task
                if leagues:
                    # Add to our local list for the final OUTPUT and KV store
                    all_leagues.extend(leagues)
                    
                    # Push data to the Apify dataset immediately
                    await Actor.push_data(leagues)
                    
                    Actor.log.info(f"Pushed {len(leagues)} leagues to dataset.")

            # 4. Final storage
            await Actor.set_value('OUTPUT', all_leagues)
            
            kv = await Actor.open_key_value_store(name='leagues')
            key = str(input_group).strip().upper() if input_group else 'ALL'
            await kv.set_value(key, all_leagues)

            await Actor.set_status_message(f'Scraped {len(all_leagues)} leagues across {len(groups)} groups')
            print(f'Scraped {len(all_leagues)} leagues across {len(groups)} groups')
        

        # --- Action: Get Teams ---
        elif action == 'get-teams':
            league_id = actor_input.get('league_id')
            all_leagues = []

            # Try to load from the Key-Value store
            kv = await Actor.open_key_value_store(name='leagues')
            all_leagues = await kv.get_value('ALL')

            if league_id:
                # If a specific ID is provided, get that league
                all_leagues = [league for league in all_leagues if league.get('id') == league_id]

            # Validation: Check if we actually have leagues to work with
            if not all_leagues:
                msg = "No league data to scrape teams, scrape leagues first"
                await Actor.set_status_message(msg)
                Actor.log.warning(msg)
                return

            Actor.log.info(f"Scraping teams for {len(all_leagues)} leagues...")

            league_semaphore = asyncio.Semaphore(5)
            async def limited_league_scrape(league):
                async with league_semaphore:
                    return await scraper.get_teams(league)

            # We use the same get_league_details or a new get_teams method
            tasks = [limited_league_scrape(lg) for lg in all_leagues]
            all_teams = []


            for task in asyncio.as_completed(tasks):
                result = await task
                if result:
                    # Push to Apify dataset immediately
                    await Actor.push_data(result)
                    all_teams.extend(result)


            await Actor.set_value('OUTPUT', all_teams)

            kv = await Actor.open_key_value_store(name='teams')
            key = league_id if league_id else 'ALL'
            await kv.set_value(key, all_teams)

            await Actor.set_status_message(f'Scraped {len(all_teams)} teams across {len(all_leagues)} leagues')
            print(f'Scraped {len(all_teams)} teams across {len(all_leagues)} leagues')

        else:
            await Actor.fail(f"Unknown action: {action}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())