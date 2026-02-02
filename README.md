# FA Full-Time League & Team Scraper

A high-performance Apify Actor designed to scrape football league structures, seasons, divisions, and team data from [The FA Full-Time](https://fulltime.thefa.com). This Actor utilizes an asynchronous architecture to handle high-volume data extraction with precision and speed.

**Go to the apify actor** [THEFA Leagues and Teams Data Scraper](https://apify.com/autosoldier/thefa-leagues-and-teams-scraper) to run it.

## 🚀 Key Features

* **Asynchronous Architecture**: Leverages `httpx` and `asyncio` for non-blocking I/O, allowing for significantly higher throughput than traditional synchronous scrapers.
* **Incremental Data Processing**: Uses `asyncio.as_completed` to push results to the Apify Dataset immediately as tasks finish. This ensures real-time progress visibility and prevents data loss in case of timeouts.
* **Smart Throttling & IP Rotation**: 
    * **Semaphore Control**: Limits concurrent requests to prevent memory exhaustion and socket errors.
    * **Fresh Connection Logic**: Initializes a new client per request to ensure unique IP rotation via your proxy provider.
* **State Persistence**: Stores league lists in the Apify Key-Value store, allowing for a decoupled workflow between league discovery and team extraction.

---

## 🛠️ Usage & Actions

The Actor operates in two main modes defined by the `action` input:

### 1. get-leagues
Scrapes the A-Z directory to find all available leagues.

* **Alphabetical Crawl**: The Actor iterates through every alphabet group (A-Z) to locate league entries.
* **Deep Metadata Extraction**: For every league found, it automatically drills down to extract associated Season IDs and Division IDs.
* **Storage**: Data is pushed to the Dataset and simultaneously cached in a Key-Value store named `leagues` under the key `ALL` for use in subsequent runs.

### 2. get-teams
Scrapes comprehensive team lists based on the league structures found.

* **Targeting**: It loads the full list of leagues from the `leagues` KV store or targets a specific `league_id` from your input.
* **Full Extraction**: It navigates through every league, every season, and every division to capture every team entry available.
* **Flattened Output**: Unpacks complex nested data into a clean, flat list of team objects for easy analysis.
* **Real-time Updates**: Data is pushed to your dataset league-by-league as the crawl progresses.

---

## 📥 Input Configuration

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| **action** | String | Yes | `get-leagues` or `get-teams`. |
| **group** | String | No | A single letter (A-Z) to limit the search. |
| **league_id** | String | No | Target a specific league ID for team scraping. |
| **proxyConfiguration** | Object | Yes | Apify Proxy settings (Residential recommended). |

### Example Input

**Scrape a specific letter group** Use this to find all leagues starting with a specific letter (e.g., "A").
```json

{
  "action": "get-leagues",
  "group": "A"
}

```
**Scrape all leagues** Use this to perform a full crawl of the entire A-Z directory.
```json

{
  "action": "get-leagues"
}
```

**Scrape teams for a specific league** Use this if you already have a League ID and want to jump straight to the team data.
```json

{
  "action": "get-teams",
  "league_id": "953857299"
}

```

**Scrape teams for all cached leagues** Use this to extract teams for every league found during your last get-leagues run.
```json
{
  "action": "get-teams",
}

```


### Example Output

**Leagues**
```json

[
  {
    "id": "418624324",
    "name": "A&S Interiors Ltd  Devon Football League",
    "url": "https://fulltime.thefa.com/index.html?league=418624324",
    "group": "A",
    "seasons": [
      {
        "id": "982860139",
        "name": "2019-20",
        "selected": false
      },
      {
        "id": "781824432",
        "name": "2020-21",
        "selected": false
      },
      {
        "id": "561675758",
        "name": "2021-22",
        "selected": false
      },
      {
        "id": "782914143",
        "name": "2022-23",
        "selected": false
      },
      {
        "id": "515848069",
        "name": "2023-24",
        "selected": false
      },
      {
        "id": "448065270",
        "name": "2024-25",
        "selected": false
      },
      {
        "id": "329660113",
        "name": "2025-26",
        "selected": true
      }
    ],
    "divisions": [
      {
        "id": "457709114",
        "name": "Devon Football League",
        "selected": true
      }
    ]
  },
  {
    "id": "798677416",
    "name": "A1 Football Factory",
    "url": "https://fulltime.thefa.com/index.html?league=798677416",
    "group": "A",
    "seasons": [
      {
        "id": "468380042",
        "name": "New College Enrichment League 2025 26",
        "selected": false
      },
      {
        "id": "879723696",
        "name": "Wednesday Night League January 2026",
        "selected": true
      }
    ],
    "divisions": [
      {
        "id": "430036677",
        "name": "Wednesday Night Premier League",
        "selected": true
      }
    ]
  }
]

```

**Teams**
```json

[
  {
    "id": "554240695",
    "name": "Dinnington Town J.F.C.",
    "link": "https://fulltime.thefa.com/displayTeam.html?divisionseason=110634580&teamID=554240695",
    "league_id": "1854955",
    "league": "Abacus Lighting Central Midlands Alliance League",
    "division_id": "953857299",
    "division": "Camper UK Premier Division North",
    "season_id": "912243998",
    "season": "2025-26"
  },
  {
    "id": "799175766",
    "name": "Kinsley Boys F.C.",
    "link": "https://fulltime.thefa.com/displayTeam.html?divisionseason=110634580&teamID=799175766",
    "league_id": "1854955",
    "league": "Abacus Lighting Central Midlands Alliance League",
    "division_id": "953857299",
    "division": "Camper UK Premier Division North",
    "season_id": "912243998",
    "season": "2025-26"
  },
  {
    "id": "654223555",
    "name": "Brodsworth Main FC First Team",
    "link": "https://fulltime.thefa.com/displayTeam.html?divisionseason=110634580&teamID=654223555",
    "league_id": "1854955",
    "league": "Abacus Lighting Central Midlands Alliance League",
    "division_id": "953857299",
    "division": "Camper UK Premier Division North",
    "season_id": "912243998",
    "season": "2025-26"
  },
  {
    "id": "321112407",
    "name": "Harworth Colliery FC",
    "link": "https://fulltime.thefa.com/displayTeam.html?divisionseason=110634580&teamID=321112407",
    "league_id": "1854955",
    "league": "Abacus Lighting Central Midlands Alliance League",
    "division_id": "953857299",
    "division": "Camper UK Premier Division North",
    "season_id": "912243998",
    "season": "2025-26"
  }
]

```

