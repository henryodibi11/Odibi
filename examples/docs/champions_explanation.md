# League of Legends Champions

This node fetches and processes champion data from Riot Games' Data Dragon API.

## Data Source

| Field | Value |
|-------|-------|
| API | Riot Games Data Dragon |
| Endpoint | `/cdn/{version}/data/en_US/champion.json` |
| Update Frequency | Per game patch (every ~2 weeks) |

## API Response Handling

The Data Dragon API returns champions as a **dictionary keyed by champion ID**:

```json
{
  "data": {
    "Aatrox": {"id": "Aatrox", "name": "Aatrox", ...},
    "Ahri": {"id": "Ahri", "name": "Ahri", ...}
  }
}
```

We use `dict_to_list: true` to convert this to a list of rows, with the original key preserved as `_key`.

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| champion_key | string | Original dictionary key (e.g., "Aatrox") |
| id | string | Champion ID (usually same as key) |
| name | string | Display name |
| title | string | Champion title (e.g., "the Darkin Blade") |
| tags | array | Role tags (e.g., ["Fighter", "Tank"]) |
| partype | string | Resource type (Mana, Energy, etc.) |
| blurb | string | Short lore description |

## Business Context

- Used for **game analytics dashboards** and match history reports
- Joined with match data using `champion_key`
- Refresh after each game patch to capture new/reworked champions

## Data Quality Notes

- Champion count should be ~160+ (grows with new releases)
- All champions have unique `id` values
- `tags` array is always non-empty
