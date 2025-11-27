import os

import pandas as pd
import requests

from odibi import transform


@transform
def fetch_github_issues(context, repo: str, limit: int = 100):
    """
    Fetches recent issues/PRs from a GitHub repository.
    """
    print(f"Fetching issues from GitHub: {repo}")

    headers = {}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"

    url = f"https://api.github.com/repos/{repo}/issues"
    params = {"state": "all", "sort": "created", "direction": "desc", "per_page": min(limit, 100)}

    issues = []
    page = 1

    try:
        while len(issues) < limit:
            response = requests.get(url, headers=headers, params={**params, "page": page})
            if response.status_code != 200:
                print(f"Warning: GitHub API returned {response.status_code}")
                break

            data = response.json()
            if not data:
                break

            for item in data:
                issues.append(
                    {
                        "id": item["id"],
                        "number": item["number"],
                        "title": item["title"],
                        "user": item["user"]["login"],
                        "state": item["state"],
                        "created_at": item["created_at"],
                        "closed_at": item["closed_at"],
                        "is_pr": "pull_request" in item,
                        "comments": item["comments"],
                    }
                )

            if len(issues) >= limit:
                break
            page += 1
    except Exception as e:
        print(f"Error fetching data: {e}")
        # Return empty DF to prevent crash if offline
        if not issues:
            return pd.DataFrame(columns=["id", "title", "created_at"])

    return pd.DataFrame(issues[:limit])


@transform
def calculate_velocity(context, current: pd.DataFrame, window_days: int = 7):
    """
    Calculates weekly velocity.
    """
    if current.empty:
        return pd.DataFrame(columns=["week", "new_issues", "closed_issues"])

    df = current.copy()
    # Convert to timezone-naive UTC
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True).dt.tz_localize(None)
    if "closed_at" in df.columns:
        df["closed_at"] = pd.to_datetime(df["closed_at"], utc=True).dt.tz_localize(None)

    # Group by week start
    df["week"] = df["created_at"].dt.to_period("W").apply(lambda r: r.start_time)

    stats = (
        df.groupby("week")
        .agg(
            new_issues=("id", "count"),
            closed_issues=("closed_at", "count"),
            active_users=("user", "nunique"),
        )
        .reset_index()
    )

    return stats.sort_values("week")
