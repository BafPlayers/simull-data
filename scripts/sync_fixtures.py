import concurrent.futures
import datetime
import json
import time
import urllib.request
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
ID_MAP_PATH = REPO_ROOT / "espn_id_map.json"
CATALOG_PATH = REPO_ROOT / "store_safe_catalog.json"
MANIFEST_PATH = REPO_ROOT / "manifest.json"

SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/tur.1/scoreboard"
DERBY_TEAM_IDS = {325, 326, 327}
TR_TZ = datetime.timezone(datetime.timedelta(hours=3))

TR_MONTHS = {
    1: "Oca",
    2: "Şub",
    3: "Mar",
    4: "Nis",
    5: "May",
    6: "Haz",
    7: "Tem",
    8: "Ağu",
    9: "Eyl",
    10: "Eki",
    11: "Kas",
    12: "Ara",
}

TR_WEEKDAYS = {
    0: "Pzt",
    1: "Sal",
    2: "Çar",
    3: "Per",
    4: "Cum",
    5: "Cmt",
    6: "Paz",
}


def fetch_json(url: str, retries: int = 3) -> dict:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=25) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as err:
            last_err = err
            if attempt + 1 < retries:
                time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"Failed fetching {url}: {last_err}")


def format_kickoff_label(dt_tr: datetime.datetime, include_time: bool) -> str:
    mon = TR_MONTHS[dt_tr.month]
    wday = TR_WEEKDAYS[dt_tr.weekday()]
    base = f"{dt_tr.day} {mon} {wday}"
    if include_time:
        return f"{base} {dt_tr.hour:02d}:{dt_tr.minute:02d}"
    return base


def build_real_season_weeks(team_map: dict[str, int]) -> list[dict]:
    sb = fetch_json(SCOREBOARD_URL)
    cal = sb.get("leagues", [{}])[0].get("calendar", [])
    dates = sorted({c[:10].replace("-", "") for c in cal if isinstance(c, str) and len(c) >= 10})
    if not dates:
        raise RuntimeError("Scoreboard returned empty season calendar")

    events_by_id: dict[str, dict] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        for res in ex.map(lambda d: fetch_json(f"{SCOREBOARD_URL}?dates={d}"), dates):
            for ev in res.get("events", []):
                eid = str(ev.get("id", ""))
                if eid:
                    events_by_id[eid] = ev

    if len(events_by_id) != 306:
        raise RuntimeError(f"Expected 306 unique events, got {len(events_by_id)}")

    parsed_events = []
    seen_pairs: set[tuple[int, int]] = set()
    team_counts: Counter[int] = Counter()

    for ev in sorted(events_by_id.values(), key=lambda e: (e["date"], int(e["id"]))):
        comp = ev["competitions"][0]
        competitors = comp["competitors"]
        home_raw = next(c for c in competitors if c.get("homeAway") == "home")
        away_raw = next(c for c in competitors if c.get("homeAway") == "away")
        ext_hid = str(home_raw["team"]["id"])
        ext_aid = str(away_raw["team"]["id"])
        if ext_hid not in team_map or ext_aid not in team_map:
            raise RuntimeError(f"Unknown team ID in event {ev['id']}: {ext_hid} vs {ext_aid}")

        sim_hid = int(team_map[ext_hid])
        sim_aid = int(team_map[ext_aid])
        seen_pairs.add((sim_hid, sim_aid))
        team_counts[sim_hid] += 1
        team_counts[sim_aid] += 1

        dt_utc = datetime.datetime.fromisoformat(ev["date"].replace("Z", "+00:00")).astimezone(
            datetime.timezone.utc
        )
        dt_tr = dt_utc.astimezone(TR_TZ)
        parsed_events.append(
            {
                "eventId": str(ev["id"]),
                "homeTeamId": sim_hid,
                "awayTeamId": sim_aid,
                "dtUtc": dt_utc,
                "dtTr": dt_tr,
            }
        )

    if len(seen_pairs) != 306:
        raise RuntimeError(f"Expected 306 unique pairs, got {len(seen_pairs)}")
    if len(team_counts) != 18 or set(team_counts.values()) != {34}:
        raise RuntimeError(f"Invalid team match distribution: {dict(team_counts)}")

    chunks: list[list[dict]] = []
    cur: list[dict] = []
    cur_teams: set[int] = set()

    for item in parsed_events:
        if cur:
            gap_days = (item["dtTr"].date() - cur[-1]["dtTr"].date()).days
            if (
                len(cur) == 9
                or item["homeTeamId"] in cur_teams
                or item["awayTeamId"] in cur_teams
                or gap_days >= 4
            ):
                chunks.append(cur)
                cur = []
                cur_teams = set()
        cur.append(item)
        cur_teams.add(item["homeTeamId"])
        cur_teams.add(item["awayTeamId"])

    if cur:
        chunks.append(cur)

    if len(chunks) != 34:
        raise RuntimeError(f"Expected 34 weeks, got {len(chunks)}")

    weeks: list[dict] = []
    for idx, week_events in enumerate(chunks):
        week_num = idx + 1
        if len(week_events) != 9:
            raise RuntimeError(f"Week {week_num} has {len(week_events)} fixtures instead of 9")
        distinct_teams = {e["homeTeamId"] for e in week_events} | {
            e["awayTeamId"] for e in week_events
        }
        if len(distinct_teams) != 18:
            raise RuntimeError(f"Week {week_num} covers {len(distinct_teams)} teams instead of 18")

        distinct_utc = {e["dtUtc"] for e in week_events}
        include_time = len(distinct_utc) > 1

        start_date = week_events[0]["dtTr"].date().isoformat()
        end_date = week_events[-1]["dtTr"].date().isoformat()

        fixtures_out = []
        for fx_idx, ev in enumerate(week_events):
            hid = ev["homeTeamId"]
            aid = ev["awayTeamId"]
            fixtures_out.append(
                {
                    "id": f"tr-w{week_num}-fx-{fx_idx + 1}",
                    "externalMatchId": ev["eventId"],
                    "homeTeamId": hid,
                    "awayTeamId": aid,
                    "kickoffUtc": ev["dtUtc"].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "kickoffLabel": format_kickoff_label(ev["dtTr"], include_time=include_time),
                    "isDerby": hid in DERBY_TEAM_IDS and aid in DERBY_TEAM_IDS,
                }
            )

        weeks.append(
            {
                "weekNumber": week_num,
                "weekLabel": f"{week_num}. Hafta",
                "startDate": start_date,
                "endDate": end_date,
                "fixtures": fixtures_out,
            }
        )

    return weeks


def main() -> None:
    id_map = json.loads(ID_MAP_PATH.read_text(encoding="utf-8"))
    team_map = {str(k): int(v) for k, v in id_map["teams"].items()}

    new_weeks = build_real_season_weeks(team_map)
    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    league = catalog["countries"][0]["league"]
    existing_weeks = league.get("seasonWeeks", [])

    if existing_weeks == new_weeks and league.get("fixtures") == new_weeks[0]["fixtures"]:
        print("No fixture changes detected; skipping manifest bump.")
        return

    league["seasonWeeks"] = new_weeks
    league["fixtures"] = new_weeks[0]["fixtures"]
    league["weekLabel"] = new_weeks[0]["weekLabel"]

    CATALOG_PATH.write_text(
        json.dumps(catalog, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest["catalogVersion"] = now_utc.strftime("%Y.%m.%d.%H%M")
    manifest["updatedAt"] = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    MANIFEST_PATH.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Updated fixtures & bumped catalogVersion -> {manifest['catalogVersion']}")


if __name__ == "__main__":
    main()
