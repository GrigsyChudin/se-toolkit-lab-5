"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

import httpx
from datetime import datetime

from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password)
    ) as client:
        response = await client.get(f"{settings.autochecker_api_url}/api/items")
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs = []
    has_more = True
    current_since = since.isoformat() if since else None

    async with httpx.AsyncClient(
        auth=(settings.autochecker_email, settings.autochecker_password)
    ) as client:
        while has_more:
            params = {"limit": 500}
            if current_since:
                params["since"] = current_since

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs", params=params
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            has_more = data.get("has_more", False)
            if has_more and logs:
                current_since = logs[-1]["submitted_at"]
            else:
                has_more = False

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


from sqlmodel import select
from app.models.item import ItemRecord


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    new_items_count = 0
    lab_map: dict[str, ItemRecord] = {}

    # Process labs
    for item in items:
        if item["type"] == "lab":
            title = item["title"]
            lab_short_id = item["lab"]
            
            stmt = select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title == title)
            existing = (await session.exec(stmt)).first()
            
            if not existing:
                new_lab = ItemRecord(type="lab", title=title)
                session.add(new_lab)
                await session.flush()  # To get the ID
                lab_map[lab_short_id] = new_lab
                new_items_count += 1
            else:
                lab_map[lab_short_id] = existing

    # Process tasks
    for item in items:
        if item["type"] == "task":
            title = item["title"]
            lab_short_id = item["lab"]
            lab_item = lab_map.get(lab_short_id)
            
            if not lab_item:
                continue
                
            stmt = select(ItemRecord).where(
                ItemRecord.type == "task", 
                ItemRecord.title == title, 
                ItemRecord.parent_id == lab_item.id
            )
            existing = (await session.exec(stmt)).first()
            
            if not existing:
                new_task = ItemRecord(type="task", title=title, parent_id=lab_item.id)
                session.add(new_task)
                new_items_count += 1

    await session.commit()
    return new_items_count


from app.models.learner import Learner
from app.models.interaction import InteractionLog


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    new_logs_count = 0
    
    # Build title lookup
    title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        title_lookup[(item["lab"], item["task"])] = item["title"]

    # Cache for learners and items to avoid too many DB calls
    learner_cache: dict[str, Learner] = {}
    item_cache: dict[str, ItemRecord] = {}

    for log in logs:
        # 1. Learner
        ext_learner_id = log["student_id"]
        if ext_learner_id not in learner_cache:
            stmt = select(Learner).where(Learner.external_id == ext_learner_id)
            learner = (await session.exec(stmt)).first()
            if not learner:
                learner = Learner(external_id=ext_learner_id, student_group=log["group"])
                session.add(learner)
                await session.flush()
            learner_cache[ext_learner_id] = learner
        learner = learner_cache[ext_learner_id]

        # 2. Item
        title = title_lookup.get((log["lab"], log["task"]))
        if not title:
            continue
            
        if title not in item_cache:
            stmt = select(ItemRecord).where(ItemRecord.title == title)
            item = (await session.exec(stmt)).first()
            if not item:
                continue
            item_cache[title] = item
        item = item_cache[title]

        # 3. & 4. InteractionLog
        ext_log_id = log["id"]
        stmt = select(InteractionLog).where(InteractionLog.external_id == ext_log_id)
        existing = (await session.exec(stmt)).first()
        
        if not existing:
            new_interaction = InteractionLog(
                external_id=ext_log_id,
                learner_id=learner.id,
                item_id=item.id,
                kind="attempt",
                score=log["score"],
                checks_passed=log["passed"],
                checks_total=log["total"],
                created_at=datetime.fromisoformat(log["submitted_at"].replace("Z", "+00:00"))
            )
            session.add(new_interaction)
            new_logs_count += 1

    await session.commit()
    return new_logs_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


from sqlalchemy import func


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    # Step 1: Items
    raw_items = await fetch_items()
    await load_items(raw_items, session)

    # Step 2: Last timestamp
    stmt = select(func.max(InteractionLog.created_at))
    last_timestamp = (await session.exec(stmt)).first()

    # Step 3: Logs
    new_logs = await fetch_logs(since=last_timestamp)
    new_records_count = await load_logs(new_logs, raw_items, session)

    # Total records
    total_stmt = select(func.count()).select_from(InteractionLog)
    total_records = (await session.exec(total_stmt)).one()

    return {
        "new_records": new_records_count,
        "total_records": total_records
    }
