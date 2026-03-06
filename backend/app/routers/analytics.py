"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, cast, Date
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


async def get_lab_and_tasks(lab_id_str: str, session: AsyncSession):
    # lab_id_str is like "lab-04", title in DB is like "Lab 04 — Testing"
    # Transform "lab-04" to "Lab 04"
    search_term = lab_id_str.replace("-", " ").title()
    
    # Find lab item
    stmt = select(ItemRecord).where(ItemRecord.type == "lab", ItemRecord.title.contains(search_term))
    lab_item = (await session.exec(stmt)).first()
    if not lab_item:
        return None, []
        
    # Find tasks
    stmt = select(ItemRecord).where(ItemRecord.type == "task", ItemRecord.parent_id == lab_item.id)
    tasks = (await session.exec(stmt)).all()
    return lab_item, tasks


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    lab_item, tasks = await get_lab_and_tasks(lab, session)
    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]
        
    task_ids = [t.id for t in tasks]
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    stmt = select(
        case(
            (InteractionLog.score <= 25, "0-25"),
            (InteractionLog.score <= 50, "26-50"),
            (InteractionLog.score <= 75, "51-75"),
            else_="76-100",
        ).label("bucket"),
        func.count().label("count"),
    ).where(InteractionLog.item_id.in_(task_ids), InteractionLog.score.is_not(None)).group_by("bucket")
    
    results = (await session.exec(stmt)).all()
    
    counts = {r[0]: r[1] for r in results}
    return [
        {"bucket": "0-25", "count": counts.get("0-25", 0)},
        {"bucket": "26-50", "count": counts.get("26-50", 0)},
        {"bucket": "51-75", "count": counts.get("51-75", 0)},
        {"bucket": "76-100", "count": counts.get("76-100", 0)},
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    lab_item, tasks = await get_lab_and_tasks(lab, session)
    if not lab_item:
        return []
        
    task_ids = [t.id for t in tasks]
    if not task_ids:
        return []

    stmt = select(
        ItemRecord.title.label("task"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(InteractionLog.id).label("attempts")
    ).join(InteractionLog, InteractionLog.item_id == ItemRecord.id).where(ItemRecord.id.in_(task_ids)).group_by(ItemRecord.id).order_by(ItemRecord.title)
    
    results = (await session.exec(stmt)).all()
    return [{"task": r[0], "avg_score": float(r[1]) if r[1] is not None else 0.0, "attempts": r[2]} for r in results]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    lab_item, tasks = await get_lab_and_tasks(lab, session)
    if not lab_item:
        return []
        
    task_ids = [t.id for t in tasks]
    if not task_ids:
        return []

    stmt = select(
        func.date(InteractionLog.created_at).label("date"),
        func.count(InteractionLog.id).label("submissions")
    ).where(InteractionLog.item_id.in_(task_ids)).group_by("date").order_by("date")
    
    results = (await session.exec(stmt)).all()
    formatted_results = []
    for r in results:
        d = r[0]
        if isinstance(d, str):
            formatted_results.append({"date": d, "submissions": r[1]})
        else:
            formatted_results.append({"date": d.isoformat(), "submissions": r[1]})
    return formatted_results


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    lab_item, tasks = await get_lab_and_tasks(lab, session)
    if not lab_item:
        return []
        
    task_ids = [t.id for t in tasks]
    if not task_ids:
        return []

    stmt = select(
        Learner.student_group.label("group"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(InteractionLog, InteractionLog.learner_id == Learner.id).where(InteractionLog.item_id.in_(task_ids)).group_by(Learner.student_group).order_by(Learner.student_group)
    
    results = (await session.exec(stmt)).all()
    return [{"group": r[0], "avg_score": float(r[1]) if r[1] is not None else 0.0, "students": r[2]} for r in results]
