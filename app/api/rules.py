from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Rule
from app.processing.window_state import clear_rule_state
from app.schemas import RuleCreate, RuleUpdate, validate_definition

router = APIRouter(tags=["rules"])


def rule_to_dict(rule: Rule) -> dict:
    return {
        "id": rule.id,
        "name": rule.name,
        "rule_type": rule.rule_type,
        "definition": rule.definition,
        "enabled": rule.enabled,
        "version": rule.version,
    }


@router.post("/rules", status_code=201)
async def create_rule(data: RuleCreate):

    try:
        definition = validate_definition(data.rule_type, data.definition)
    except ValueError as error:
        raise HTTPException(
            status_code=422, detail=f"Invalid rule definition: {error}"
        )

    async with SessionLocal() as db:

        rule = Rule(
            name=data.name,
            rule_type=data.rule_type,
            definition=definition,
            enabled=data.enabled,
        )

        db.add(rule)
        await db.commit()
        await db.refresh(rule)

        return rule_to_dict(rule)


@router.get("/rules")
async def list_rules():

    async with SessionLocal() as db:

        result = await db.execute(select(Rule).order_by(Rule.id))
        rules = result.scalars().all()

        return [rule_to_dict(rule) for rule in rules]


@router.get("/rules/{rule_id}")
async def get_rule(rule_id: int):

    async with SessionLocal() as db:

        rule = await db.get(Rule, rule_id)

        if not rule:
            raise HTTPException(status_code=404, detail="Rule not found")

        return rule_to_dict(rule)


@router.put("/rules/{rule_id}")
async def update_rule(rule_id: int, data: RuleUpdate):

    async with SessionLocal() as db:

        rule = await db.get(Rule, rule_id)

        if not rule:
            raise HTTPException(status_code=404, detail="Rule not found")

        rule_type = data.rule_type or rule.rule_type

        if data.definition is not None:
            try:
                rule.definition = validate_definition(rule_type, data.definition)
            except ValueError as error:
                raise HTTPException(
                    status_code=422, detail=f"Invalid rule definition: {error}"
                )
            rule.rule_type = rule_type

        if data.name is not None:
            rule.name = data.name

        if data.enabled is not None:
            rule.enabled = data.enabled

        rule.version += 1
        await db.commit()

    # Old window state belongs to the old definition: clear it so the
    # next event starts from zero. (Same process, so this is safe.)
    clear_rule_state(rule_id)

    return rule_to_dict(rule)


@router.delete("/rules/{rule_id}", status_code=204)
async def delete_rule(rule_id: int):

    async with SessionLocal() as db:

        rule = await db.get(Rule, rule_id)

        if not rule:
            raise HTTPException(status_code=404, detail="Rule not found")

        await db.delete(rule)
        await db.commit()

    # Existing alerts stay untouched: they are historical records.
    clear_rule_state(rule_id)
