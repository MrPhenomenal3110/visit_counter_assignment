from fastapi import APIRouter, HTTPException
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount

router = APIRouter()

# Reuse a single instance (don't recreate every request)
visit_counter_service = VisitCounterService()

@router.post("/visit/{page_id}")
async def record_visit(page_id: str):
    try:
        await visit_counter_service.increment_visit(page_id)
        return {"status": "success", "message": f"Visit recorded for page {page_id}"}
    except Exception as e:
        print("Error: ", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}", response_model=VisitCount)
async def get_visits(page_id: str):
    try:
        count, served_via = await visit_counter_service.get_visit_count(page_id)
        return VisitCount(visits=count, served_via=served_via)
    except Exception as e:
        print("Error: ", str(e))
        raise HTTPException(status_code=500, detail=str(e))
