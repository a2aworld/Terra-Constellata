from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from sqlalchemy.orm import Session
import aiofiles
import os
from ..database import get_db
from .. import crud, schemas

router = APIRouter()

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.post("/upload", response_model=schemas.Multimedia)
async def upload_file(
    file: UploadFile = File(...), content_id: int = None, db: Session = Depends(get_db)
):
    if not content_id:
        raise HTTPException(status_code=400, detail="content_id is required")

    file_path = f"{UPLOAD_DIR}/{file.filename}"

    async with aiofiles.open(file_path, "wb") as out_file:
        content = await file.read()
        await out_file.write(content)

    # Determine file type
    file_type = file.content_type.split("/")[0] if file.content_type else "unknown"

    multimedia_data = schemas.MultimediaCreate(
        filename=file.filename, file_type=file_type, content_id=content_id
    )

    return crud.create_multimedia(
        db=db, multimedia=multimedia_data, file_path=file_path
    )


@router.get("/{multimedia_id}", response_model=schemas.Multimedia)
def get_multimedia(multimedia_id: int, db: Session = Depends(get_db)):
    db_multimedia = crud.get_multimedia(db, multimedia_id=multimedia_id)
    if db_multimedia is None:
        raise HTTPException(status_code=404, detail="Multimedia not found")
    return db_multimedia
