from typing import Optional
from pydantic import BaseModel
from uuid import UUID


class Token(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class TokenPayload(BaseModel):
    sub: Optional[str] = None
    type: Optional[str] = None


class TokenData(BaseModel):
    user_id: UUID 


class TokenRefresh(BaseModel):
    refresh_token: str 