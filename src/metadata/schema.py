from pydantic import BaseModel, Field
from typing import Optional, List

class PersonOrOrganization(BaseModel):
    type: str = Field(alias="@type")
    name: str
    role: Optional[str] = None

class PropertyValue(BaseModel):
    type: str = Field(alias="@type")
    name: str
    value: str
    
class CreativeWork(BaseModel):
    type: str = Field(alias="@type")
    name: Optional[str] = None
    author: Optional[List[PersonOrOrganization]] = None
    inLanguage: Optional[str] = None
    url: Optional[List[str]] = None


class Book(BaseModel):
    context: str = Field(alias="@context")
    type: str = Field(alias="@type")

    # Core metadata
    name: Optional[str] = None
    author: Optional[List[PersonOrOrganization]] = None
    contributor: Optional[List[PersonOrOrganization]] = None
    publisher: Optional[PersonOrOrganization] = None
    datePublished: Optional[str] = None
    isbn: Optional[List[str]] = None
    inLanguage: Optional[str] = None
    description: Optional[str] = None
    numberOfPages: Optional[int] = None
    bookEdition: Optional[int] = None
    additionalProperty: Optional[List[PropertyValue]] = None

    # Optional enhancements
    genre: Optional[List[str]] = None
    audience: Optional[str] = None
    accessMode: Optional[str] = None
    accessModeSufficient: Optional[List[str]] = None
    suggestedMinAge: Optional[int] = None
    isBasedOn: Optional[CreativeWork] = None 
    
    class Config:
        populate_by_name = True