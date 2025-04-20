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
    name: Optional[str]
    author: Optional[List[PersonOrOrganization]]
    contributor: Optional[List[PersonOrOrganization]]
    publisher: Optional[PersonOrOrganization]
    datePublished: Optional[str]
    isbn: Optional[str]
    inLanguage: Optional[str]
    description: Optional[str]
    numberOfPages: Optional[int]
    bookEdition: Optional[str]
    additionalProperty: Optional[List[PropertyValue]]

    # Optional enhancements
    genre: Optional[List[str]]
    audience: Optional[str]
    accessMode: Optional[str]
    accessModeSufficient: Optional[List[str]]
    suggestedAge: Optional[str]
    translator: Optional[List[PersonOrOrganization]]
    isBasedOn: Optional[CreativeWork] = None 
    
    class Config:
        populate_by_name = True