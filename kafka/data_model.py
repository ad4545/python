from pydantic import BaseModel



class Coordinates(BaseModel):
    x:int
    y:int
    z:int


class Pose(BaseModel):
     translation:Coordinates
     rotation:Coordinates


class SubActions(BaseModel):
     type:str
     paths:list[Pose]


class Actions(BaseModel):
     task_id:int
     taskName:str
     subTasks:list[SubActions]


class VDAMessage(BaseModel):
    message_id: str
    tasks:list[Actions]
