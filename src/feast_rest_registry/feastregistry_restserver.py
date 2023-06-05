from fastapi import FastAPI, HTTPException
import traceback

import feast_registry_interface as interface
from feast.errors import FeastObjectNotFoundException


app = FastAPI()
registry = interface.ServedSqlRegistry(
    project="feast_servedregistry",
    engine_path="postgresql+psycopg2://feast:feast@db:5432/feast",
    # registry_config
    # repo_path
)


@app.get("/projects")
def list_projects() -> interface.ReturnObjectList:
    return registry._list_served_projects()


@app.delete("/teardown")
def delete_registry():
    try:
        registry.teardown()
    except BaseException as err:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )


@app.post("/{project}")
def apply_resource(
    project: str,
    resource: interface.PostableResourceType,
    name: str,
    obj_application: interface.ApplicationObject
):
    try:
        return registry._apply_served_object(
            project=project,
            resource=resource,
            name=name,
            obj=obj_application
        )
    except BaseException as err:
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )


@app.delete("/{project}")
def delete_entity(
    project: str,
    resource: interface.DeletableResourceType,
    name: str
) -> interface.ReturnDeletionCount:
    try:
        registry._delete_served_object(
            resource=resource,
            project=project,
            name=name,
        )
    except FeastObjectNotFoundException as err:
        raise HTTPException(status_code=404, detail=str(err))
    except BaseException as err:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )


@app.get("/{project}")
def get_resource(
    project: str,
    resource: interface.GettableResourceType,
    name: str,
) -> interface.ReturnObject:
    try:
        return registry._get_served_object(
            resource=resource,
            project=project,
            name=name
        )
    except FeastObjectNotFoundException as err:
        raise HTTPException(status_code=404, detail=str(err))
    except BaseException as err:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )


@app.get("/{project}/list")
def list_resource(
    project: str,
    resource: interface.ListableResourceType,
) -> interface.ReturnObjectList:
    try:
        return registry._list_served_objects(
            project=project,
            resource=resource,
        )
    except FeastObjectNotFoundException as err:
        raise HTTPException(status_code=404, detail=str(err))
    except BaseException as err:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )


@app.get("/{project}/last_updated")
def get_last_updated(
    project: str
) -> interface.ReturnDatetime:
    return interface.ReturnDatetime(
        datetime=registry._get_last_updated_metadata(project).isoformat()
    )


@app.post("/{project}/user_metadata")
def apply_resource_user_metadata(
    project: str,
    resource: interface.FeatureViewResourceType,
    name: str,
    obj_application: interface.ApplicationObject
):
    try:
        return registry._apply_served_user_metadata(
            resource=resource,
            project=project,
            name=name,
            obj=obj_application
        )
    except FeastObjectNotFoundException as err:
        raise HTTPException(status_code=404, detail=str(err))
    except BaseException as err:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )


@app.get("/{project}/user_metadata")
def get_resource_user_metadata(
    project: str,
    resource: interface.FeatureViewResourceType,
    name: str,
) -> interface.ReturnObject:
    try:
        return registry._get_served_user_metadata(
            resource=resource,
            name=name,
            project=project
        )
    except FeastObjectNotFoundException as err:
        raise HTTPException(status_code=404, detail=str(err))
    except BaseException as err:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )


@app.get("/{project}/feast_metadata")
def list_project_metadata(
    project: str,
) -> interface.ReturnObjectList:
    try:
        return registry._list_served_project_metadata(
            project=project
        )
    except FeastObjectNotFoundException as err:
        raise HTTPException(status_code=404, detail=str(err))
    except BaseException as err:
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"{err}\n{traceback.format_exc()}"
        )
