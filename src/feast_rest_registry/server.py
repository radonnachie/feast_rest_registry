import logging
import traceback
import argparse

from fastapi import FastAPI, HTTPException, Response, status

from feast_rest_registry import interface
from feast.errors import FeastObjectNotFoundException

import uvicorn


logger = logging.getLogger("feast_rest_registry")


def get_app(
        engine_path: str,
        project_name: str = "feast_servedregistry"
):
    app = FastAPI()
    registry = interface.ServedSqlRegistry(
        project=project_name,
        engine_path=engine_path,
    )

    @app.get("/health")
    def health():
        return Response(status_code=status.HTTP_200_OK)

    @app.get("/projects")
    def list_projects() -> interface.ReturnObjectList:
        return registry._list_served_projects()

    @app.delete("/teardown")
    def delete_registry():
        try:
            registry.teardown()
        except BaseException as err:
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err))
        except BaseException as err:
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err))
        except BaseException as err:
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err))
        except BaseException as err:
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err))
        except BaseException as err:
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err))
        except BaseException as err:
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
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
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err))
        except BaseException as err:
            logger.error(traceback.format_exc())
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"{err}\n{traceback.format_exc()}"
            )

    return app, registry


def cli_start_server():
    parser = argparse.ArgumentParser(
        description="Start the REST server for a FEAST SQL Registry.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "engine_path",
        type=str,
        help="The SQL alchemy engine path to the SQL database that will host the registry.",
    )
    parser.add_argument(
        "--host",
        type=str,
        default='127.0.0.1',
        help="The host IP to serve on.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="The port to serve on.",
    )
    parser.add_argument(
        "-l", "--log-path",
        type=str,
        default="std.out",
        help="The filepath to write logs.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Increase the verbosity of the logs (0=Error, 1=Warn, 2=Info, 3=Debug)."
    )

    args = parser.parse_args()
    if args.verbose > 3:
        args.verbose = 3

    logger_level = [
        logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG
    ][args.verbose]

    app, registry = get_app(args.engine_path)

    logger_handler_dict = {
        "level": logger_level,
        "formatter": "standard",
        "class": "logging.StreamHandler",
        "stream": "ext://sys.stdout",
    }
    if args.log_path != "std.out":
        logger_handler_dict = {
            "level": logger_level,
            "class": "logging.FileHandler",
            "formatter": "standard",
            "filename": args.log_path
        }

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_config={
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                },
            },
            "handlers": {
                "default": logger_handler_dict
            },
            "loggers": {
                "feast_rest_registry": {
                    "handlers": ["default"],
                    "level": logger_level,
                    "propagate": False
                },
                "uvicorn": {
                    "handlers": ["default"],
                    "level": logger_level,
                    "propagate": False
                },
            }
        },
        log_level=logger_level
    )
