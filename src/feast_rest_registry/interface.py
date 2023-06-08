import logging
import base64
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Optional, Union, Set
import uuid

from pydantic import BaseModel
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.engine import Engine

from feast import usage
import feast.infra.registry.sql as feast_sql_registry
# from feast.infra.registry.base_registry import BaseRegistry
from abc import ABC

from feast.errors import (
    DataSourceObjectNotFoundException,
    EntityNotFoundException,
    FeatureServiceNotFoundException,
    FeatureViewNotFoundException,
    SavedDatasetNotFound,
    ValidationReferenceNotFound,
)

from feast.project_metadata import ProjectMetadata
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.FeatureService_pb2 import (
    FeatureService as FeatureServiceProto,
)
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.InfraObject_pb2 import Infra as InfraProto
from feast.protos.feast.core.OnDemandFeatureView_pb2 import (
    OnDemandFeatureView as OnDemandFeatureViewProto,
)
from feast.protos.feast.core.RequestFeatureView_pb2 import (
    RequestFeatureView as RequestFeatureViewProto,
)
from feast.protos.feast.core.SavedDataset_pb2 import SavedDataset as SavedDatasetProto
from feast.protos.feast.core.StreamFeatureView_pb2 import (
    StreamFeatureView as StreamFeatureViewProto,
)
from feast.protos.feast.core.ValidationProfile_pb2 import (
    ValidationReference as ValidationReferenceProto,
)
from feast.repo_config import RegistryConfig


logger = logging.getLogger("feast_rest_registry")


class PostableResourceType(str, Enum):
    entity = "entity"
    data_source = "data_source"
    feature_view = "feature_view"
    stream_feature_view = "stream_feature_view"
    on_demand_feature_view = "on_demand_feature_view"
    request_feature_view = "request_feature_view"
    feature_service = "feature_service"
    saved_dataset = "saved_dataset"
    validation_reference = "validation_reference"
    managed_infra = "managed_infra"


class DeletableResourceType(str, Enum):
    entity = "entity"
    data_source = "data_source"
    feature_service = "feature_service"
    feature_view = "feature_view"
    saved_dataset = "saved_dataset"
    validation_reference = "validation_reference"


class GettableResourceType(str, Enum):
    entity = "entity"
    data_source = "data_source"
    feature_service = "feature_service"
    stream_feature_view = "stream_feature_view"
    on_demand_feature_view = "on_demand_feature_view"
    feature_view = "feature_view"
    request_feature_view = "request_feature_view"
    saved_dataset = "saved_dataset"
    validation_reference = "validation_reference"
    managed_infra = "managed_infra"


class QueryableResourceType(str, Enum):
    entity = "entity"
    data_source = "data_source"
    feature_view = "feature_view"
    stream_feature_view = "stream_feature_view"
    on_demand_feature_view = "on_demand_feature_view"
    request_feature_view = "request_feature_view"
    feature_service = "feature_service"
    saved_dataset = "saved_dataset"
    validation_reference = "validation_reference"


class FeatureViewResourceType(str, Enum):
    feature_view = "feature_view"
    stream_feature_view = "stream_feature_view"
    on_demand_feature_view = "on_demand_feature_view"
    request_feature_view = "request_feature_view"


def _infer_resource_table(resource: str):
    if resource == "entity":
        return feast_sql_registry.entities
    if resource == "data_source":
        return feast_sql_registry.data_sources
    if resource == "feature_view":
        return feast_sql_registry.feature_views
    if resource == "request_feature_view":
        return feast_sql_registry.request_feature_views
    if resource == "stream_feature_view":
        return feast_sql_registry.stream_feature_views
    if resource == "on_demand_feature_view":
        return feast_sql_registry.on_demand_feature_views
    if resource == "feature_service":
        return feast_sql_registry.feature_services
    if resource == "saved_dataset":
        return feast_sql_registry.saved_datasets
    if resource == "validation_reference":
        return feast_sql_registry.validation_references
    if resource == "managed_infra":
        return feast_sql_registry.managed_infra
    if resource == "feast_metadata":
        return feast_sql_registry.feast_metadata
    raise ValueError(f"No known table for resource '{resource}'.")


def _infer_resource_fields(resource: str):
    if resource == "managed_infra":
        return "infra_name", "infra_proto"
    if resource == "request_feature_view":
        return "feature_view_name", "feature_view_proto"
    if resource == "stream_feature_view":
        return "feature_view_name", "feature_view_proto"
    if resource == "on_demand_feature_view":
        return "feature_view_name", "feature_view_proto"
    return f"{resource}_name", f"{resource}_proto"


def _infer_resource_not_found_exception(resource: str):
    if resource == "entity":
        return EntityNotFoundException
    if resource == "data_source":
        return DataSourceObjectNotFoundException
    if resource == "feature_view":
        return FeatureViewNotFoundException
    if resource == "request_feature_view":
        return FeatureViewNotFoundException
    if resource == "stream_feature_view":
        return FeatureViewNotFoundException
    if resource == "on_demand_feature_view":
        return FeatureViewNotFoundException
    if resource == "feature_service":
        return FeatureServiceNotFoundException
    if resource == "saved_dataset":
        return SavedDatasetNotFound
    if resource == "validation_reference":
        return ValidationReferenceNotFound
    if resource == "managed_infra":
        return RuntimeError

    raise ValueError(f"No known not-found excption for resource '{resource}'.")


def _infer_resource_proto_class(resource):
    if resource == "entity":
        return EntityProto
    if resource == "data_source":
        return DataSourceProto
    if resource == "feature_view":
        return FeatureViewProto
    if resource == "request_feature_view":
        return RequestFeatureViewProto
    if resource == "stream_feature_view":
        return StreamFeatureViewProto
    if resource == "on_demand_feature_view":
        return OnDemandFeatureViewProto
    if resource == "feature_service":
        return FeatureServiceProto
    if resource == "saved_dataset":
        return SavedDatasetProto
    if resource == "validation_reference":
        return ValidationReferenceProto
    if resource == "managed_infra":
        return InfraProto
    raise ValueError(f"No known Proto for resource '{resource}'.")


class ApplicationObject(BaseModel):
    proto: str
    last_updated_timestamp: Union[str, datetime]


class ReturnDeletionCount(BaseModel):
    count: int


class ReturnObject(BaseModel):
    protostring: str


class ReturnObjectList(BaseModel):
    protostrings: List[str]


class ReturnStringList(BaseModel):
    strings: List[str]


class ReturnResource(BaseModel):
    name: str
    type: str
    project: str


class ReturnResourceList(BaseModel):
    resources: List[ReturnResource]


class ReturnDatetime(BaseModel):
    datetime: Union[str, datetime]


class ServedSqlRegistry(ABC):
    def __init__(
        self,
        project: str,
        engine_path: Optional[str] = None,
        registry_config: Optional[
            Union[RegistryConfig, feast_sql_registry.SqlRegistryConfig]
        ] = None,
        repo_path: Optional[Path] = None,
    ):
        if registry_config is not None:
            engine_path = registry_config.path

        assert engine_path, "No SQLAlchemy engine path provided."

        self.engine: Engine = create_engine(engine_path, echo=False)
        feast_sql_registry.metadata.create_all(self.engine)

        self.project = project

    def teardown(self):
        for t in {
            feast_sql_registry.entities,
            feast_sql_registry.data_sources,
            feast_sql_registry.feature_views,
            feast_sql_registry.feature_services,
            feast_sql_registry.on_demand_feature_views,
            feast_sql_registry.request_feature_views,
            feast_sql_registry.saved_datasets,
            feast_sql_registry.validation_references,
        }:
            with self.engine.connect() as conn:
                stmt = delete(t)
                conn.execute(stmt)

    def _apply_served_object(
        self,
        resource: PostableResourceType,
        project: str,
        name: str,
        obj: ApplicationObject,
    ):
        table = _infer_resource_table(resource.value)
        id_field_name, proto_field_name = _infer_resource_fields(resource.value)
        self._maybe_init_project_metadata(project)

        assert name, f"name needs to be provided for {obj}"

        with self.engine.connect() as conn:
            update_datetime = datetime.fromisoformat(obj.last_updated_timestamp)
            update_time = int(update_datetime.timestamp())
            stmt = select(table).where(
                getattr(table.c, id_field_name) == name, table.c.project_id == project
            )
            row = conn.execute(stmt).first()

            obj_proto_bytes = base64.b64decode(obj.proto.encode("ascii"))

            if row:
                values = {
                    proto_field_name: obj_proto_bytes,
                    "last_updated_timestamp": update_time,
                }
                update_stmt = (
                    update(table)
                    .where(getattr(table.c, id_field_name) == name)
                    .values(
                        values,
                    )
                )
                conn.execute(update_stmt)
            else:
                proto_class = _infer_resource_proto_class(resource.value)
                obj_proto = proto_class.FromString(obj_proto_bytes)

                if hasattr(obj_proto, "meta") and hasattr(
                    obj_proto.meta, "created_timestamp"
                ):
                    obj_proto.meta.created_timestamp.FromDatetime(update_datetime)

                values = {
                    id_field_name: name,
                    proto_field_name: obj_proto.SerializeToString(),
                    "last_updated_timestamp": update_time,
                    "project_id": project,
                }
                insert_stmt = insert(table).values(
                    values,
                )
                conn.execute(insert_stmt)

            self._set_last_updated_metadata(update_datetime, project)

    def _delete_served_object(
        self, resource: DeletableResourceType, project: str, name: str
    ) -> ReturnDeletionCount:
        table = _infer_resource_table(resource.value)
        id_field_name, proto_field_name = _infer_resource_fields(resource.value)
        not_found_exception = _infer_resource_not_found_exception(resource.value)

        with self.engine.connect() as conn:
            stmt = delete(table).where(
                getattr(table.c, id_field_name) == name, table.c.project_id == project
            )
            rows = conn.execute(stmt)
            if rows.rowcount < 1:
                raise not_found_exception(name, project)
            self._set_last_updated_metadata(datetime.utcnow(), project)

            return ReturnDeletionCount(count=rows.rowcount)

    def _get_served_object(
        self, resource: GettableResourceType, project: str, name: str
    ) -> ReturnObject:
        table = _infer_resource_table(resource.value)
        id_field_name, proto_field_name = _infer_resource_fields(resource.value)
        not_found_exception = _infer_resource_not_found_exception(resource.value)

        self._maybe_init_project_metadata(project)

        with self.engine.connect() as conn:
            stmt = select(table).where(
                getattr(table.c, id_field_name) == name, table.c.project_id == project
            )
            row = conn.execute(stmt).first()
            if row:
                return ReturnObject(
                    protostring=base64.b64encode(row[proto_field_name]).decode("ascii")
                )
        raise not_found_exception(name, project)

    def _list_served_objects(
        self, resource: QueryableResourceType, project: str
    ) -> ReturnObjectList:
        table = _infer_resource_table(resource.value)
        id_field_name, proto_field_name = _infer_resource_fields(resource.value)

        self._maybe_init_project_metadata(project)
        with self.engine.connect() as conn:
            stmt = select(table).where(table.c.project_id == project)
            rows = conn.execute(stmt).all()
            protostrings = []
            if rows:
                protostrings = [
                    base64.b64encode(row[proto_field_name]).decode("ascii")
                    for row in rows
                ]
        return ReturnObjectList(protostrings=protostrings)

    def _apply_served_user_metadata(
        self,
        resource: FeatureViewResourceType,
        project: str,
        name: str,
        obj: ApplicationObject,
    ):
        table = _infer_resource_table(resource.value)

        with self.engine.connect() as conn:
            stmt = select(table).where(
                getattr(table.c, "feature_view_name") == name,
                table.c.project_id == project,
            )
            row = conn.execute(stmt).first()
            update_datetime = datetime.fromisoformat(obj.last_updated_timestamp)
            update_time = int(update_datetime.timestamp())
            if row:
                values = {
                    "user_metadata": base64.b64decode(obj.proto.encode("ascii")),
                    "last_updated_timestamp": update_time,
                }
                update_stmt = (
                    update(table)
                    .where(
                        getattr(table.c, "feature_view_name") == name,
                        table.c.project_id == project,
                    )
                    .values(
                        values,
                    )
                )
                conn.execute(update_stmt)
            else:
                raise FeatureViewNotFoundException(name, project=project)

    def _get_served_user_metadata(
        self, resource: FeatureViewResourceType, project: str, name: str
    ) -> ReturnObject:
        table = _infer_resource_table(resource.value)

        with self.engine.connect() as conn:
            stmt = select(table).where(getattr(table.c, "feature_view_name") == name)
            row = conn.execute(stmt).first()
            if row:
                return ReturnObject(
                    protostring=base64.b64encode(row["user_metadata"]).decode("ascii")
                )
            else:
                raise FeatureViewNotFoundException(name, project=project)

    def _list_served_project_metadata(
        self,
        project: str,
    ) -> ReturnObjectList:
        return ReturnObjectList(
            protostrings=[
                base64.b64encode(proj_metadata.to_proto().SerializeToString()).decode(
                    "ascii"
                )
                for proj_metadata in self.list_project_metadata(project)
            ]
        )

    def _list_served_projects(self) -> ReturnStringList:
        return ReturnStringList(
            strings=self._get_all_projects()
        )

    def _list_served_resources(
        self,
        resource: Optional[QueryableResourceType] = None,
        name: Optional[str] = None
    ) -> ReturnResourceList:
        resource_types = QueryableResourceType
        if resource is not None:
            resource_types = [resource]
        logger.debug(f"Querying resource_types: {[r.value for r in resource_types]}.")

        resources = []
        with self.engine.connect() as conn:
            for resource_type in resource_types:
                table = _infer_resource_table(resource_type.value)
                id_field_name, _ = _infer_resource_fields(resource_type.value)

                stmt = select(table)
                if name is not None:
                    stmt = stmt.where(
                        getattr(table.c, id_field_name).like(f"%{name}%")
                    )

                resources += [
                    ReturnResource(
                        name=row[id_field_name],
                        type=resource_type,
                        project=row["project_id"]
                    )
                    for row in conn.execute(stmt).all()
                ]

        return ReturnResourceList(
            resources=resources
        )

    def _get_all_projects(self) -> Set[str]:
        projects = set()
        with self.engine.connect() as conn:
            for table in {
                feast_sql_registry.entities,
                feast_sql_registry.data_sources,
                feast_sql_registry.feature_views,
                feast_sql_registry.request_feature_views,
                feast_sql_registry.on_demand_feature_views,
                feast_sql_registry.stream_feature_views,
            }:
                stmt = select(table)
                rows = conn.execute(stmt).all()
                for row in rows:
                    projects.add(row["project_id"])

        return projects

    def list_project_metadata(self, project: str) -> List[ProjectMetadata]:
        with self.engine.connect() as conn:
            stmt = select(feast_sql_registry.feast_metadata).where(
                feast_sql_registry.feast_metadata.c.project_id == project,
            )
            rows = conn.execute(stmt).all()
            if rows:
                project_metadata = ProjectMetadata(project_name=project)
                for row in rows:
                    if (
                        row["metadata_key"]
                        == feast_sql_registry.FeastMetadataKeys.PROJECT_UUID.value
                    ):
                        project_metadata.project_uuid = row["metadata_value"]
                        break
                    # TODO(adchia): Add other project metadata in a structured way
                return [project_metadata]
        return []

    def _set_last_updated_metadata(self, last_updated: datetime, project: str):
        with self.engine.connect() as conn:
            stmt = select(feast_sql_registry.feast_metadata).where(
                feast_sql_registry.feast_metadata.c.metadata_key
                == feast_sql_registry.FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                feast_sql_registry.feast_metadata.c.project_id == project,
            )
            row = conn.execute(stmt).first()

            update_time = int(last_updated.timestamp())

            values = {
                "metadata_key": feast_sql_registry.FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                "metadata_value": f"{update_time}",
                "last_updated_timestamp": update_time,
                "project_id": project,
            }
            if row:
                update_stmt = (
                    update(feast_sql_registry.feast_metadata)
                    .where(
                        feast_sql_registry.feast_metadata.c.metadata_key
                        == feast_sql_registry.FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                        feast_sql_registry.feast_metadata.c.project_id == project,
                    )
                    .values(values)
                )
                conn.execute(update_stmt)
            else:
                insert_stmt = insert(feast_sql_registry.feast_metadata).values(
                    values,
                )
                conn.execute(insert_stmt)

    def _get_last_updated_metadata(self, project: str):
        with self.engine.connect() as conn:
            stmt = select(feast_sql_registry.feast_metadata).where(
                feast_sql_registry.feast_metadata.c.metadata_key
                == feast_sql_registry.FeastMetadataKeys.LAST_UPDATED_TIMESTAMP.value,
                feast_sql_registry.feast_metadata.c.project_id == project,
            )
            row = conn.execute(stmt).first()
            if not row:
                return None
            update_time = int(row["last_updated_timestamp"])

            return datetime.utcfromtimestamp(update_time)

    def _maybe_init_project_metadata(self, project):
        # Initialize project metadata if needed
        with self.engine.connect() as conn:
            update_datetime = datetime.utcnow()
            update_time = int(update_datetime.timestamp())
            stmt = select(feast_sql_registry.feast_metadata).where(
                feast_sql_registry.feast_metadata.c.metadata_key
                == feast_sql_registry.FeastMetadataKeys.PROJECT_UUID.value,
                feast_sql_registry.feast_metadata.c.project_id == project,
            )
            row = conn.execute(stmt).first()
            if row:
                usage.set_current_project_uuid(row["metadata_value"])
            else:
                new_project_uuid = f"{uuid.uuid4()}"
                values = {
                    "metadata_key": feast_sql_registry.FeastMetadataKeys.PROJECT_UUID.value,
                    "metadata_value": new_project_uuid,
                    "last_updated_timestamp": update_time,
                    "project_id": project,
                }
                insert_stmt = insert(feast_sql_registry.feast_metadata).values(values)
                conn.execute(insert_stmt)
                usage.set_current_project_uuid(new_project_uuid)
