import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Path

from openaq_api.db import DB
from openaq_api.v3.models.queries import (
    BboxQuery,
    CountryIdQuery,
    CountryIsoQuery,
    MobileQuery,
    MonitorQuery,
    OwnerQuery,
    Paging,
    ProviderQuery,
    QueryBaseModel,
    QueryBuilder,
    RadiusQuery,
)
from openaq_api.v3.models.responses import LocationsResponse

logger = logging.getLogger("locations")

router = APIRouter(
    prefix="/v3",
    tags=["v3-alpha"],
    include_in_schema=True,
)


class LocationPathQuery(QueryBaseModel):
    """Path query to filter results by locations ID.

    Inherits from QueryBaseModel.

    Attributes:
        locations_id: locations ID value.
    """

    locations_id: int = Path(
        description="Limit the results to a specific location by id", ge=1
    )

    def where(self) -> str:
        """Generates SQL condition for filtering to a single locations_id

        Overrides the base QueryBaseModel `where` method

        Returns:
            string of WHERE clause
        """
        return "id = :locations_id"


class LocationsQueries(
    Paging,
    RadiusQuery,
    BboxQuery,
    ProviderQuery,
    OwnerQuery,
    CountryIdQuery,
    CountryIsoQuery,
    MobileQuery,
    MonitorQuery,
):
    ...


@router.get(
    "/locations/{locations_id}",
    response_model=LocationsResponse,
    summary="Get a location by ID",
    description="Provides a location by location ID",
)
async def location_get(
    locations: Annotated[LocationPathQuery, Depends(LocationPathQuery.depends())],
    db: DB = Depends(),
):
    response = await fetch_locations(locations, db)
    return response


@router.get(
    "/locations",
    response_model=LocationsResponse,
    summary="Get locations",
    description="Provides a list of locations",
)
async def locations_get(
    locations: Annotated[LocationsQueries, Depends(LocationsQueries.depends())],
    db: DB = Depends(),
):
    response = await fetch_locations(locations, db)
    return response


async def fetch_locations(query, db):
    query_builder = QueryBuilder(query)
    sql = f"""
    SELECT id
    , name
    , ismobile as is_mobile
    , ismonitor as is_monitor
    , city as locality
    , country
    , owner
    , provider
    , coordinates
    , instruments
    , sensors
    , timezone
    , bbox(geom) as bounds
    , datetime_first
    , datetime_last
    {query_builder.fields() or ''} 
    {query_builder.total()}
    FROM locations_view_cached
    {query_builder.where()}
    {query_builder.pagination()}
    """
    response = await db.fetchPage(sql, query_builder.params())
    return response
