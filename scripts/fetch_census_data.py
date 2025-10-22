from enum import Enum
from functools import reduce
from http.client import HTTPSConnection
from itertools import groupby
from json import dumps, loads
from os import remove
from sqlite3 import Connection, ProgrammingError, connect
from sys import exit
from typing import Any, Callable, Dict, Iterator, List

import fiona  # type: ignore

with open("census-api.key") as keyfile:
    API_KEY = keyfile.read().strip()
URL_ROOT = "api.census.gov"
SHAPEFILE_URL = "zip+https://www2.census.gov/geo/tiger/GENZ2024/shp/"
SCHEMA_PATH = "schema.sql"
DB_PATH = "lib/farmfind.db"
MEDIAN_INCOME = "B19326_001E"


class Dataset(Enum):
    ACS = "acs/acs1"
    CHARAGEGROUPS = "pep/charagegroups"
    GEOINFO = "geoinfo"

    def __str__(self):
        return str(self.value)


def mk_query(year: int, dataset: Dataset, **kwargs: Any) -> str:
    """Assemble a query for API consumption.

    >>> mk_query(year=2019,dataset=Dataset.CHARAGEGROUPS,get="NAME,POP",HISP=2,_for="state:*")
    '/data/2019/pep/charagegroups?get=NAME,POP&HISP=2&for=state:*'
    """

    return (
        f"/data/{year}/{dataset}"
        f"?{'&'.join(f'{k.strip("_")}={str(v)}' for k, v in kwargs.items())}"
        f"&key={API_KEY}"
    )


def get(query: str) -> Any | None:
    conn = HTTPSConnection(URL_ROOT)
    conn.request("GET", query)
    resp = conn.getresponse()
    match resp.status:
        case 200:
            return loads(resp.read())
    raise RuntimeError(f"GET {query} returned {resp.status}: {resp.reason}")


def expand(data) -> Iterator[Dict[str, Any]]:
    header = None
    for item in data:
        if header is None:
            header = item
        else:
            yield dict(zip(header, item))


def join_dicts(*dicts: Dict[str, Any], by: Callable[[Dict[str, Any]], Any]) -> List[Dict]:
    return [
        reduce(lambda x, y: x | y, grp) for _, grp in groupby(sorted(dicts, key=by), key=by)
    ]


def from_shapefile(url: str, **prop_aliases: str) -> List[Dict[str, Any]]:
    with fiona.open(url) as recs:
        return [
            {
                **{
                    alias: dict(rec.properties)[prop]
                    for prop, alias in prop_aliases.items()
                },
                "geometry": dumps(dict(rec.geometry)),
            }
            for rec in recs
        ]


def insertmany(conn: Connection, table: str, *records: Dict[str, Any]) -> None:
    for rec in records:
        try:
            insert(conn, table, **rec)
        except ProgrammingError:
            print(f"skipped {rec}")


def insert(conn: Connection, table: str, **kv: Any) -> None:
    conn.execute(f"insert into {', '.join(kv)} values ({', '.join(f':{k}' for k in kv)})")


if __name__ == "__main__":
    try:
        remove(DB_PATH)
    except FileNotFoundError:
        pass
    with open(SCHEMA_PATH) as schema, connect(DB_PATH) as conn:
        for stmt in schema.read().split(";"):
            conn.execute(stmt)
        insertmany(
            conn,
            "state",
            *join_dicts(
                *expand(
                    get(mk_query(2023, Dataset.GEOINFO, get="NAME", _for="state:*"))
                ),
                *expand(
                    get(mk_query(2024, Dataset.ACS, get=MEDIAN_INCOME, _for="state:*"))
                ),
                *from_shapefile(
                    SHAPEFILE_URL + "cb_2024_us_state_5m.zip", STATEFP="state"
                ),
                by=lambda rec: int(rec["state"]),
            ),
        )
        insertmany(
            conn,
            "county",
            *join_dicts(
                *expand(
                    get(mk_query(2023, Dataset.GEOINFO, get="NAME", _for="county:*"))
                ),
                *expand(
                    get(mk_query(2024, Dataset.ACS, get=MEDIAN_INCOME, _for="county:*"))
                ),
                *from_shapefile(
                    SHAPEFILE_URL + "cb_2024_us_county_20m.zip",
                    COUNTYFP="county",
                    STATEFP="state",
                ),
                by=lambda rec: (int(rec["county"]), int(rec["state"])),
            ),
        )
        conn.commit()
