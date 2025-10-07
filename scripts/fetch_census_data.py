from enum import Enum
from http.client import HTTPSConnection
from json import loads
from os import remove
from sqlite3 import connect
from sys import exit
from typing import Any

API_KEY = 'de13c14fa8937c82618fa846f90fd7cc8df4b02d'
URL_ROOT = "api.census.gov"
DB_PATH = "lib/farmfind.db"
MEDIAN_INCOME = "B19326_001E"

SCHEMA = [
    """\
    create table states (
        state_id int,
        name text
    );
    """,
    """\
    create table counties (
        county_id int,
        state_id int,
        name text
    );
    """,
    """\
    create table median_income_state (
        state_id int,
        income int
    );
    """,
    """\
    create table median_income_county (
        state_id int,
        county_id int,
        income int
    );
    """,
]


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


def expand(data):
    header = None
    for item in data:
        if header is None:
            header = item
        else:
            yield dict(zip(header, item))


if __name__ == "__main__":
    try:
        remove(DB_PATH)
    except ValueError:
        pass
    with connect(DB_PATH) as conn:
        for stmt in SCHEMA:
            conn.execute(stmt)
        conn.executemany(
            "insert into states (name, state_id) values (:NAME, :state)",
            expand(get(mk_query(2023, Dataset.GEOINFO, get="NAME", _for="state:*")))
        )
        conn.executemany(
            "insert into counties (name, state_id, county_id) values (:NAME, :state, :county)",
            expand(get(mk_query(2023, Dataset.GEOINFO, get="NAME", _for="county:*")))
        )
        conn.executemany(
            f"insert into median_income_state (state_id, income) values (:state, :{MEDIAN_INCOME})",
            expand(get(mk_query(2024, Dataset.ACS, get=MEDIAN_INCOME, _for="state:*"))),
        )
        conn.executemany(
            f"insert into median_income_county (state_id, county_id, income) values (:state, :county, :{MEDIAN_INCOME})",
            expand(get(mk_query(2024, Dataset.ACS, get=MEDIAN_INCOME, _for="county:*"))),
        )
        conn.commit()
