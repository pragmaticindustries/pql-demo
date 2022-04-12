import random
import uuid
from datetime import datetime, timedelta
from random import uniform
from typing import Iterable

from main import get_all_assets
from pql import EntityContext, agg_functions, RootContext, Query, Projection, Aggregation, SubQuery

random.seed(123)


def create_cycles(n):
    cycles = []
    timestamp = datetime.now()
    for i in range(0, n):
        cycle_duration = timedelta(seconds=uniform(20.0, 30.0))
        pause = timedelta(seconds=uniform(1.0, 60.0))
        cycles.append({"id": i, "start": timestamp, "end": timestamp + cycle_duration, "machine": "LHL 01"})

        timestamp = timestamp + cycle_duration + pause

    return cycles


def create_tools(n):
    tools = []
    timestamp = datetime.now()
    for i in range(0, n):
        duration = timedelta(minutes=uniform(20.0, 40.0))
        pause = timedelta(minutes=uniform(5.0, 15.0))
        tools.append({"id": uuid.uuid4(), "name": f"Tool {i}", "start": timestamp, "end": timestamp + duration,
                      "machine": "LHL 01"})

        timestamp = timestamp + duration + pause

    return tools


def create_materials(n):
    materials = []
    timestamp = datetime.now()
    for i in range(0, n):
        duration = timedelta(minutes=uniform(10.0, 20.0))
        pause = timedelta(minutes=uniform(0.0, 5.0))
        materials.append(
            {"id": uuid.uuid4(), "material": f"Material {i % 2}", "start": timestamp, "end": timestamp + duration,
             "machine": "LHL 01"})

        timestamp = timestamp + duration + pause

    return materials


generated_tools = create_tools(5)
generated_cycles = create_cycles(100)
generated_materials = create_materials(10)


def get_all_assets(name: str) -> Iterable[dict]:
    if name == "Tools":
        return generated_tools
    elif name == "Cycles":
        return generated_cycles
    elif name == "Materials":
        return generated_materials
    else:
        raise Exception("")


class InMemoryAssetRetriever:

    def get_assets(self, asset_type, where_clause, group_by_clause):
        assets = get_all_assets(asset_type)
        if where_clause:
            assets = [o for o in assets if where_clause(o)]
        if group_by_clause:
            def get_group_for_object(o):
                return tuple([p.execute(EntityContext(None, o)) for p in group_by_clause])

            groups = list({get_group_for_object(o) for o in assets})

            # Now return the assets in chunks
            assets = [[o for o in assets if get_group_for_object(o) == g] for g in groups]
        return assets


asset_retriever = InMemoryAssetRetriever()
context = RootContext(asset_retriever.get_assets, lambda s: agg_functions.get(s))


def test_query():
    query = Query([
        Projection("name"),
        Aggregation("count", Query([Projection("*")], "Cycles"), name="cycles"),
        Aggregation("flatten", Query([Projection("material")], "Materials"), name="products"),
        SubQuery(
            Query([
                Projection("material"), Aggregation("count", Query([Projection("*")], "Cycles"), name="cycles")
            ], "Materials"), name="material_and_count")
    ], "Tools")

    results = query.execute(context)

    assert results[0] == {'cycles': 24, 'name': 'Tool 0', 'material_and_count': [
        {'cycles': 21, 'material': 'Material 0'},
        {'cycles': 19, 'material': 'Material 1'}],
                          'products': ['Material 0', 'Material 1']}
