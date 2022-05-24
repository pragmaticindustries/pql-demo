import random
import uuid
from datetime import datetime, timedelta
from random import uniform
from typing import Iterable, List, Dict

from pql import (
    Query,
    Projection,
    Aggregation,
    SubQuery,
    RootContext,
    agg_functions,
    EntityContext,
    EqPredicate,
    GreaterPredicate,
    GreaterEqPredicate,
    LowerPredicate,
    LowerEqPredicate,
    Predicate,
)


def create_cycles(n):
    random.seed(123)
    cycles: List[Dict] = []
    timestamp: datetime = datetime.now()
    for i in range(0, n):
        cycle_duration: timedelta = timedelta(seconds=uniform(20.0, 30.0))
        pause: timedelta = timedelta(seconds=uniform(1.0, 60.0))
        cycles.append(
            {
                "id": i,
                "start": timestamp,
                "end": timestamp + cycle_duration,
                "machine": "LHL 01",
            }
        )

        timestamp = timestamp + cycle_duration + pause

    return cycles


def create_tools(n):
    random.seed(123)
    tools: List[Dict] = []
    timestamp: datetime = datetime.now()
    for i in range(0, n):
        duration: timedelta = timedelta(minutes=uniform(20.0, 40.0))
        pause: timedelta = timedelta(minutes=uniform(5.0, 15.0))
        tools.append(
            {
                "id": uuid.uuid4(),
                "name": f"Tool {i}",
                "start": timestamp,
                "end": timestamp + duration,
                "machine": "LHL 01",
            }
        )

        timestamp = timestamp + duration + pause

    return tools


def create_materials(n):
    random.seed(123)
    materials: List[Dict] = []
    timestamp: datetime = datetime.now()
    for i in range(0, n):
        duration: timedelta = timedelta(minutes=uniform(10.0, 20.0))
        pause: timedelta = timedelta(minutes=uniform(0.0, 5.0))
        materials.append(
            {
                "id": uuid.uuid4(),
                "material": f"Material {i % 2}",
                "start": timestamp,
                "end": timestamp + duration,
                "machine": "LHL 01",
            }
        )

        timestamp = timestamp + duration + pause

    return materials


def print_index_of_array(array: []):
    element: dict
    for element in array:
        print(element)


generated_tools = create_tools(5)
generated_cycles = create_cycles(100)
generated_materials = create_materials(10)


# print_index_of_array(generated_tools)
# print_index_of_array(generated_cycles)
# print_index_of_array(generated_materials)


def get_all_assets(name: str) -> Iterable[dict]:
    if name == "Tools":
        return generated_tools
    elif name == "Cycles":
        return generated_cycles
    elif name == "Materials":
        return generated_materials
    else:
        raise Exception("")
    # Root Context is what defines the boundaries and where to read the entities from


class InMemoryAssetRetriever:
    def get_assets(self, asset_type, where_clause, group_by_clause):
        assets = get_all_assets(asset_type)
        if where_clause:
            if isinstance(where_clause, Predicate):
                assets = [element for element in assets if where_clause.check(element)]
            else:
                assets = [o for o in assets if where_clause(o)]
        if group_by_clause:

            def get_group_for_object(o):
                return tuple(
                    [p.execute(EntityContext(None, o)) for p in group_by_clause]
                )

            groups: list = list({get_group_for_object(o) for o in assets})

            # Now return the assets in chunks
            assets = [
                [o for o in assets if get_group_for_object(o) == g] for g in groups
            ]
        return assets


if __name__ == "__main__":
    # SELECT t.name, COUNT(SELECT c FROM Cycles [WHERE t.start <= c.start AND c.start < t.end]),
    # LIST(SELECT m.material FROM Materials [WHERE t.start <= m.start AND m.start < t.end])
    # FROM Tools
    asset_retriever: InMemoryAssetRetriever = InMemoryAssetRetriever()
    # context = RootContext(asset_retriever.get_assets, lambda s: agg_functions.get(s))

    context: RootContext = RootContext(
        asset_retriever.get_assets, lambda s: agg_functions.get(s)
    )
    # Concrete Example:
    # SELECT t.name, COUNT(SELECT c FROM Cycles) AS "cycles" FROM Tools
    print(context)
    query: Query = Query(
        [
            Projection("id"),
            Aggregation("count", Query([Projection("id")], "Cycles"), name="cycles"),
        ],
        "Tools",
    )
    results: List[Dict] = query.execute(context)

    # The result is a list of dicts representing a (probably nested) table

    print(results)
    # Concrete Example:
    # SELECT COUNT(*) FROM Cycles, ToolEquipped AS t WHERE t.name = "Tool 0"

    query: Query = Query(
        [
            Projection("id"),
            Aggregation("count", Query([Projection("id")], "Cycles"), name="cycles"),
        ],
        "Tools",
        EqPredicate("name", "Tool 0"),
    )
    results: List[Dict] = query.execute(context)
    print(results)

    # Concrete Example:
    # SELECT t.name, COUNT(SELECT c FROM Cycles) AS "cycles", FLATTEN(SELECT m.material FROM Materials) AS "products",
    # (SELECT m.material, COUNT(SELECT c FROM Cycles) FROM Materials) AS "material_and_count"
    # FROM Tools AS t
    query: Query = Query(
        [
            Projection("name"),
            Aggregation("count", Query([Projection("id")], "Cycles"), name="cycles"),
            Aggregation(
                "flatten", Query([Projection("material")], "Materials"), name="products"
            ),
            SubQuery(
                Query(
                    [
                        Projection("material"),
                        Aggregation(
                            "count", Query([Projection("id")], "Cycles"), name="cycles"
                        ),
                    ],
                    "Materials",
                ),
                name="material_and_count",
            ),
        ],
        "Tools",
    )

    results: List[Dict] = query.execute(context)

    # The result is a list of dicts representing a (probably nested) table
    print(results)

    # Example 2: Filter by Material
    # SELECT m.material, COUNT(SELECT c FROM Cycles) AS "cycles"
    # FROM Materials
    query: Query = Query(
        [
            Projection("material"),
            Aggregation("count", Query([Projection("id")], "Cycles"), name="cycles"),
        ],
        "Materials",
        group_by_clause=[Projection("material")],
    )

    results: List[Dict] = query.execute(context)

    # The result is a list of dicts representing a (probably nested) table
    print(results)

    query: Query = Query(
        [
            Projection("id"),
            Projection("material"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Materials",
    )
    results_all_materials: List[Dict] = query.execute(context)

    assert results_all_materials == generated_materials

    query: Query = Query(
        [
            Projection("id"),
            Projection("name"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Tools",
    )
    results_all_tools: List[Dict] = query.execute(context)

    assert results_all_tools == generated_tools

    query: Query = Query(
        [
            Projection("id"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Cycles",
    )
    results_all_cycles: List[Dict] = query.execute(context)

    assert results_all_cycles == generated_cycles

    query: Query = Query(
        [
            Projection("id"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Cycles",
        EqPredicate("id", generated_cycles[0].get("id")),
    )
    results_eq_cycles: List[Dict] = query.execute(context)

    assert results_eq_cycles[0] == generated_cycles[0]

    query: Query = Query(
        [
            Projection("id"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Cycles",
        LowerPredicate("id", 5),
    )
    results_lower_cycle: List[Dict] = query.execute(context)

    first_four = generated_cycles[0:5]
    assert results_lower_cycle == first_four

    query: Query = Query(
        [
            Projection("id"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Cycles",
        LowerEqPredicate("id", 5),
    )
    results_lower_eq_cycles: List[Dict] = query.execute(context)

    first_five = generated_cycles[0:6]
    assert results_lower_eq_cycles == first_five

    query: Query = Query(
        [
            Projection("id"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Cycles",
        GreaterPredicate("id", 5),
    )
    results_greater_cycles: List[Dict] = query.execute(context)

    first_four = generated_cycles[6:]
    assert results_greater_cycles == first_four

    query: Query = Query(
        [
            Projection("id"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Cycles",
        GreaterEqPredicate("id", 5),
    )
    results_greater_eq_cycles: List[Dict] = query.execute(context)

    first_five = generated_cycles[5:]
    assert results_greater_eq_cycles == first_five

    # Materials
    query: Query = Query(
        [
            Projection("id"),
            Projection("material"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Materials",
        EqPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_eq: List[Dict] = query.execute(context)
    assert results_eq[0] == generated_materials[int(len(generated_materials) / 2)]

    query: Query = Query(
        [
            Projection("id"),
            Projection("material"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Materials",
        LowerPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_lower_materials: List[Dict] = query.execute(context)

    last_half_materials = generated_materials[0 : int(len(generated_materials) / 2)]
    assert results_lower_materials == last_half_materials

    query: Query = Query(
        [
            Projection("id"),
            Projection("material"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Materials",
        LowerEqPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_lower_eq_materials: List[Dict] = query.execute(context)

    last_half_plus_one_materials = generated_materials[
        0 : int(len(generated_materials) / 2) + 1
    ]
    assert results_lower_eq_materials == last_half_plus_one_materials

    query: Query = Query(
        [
            Projection("id"),
            Projection("material"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Materials",
        GreaterPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_greater_materials: List[Dict] = query.execute(context)

    first_half_materials = generated_materials[int(len(generated_materials) / 2) + 1 :]
    assert results_greater_materials == first_half_materials

    query: Query = Query(
        [
            Projection("id"),
            Projection("material"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Materials",
        GreaterEqPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_greater_eq_materials: List[Dict] = query.execute(context)

    first_half_minus_one_materials = generated_materials[
        int(len(generated_materials) / 2) :
    ]
    assert results_greater_eq_materials == first_half_minus_one_materials

    # Tools
    query: Query = Query(
        [
            Projection("id"),
            Projection("name"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Tools",
        EqPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_eq_tool = query.execute(context)

    assert results_eq_tool[0] == generated_tools[int(len(generated_tools) / 2)]

    query: Query = Query(
        [
            Projection("id"),
            Projection("name"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Tools",
        LowerPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_tool_lower: List[Dict] = query.execute(context)

    first_half_tools = generated_tools[0 : int(len(generated_tools) / 2)]
    assert results_tool_lower == first_half_tools

    query: Query = Query(
        [
            Projection("id"),
            Projection("name"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Tools",
        LowerEqPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_lower_eq_tools: List[Dict] = query.execute(context)

    first_half_plus_one_tools = generated_tools[0 : int(len(generated_tools) / 2) + 1]
    assert results_lower_eq_tools == first_half_plus_one_tools

    query: Query = Query(
        [
            Projection("id"),
            Projection("name"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Tools",
        GreaterPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_greater_tools: List[Dict] = query.execute(context)

    greater_tools = generated_tools[int(len(generated_tools) / 2) + 1 :]
    assert results_greater_tools == greater_tools

    query: Query = Query(
        [
            Projection("id"),
            Projection("name"),
            Projection("start"),
            Projection("end"),
            Projection("machine"),
        ],
        "Tools",
        GreaterEqPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_greater_eq_tools: List[Dict] = query.execute(context)

    greater_eq_tools = generated_tools[int(len(generated_tools) / 2) :]
    assert results_greater_eq_tools == greater_eq_tools
