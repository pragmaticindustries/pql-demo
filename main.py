import random
import uuid
from datetime import datetime, timedelta
from random import uniform
from typing import Iterable, List, Dict, Union, Callable

from sqlalchemy.engine import row

from database_methods import (
    PqlEntity,
    get_entity_with_name,
    get_all_names,
    get_values_from_all_entitys_as_dict,
    execute_raw_sql_query,
    get_entity_with_name_and_predicate,
    migrate,
    insert_entity,
    delete_all_rows_from_pql_entity,
    delete_all_rows_from_counter_saving,
)
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
        element_dict: dict = {
            "id": i,
            "start": f"{timestamp}",
            "end": f"{timestamp + cycle_duration}",
            "machine": "LHL 01",
        }
        cycles.append(element_dict)
        insert_entity(
            PqlEntity(
                id=element_dict.get("id"),
                name="Cycle",
                start=element_dict.get("start"),
                end=element_dict.get("end"),
                value=element_dict,
            )
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
        element_dict: dict = {
            "id": str(uuid.uuid4()),
            "tool_name": f"Tool {i}",
            "start": f"{timestamp}",
            "end": f"{timestamp + duration}",
            "machine": "LHL 01",
        }
        tools.append(element_dict)
        insert_entity(
            PqlEntity(
                id=element_dict.get("id"),
                name="ToolEquipped",
                start=element_dict.get("start"),
                end=element_dict.get("end"),
                value=element_dict,
            )
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
        element_dict: dict = {
            "id": str(uuid.uuid4()),
            "material_name": f"Material {i % 2}",
            "start": f"{timestamp}",
            "end": f"{timestamp + duration}",
            "machine": "LHL 01",
        }
        materials.append(element_dict)
        insert_entity(
            PqlEntity(
                id=element_dict.get("id"),
                name="MaterialEquipped",
                start=element_dict.get("start"),
                end=element_dict.get("end"),
                value=element_dict,
            )
        )
        timestamp = timestamp + duration + pause

    return materials


def get_distinct_names_from_db():
    all_names = get_all_names()
    el: row
    names: [str] = []
    for el in all_names:
        names.append(el._mapping.get("name"))
    return names


def get_sorted_entitys():
    all_names: [str] = get_distinct_names_from_db()
    end_dict: dict = {}
    for el in all_names:
        mid_array = []
        entity: PqlEntity
        for entity in get_entity_with_name(el):
            mid_array.append(entity.value)
        end_dict.update({el: mid_array})
    return end_dict


def print_index_of_array(array: []):
    element: dict
    for element in array:
        print(element)


migrate()
delete_all_rows_from_pql_entity()
delete_all_rows_from_counter_saving()
create_tools(5)
create_cycles(100)
create_materials(10)

all_assets = get_sorted_entitys()
generated_tools = all_assets.get("ToolEquipped")
generated_cycles = all_assets.get("Cycle")
generated_materials = all_assets.get("MaterialEquipped")


def get_all_assets(name: str) -> Iterable[dict]:
    try:
        return all_assets.get(name)
    except Exception as ex:
        print(ex)
        raise Exception("")


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


class DbMemoryAssetRetriever:
    def get_assest(
        self, asset_type, where_clause: Union[Predicate, Callable], group_by_clause
    ):
        assets = get_values_from_all_entitys_as_dict(asset_type)
        group_by_sql: str = ""
        if group_by_clause:
            print(group_by_clause)
            group_by_sql = "GROUP BY "
            for el in group_by_clause:
                group_by_sql += el.field
        if where_clause:
            if isinstance(where_clause, Predicate):
                where_clause_type = type(where_clause)
                assets = execute_raw_sql_query(
                    asset_type,
                    self.wrap_pql_predicate_to_sqll(
                        where_clause_type,
                        where_clause.property,
                        where_clause.value,
                        asset_type,
                    ),
                    group_by_sql,
                )
                # for later usage if where_clause is an list of Where clauses.
                # where_clauses = [element for element in where_clause self.wrap_pql_predicate_to_sql(type(element))]
                # assets = [element for element in assets if where_clause.check(element)]
            else:
                assets = [o for o in assets if where_clause(o)]
        return assets

    def wrap_name_to_pql_attribute(self, property: str):
        if property == "id":
            return PqlEntity.id
        elif property == "name":
            return PqlEntity.name
        elif property == "start":
            return PqlEntity.start
        elif property == "end":
            return PqlEntity.end
        elif property == "value":
            return PqlEntity.value
        else:
            return PqlEntity

    def wrap_pql_predicate_to_sql(self, test, property, value, name):
        property_sql = self.wrap_name_to_pql_attribute(property)
        if test == EqPredicate:
            return "PqlEntity.name == name,property_sql == value"
        elif test == GreaterPredicate:
            return "PqlEntity.name == name,property_sql > value"
        elif test == GreaterEqPredicate:
            return "PqlEntity.name == name ,property_sql >= value"
        elif test == LowerPredicate:
            return "PqlEntity.name == name, property_sql < value"
        elif test == LowerEqPredicate:
            return "PqlEntity.name==name, property_sql <= value"
        else:
            return ""

    def wrap_pql_predicate_to_sqll(self, test, property: str, value, name):
        if test == EqPredicate:
            if type(value) == str:
                return f"json_extract(value, '$.{property}') = '{value}' "
            else:
                return f"json_extract(value, '$.{property}') = {value} "
        elif test == GreaterPredicate:
            if type(value) == str:
                return f"json_extract(value, '$.{property}') > '{value}' "
            else:
                return f"json_extract(value, '$.{property}') > {value} "
        elif test == GreaterEqPredicate:
            if type(value) == str:
                return f"json_extract(value, '$.{property}') >= '{value}' "
            else:
                return f"json_extract(value, '$.{property}') >= {value} "
        elif test == LowerPredicate:
            if type(value) == str:
                return f"json_extract(value, '$.{property}') < '{value}' "
            else:
                return f"json_extract(value, '$.{property}') < {value} "
        elif test == LowerEqPredicate:
            if type(value) == str:
                return f"json_extract(value, '$.{property}') <= '{value}' "
            else:
                return f"json_extract(value, '$.{property}') <= {value} "
        else:
            return ""


if __name__ == "__main__":
    # SELECT t.name, COUNT(SELECT c FROM Cycles [WHERE t.start <= c.start AND c.start < t.end]),
    # LIST(SELECT m.material FROM Materials [WHERE t.start <= m.start AND m.start < t.end])
    # FROM Tools
    # * Cycle
    # * MaterialEquipped
    # * ToolEquipped

    dbassetRetriever: DbMemoryAssetRetriever = DbMemoryAssetRetriever()
    context: RootContext = RootContext(
        dbassetRetriever.get_assest, lambda s: agg_functions.get(s)
    )
    # Concrete Example:
    # SELECT t.name, COUNT(SELECT c FROM Cycles) AS "cycles" FROM Tool
    # a = ctypes.cast(context, ctypes.py_object).value
    # print(a)
    query: Query = Query(
        [
            Projection("tool_name"),
            Projection("id"),
            Projection("uuid"),
            Aggregation("count", Query([Projection("id")], "Cycle"), name="cycles"),
        ],
        "ToolEquipped",
    )
    results: List[Dict] = query.execute(context)

    # The result is a list of dicts representing a (probably nested) table

    print(results)
    # Concrete Example:
    # SELECT COUNT(*) FROM Cycles, ToolEquipped AS t WHERE t.name = "Tool 0"

    query: Query = Query(
        [
            Projection("tool_name"),
            Projection("id"),
            Projection("uuid"),
            Aggregation("count", Query([Projection("id")], "Cycle"), name="cycles"),
        ],
        "ToolEquipped",
        EqPredicate("tool_name", "Tool 0"),
    )
    results: List[Dict] = query.execute(context)
    print(results)

    # Concrete Example:
    # SELECT t.name, COUNT(SELECT c FROM Cycles) AS "cycles", FLATTEN(SELECT m.material FROM Materials) AS "products",
    # (SELECT m.material, COUNT(SELECT c FROM Cycles) FROM Materials) AS "material_and_count"
    # FROM Tools AS t
    query: Query = Query(
        [
            Projection("tool_name"),
            Aggregation("count", Query([Projection("id")], "Cycle"), name="cycles"),
            Aggregation(
                "flatten",
                Query([Projection("material_name")], "MaterialEquipped"),
                name="products",
            ),
            SubQuery(
                Query(
                    [
                        Projection("material_name"),
                        Aggregation(
                            "count", Query([Projection("id")], "Cycle"), name="cycles"
                        ),
                    ],
                    "MaterialEquipped",
                ),
                name="material_and_count",
            ),
        ],
        "ToolEquipped",
    )
    #
    results: List[Dict] = query.execute(context)

    # The result is a list of dicts representing a (probably nested) table
    print(results)

    # Example 2: Filter by Material
    # SELECT m.material, COUNT(SELECT c FROM Cycles) AS "cycles"
    # FROM Materials
    query: Query = Query(
        [
            Projection("material_name"),
            Aggregation("count", Query([Projection("id")], "Cycle"), name="cycles"),
        ],
        "MaterialEquipped",
        group_by_clause=[Projection("material_name")],
    )
    #
    results: List[Dict] = query.execute(context)

    # The result is a list of dicts representing a (probably nested) table
    print(results)

    query: Query = Query(
        [
            Projection("*"),
        ],
        "MaterialEquipped",
    )
    results_all_materials: List[Dict] = query.execute(context)

    assert results_all_materials == generated_materials

    query: Query = Query(
        [
            Projection("*"),
        ],
        "ToolEquipped",
    )
    results_all_tools: List[Dict] = query.execute(context)

    assert results_all_tools == generated_tools

    query: Query = Query(
        [
            Projection("*"),
        ],
        "Cycle",
    )
    results_all_cycles: List[Dict] = query.execute(context)

    assert results_all_cycles == generated_cycles

    query: Query = Query(
        [
            Projection("*"),
        ],
        "Cycle",
        EqPredicate("start", generated_cycles[0].get("start")),
    )
    results_eq_cycles: List[Dict] = query.execute(context)

    assert results_eq_cycles[0] == generated_cycles[0]

    query: Query = Query(
        [
            Projection("*"),
        ],
        "Cycle",
        LowerPredicate(
            "start", generated_cycles[int(len(generated_cycles) / 2)].get("start")
        ),
    )
    results_lower_cycle: List[Dict] = query.execute(context)

    first_four = generated_cycles[0 : int(len(generated_cycles) / 2)]
    assert results_lower_cycle == first_four

    query: Query = Query(
        [
            Projection("*"),
        ],
        "Cycle",
        LowerEqPredicate(
            "start", generated_cycles[int(len(generated_cycles) / 2)].get("start")
        ),
    )
    results_lower_eq_cycles: List[Dict] = query.execute(context)

    first_five = generated_cycles[0 : int(len(generated_cycles) / 2) + 1]
    assert results_lower_eq_cycles == first_five

    query: Query = Query(
        [
            Projection("*"),
        ],
        "Cycle",
        GreaterPredicate(
            "start", generated_cycles[int(len(generated_cycles) / 2)].get("start")
        ),
    )
    results_greater_cycles: List[Dict] = query.execute(context)

    first_four = generated_cycles[int(len(generated_cycles) / 2) + 1 :]
    assert results_greater_cycles == first_four

    query: Query = Query(
        [
            Projection("*"),
        ],
        "Cycle",
        GreaterEqPredicate(
            "start", generated_cycles[int(len(generated_cycles) / 2)].get("start")
        ),
    )
    results_greater_eq_cycles: List[Dict] = query.execute(context)

    first_five = generated_cycles[int(len(generated_cycles) / 2) :]
    assert results_greater_eq_cycles == first_five

    # Materials
    query: Query = Query(
        [
            Projection("*"),
        ],
        "MaterialEquipped",
        EqPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_eq: List[Dict] = query.execute(context)
    assert results_eq[0] == generated_materials[int(len(generated_materials) / 2)]

    query: Query = Query(
        [
            Projection("*"),
        ],
        "MaterialEquipped",
        LowerPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_lower_materials: List[Dict] = query.execute(context)

    last_half_materials = generated_materials[0 : int(len(generated_materials) / 2)]
    assert results_lower_materials == last_half_materials

    query: Query = Query(
        [
            Projection("*"),
        ],
        "MaterialEquipped",
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
            Projection("*"),
        ],
        "MaterialEquipped",
        GreaterPredicate(
            "start", generated_materials[int(len(generated_materials) / 2)].get("start")
        ),
    )
    results_greater_materials: List[Dict] = query.execute(context)

    first_half_materials = generated_materials[int(len(generated_materials) / 2) + 1 :]
    assert results_greater_materials == first_half_materials

    query: Query = Query(
        [
            Projection("*"),
        ],
        "MaterialEquipped",
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
            Projection("*"),
        ],
        "ToolEquipped",
        EqPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_eq_tool = query.execute(context)

    assert results_eq_tool[0] == generated_tools[int(len(generated_tools) / 2)]

    query: Query = Query(
        [
            Projection("*"),
        ],
        "ToolEquipped",
        LowerPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_tool_lower: List[Dict] = query.execute(context)

    first_half_tools = generated_tools[0 : int(len(generated_tools) / 2)]
    assert results_tool_lower == first_half_tools

    query: Query = Query(
        [
            Projection("*"),
        ],
        "ToolEquipped",
        LowerEqPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_lower_eq_tools: List[Dict] = query.execute(context)

    first_half_plus_one_tools = generated_tools[0 : int(len(generated_tools) / 2) + 1]
    assert results_lower_eq_tools == first_half_plus_one_tools

    query: Query = Query(
        [
            Projection("*"),
        ],
        "ToolEquipped",
        GreaterPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_greater_tools: List[Dict] = query.execute(context)

    greater_tools = generated_tools[int(len(generated_tools) / 2) + 1 :]
    assert results_greater_tools == greater_tools

    query: Query = Query(
        [
            Projection("*"),
        ],
        "ToolEquipped",
        GreaterEqPredicate(
            "start", generated_tools[int(len(generated_tools) / 2)].get("start")
        ),
    )
    results_greater_eq_tools: List[Dict] = query.execute(context)

    greater_eq_tools = generated_tools[int(len(generated_tools) / 2) :]
    assert results_greater_eq_tools == greater_eq_tools
