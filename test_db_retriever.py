from typing import List, Dict

from database_methods import (
    PqlEntity,
    insert_entity,
    delete_all_rows_from_pql_entity,
    migrate,
    delete_all_rows_from_counter_saving,
)
from main import DbMemoryAssetRetriever
from pql import (
    Query,
    Projection,
    Aggregation,
    RootContext,
    agg_functions,
    EqPredicate,
)


def test_one():
    states: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 2,
                "material_equipped": 1,
            },
            {
                "id": 22,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "start": 3,
                "end": 4,
                "material_equipped": 1,
            },
        ],
        "MaterialEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "material_name": "Material 1",
                "start": 1,
                "end": 4,
            }
        ],
    }
    migrate()
    delete_all_rows_from_pql_entity()
    delete_all_rows_from_counter_saving()
    for el in states:
        for en in states.get(el):
            insert_entity(
                PqlEntity(
                    id=en.get("uuid"),
                    name=el,
                    start=en.get("start"),
                    end=en.get("end"),
                    value=en,
                )
            )

    # Concrete Example:
    # SELECT COUNT(*) FROM Cycles, Material AS m WHERE m.material_name = "Material 1"
    dbassetRetriever: DbMemoryAssetRetriever = DbMemoryAssetRetriever()
    context: RootContext = RootContext(
        dbassetRetriever.get_assest, lambda s: agg_functions.get(s)
    )
    query: Query = Query(
        [
            Projection("material_name"),
            Projection("id"),
            Projection("uuid"),
            Aggregation("count", Query([Projection("id")], "Cycle"), name="cycles"),
        ],
        "MaterialEquipped",
        EqPredicate("material_name", "Material 1"),
    )
    results: List[Dict] = query.execute(context)
    assert results == [
        {"material_name": "Material 1"},
        {"id": 1},
        {"uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db"},
        {"cycles": 2},
    ]


def test_two():
    states: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 2,
                "material_equipped": 1,
            },
            {
                "id": 22,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "start": 3,
                "end": 4,
                "material_equipped": 1,
            },
        ],
        "MaterialEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "material_name": "Material 1",
                "start": 1,
                "end": 4,
            }
        ],
    }
    migrate()
    delete_all_rows_from_pql_entity()
    delete_all_rows_from_counter_saving()
    for el in states:
        for en in states.get(el):
            insert_entity(
                PqlEntity(
                    id=en.get("uuid"),
                    name=el,
                    start=en.get("start"),
                    end=en.get("end"),
                    value=en,
                )
            )

    # Concrete Example:
    # SELECT COUNT(*) FROM Cycles, Material AS m WHERE m.material_name = "Material 1"
    dbassetRetriever: DbMemoryAssetRetriever = DbMemoryAssetRetriever()
    context: RootContext = RootContext(
        dbassetRetriever.get_assest, lambda s: agg_functions.get(s)
    )
    query: Query = Query(
        [
            Aggregation("count", Query([Projection("*")], "Cycle"), name="cycles"),
        ],
        "Cycle",
        group_by_clause=[Projection("Cycle")],
    )
    results: List[Dict] = query.execute(context)
    assert results == [{"cycles": 1}, {"cycles": 1}]
