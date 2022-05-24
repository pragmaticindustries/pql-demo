from typing import List, Dict

from database_methods import (
    migrate,
    delete_all_rows_from_pql_entity,
    delete_all_rows_from_counter_saving,
)
from main import DbMemoryAssetRetriever, create_tools, create_cycles, create_materials
from pql import (
    agg_functions,
    RootContext,
    Query,
    Projection,
    Aggregation,
    SubQuery,
)



def test_query():
    migrate()
    delete_all_rows_from_pql_entity()
    delete_all_rows_from_counter_saving()
    create_tools(5)
    create_cycles(100)
    create_materials(10)

    dbassetRetriever: DbMemoryAssetRetriever = DbMemoryAssetRetriever()
    context: RootContext = RootContext(
        dbassetRetriever.get_assest, lambda s: agg_functions.get(s)
    )

    query: Query = Query(
        [
            Projection("tool_name"),
            Aggregation("count", Query([Projection("*")], "Cycle"), name="cycles"),
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
                            "count", Query([Projection("*")], "Cycle"), name="cycles"
                        ),
                    ],
                    "MaterialEquipped",
                ),
                name="material_and_count",
            ),
        ],
        "ToolEquipped",
    )

    results: List[Dict] = query.execute(context)
    assert results == [
        {"tool_name": "Tool 0"},
        {"cycles": 26},
        {"products": ["Material 0", "Material 1"]},
        {
            "material_and_count": [
                {"material_name": "Material 0"},
                {"cycles": 14},
                {"material_name": "Material 1"},
                {"cycles": 14},
            ]
        },
        {"tool_name": "Tool 1"},
        {"cycles": 31},
        {"products": ["Material 1"]},
        {"material_and_count": [{"material_name": "Material 1"}, {"cycles": 16}]},
        {"tool_name": "Tool 2"},
        {"cycles": 28},
        {"products": ["Material 0", "Material 1", "Material 0"]},
        {
            "material_and_count": [
                {"material_name": "Material 0"},
                {"cycles": 20},
                {"material_name": "Material 1"},
                {"cycles": 7},
                {"material_name": "Material 0"},
                {"cycles": 0},
            ]
        },
        {"tool_name": "Tool 3"},
        {"cycles": 0},
        {"products": ["Material 1", "Material 0"]},
        {
            "material_and_count": [
                {"material_name": "Material 1"},
                {"cycles": 0},
                {"material_name": "Material 0"},
                {"cycles": 0},
            ]
        },
        {"tool_name": "Tool 4"},
        {"cycles": 0},
        {"products": []},
        {"material_and_count": []},
    ]
