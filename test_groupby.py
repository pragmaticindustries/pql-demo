import random
from typing import List, Dict

from main import create_tools, create_cycles, create_materials, InMemoryAssetRetriever
from pql import (
    agg_functions,
    RootContext,
    Query,
    Projection,
    Aggregation,
    SubQuery,
)

generated_tools = create_tools(5)
generated_cycles = create_cycles(100)
generated_materials = create_materials(10)

asset_retriever = InMemoryAssetRetriever()
context = RootContext(asset_retriever.get_assets, lambda s: agg_functions.get(s))


def test_query():
    query: Query = Query(
        [
            Projection("name"),
            Aggregation("count", Query([Projection("*")], "Cycles"), name="cycles"),
            Aggregation(
                "flatten", Query([Projection("material")], "Materials"), name="products"
            ),
            SubQuery(
                Query(
                    [
                        Projection("material"),
                        Aggregation(
                            "count", Query([Projection("*")], "Cycles"), name="cycles"
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
    assert results[0] == {
        "name": "Tool 0",
        "cycles": 26,
        "products": ["Material 0", "Material 1"],
        "material_and_count": [
            {"material": "Material 0", "cycles": 14},
            {"material": "Material 1", "cycles": 14},
        ],
    }


# def test_part_query():
#     query = Query(
#                 [
#                     Projection("material"),
#                     Aggregation(
#                         "count", Query([Projection("*")], "Cycles"), name="cycles"
#                     ),
#                 ],
#                 "Materials",
#             )
#     resulst = query.execute(context)
#     print(resulst)
#
# def test_part_qury2():
#     query = Query(
#     [
#         Projection("name"),
#         Aggregation("count", Query([Projection("*")], "Cycles"), name="cycles"),
#         Aggregation(
#             "flatten", Query([Projection("material")], "Materials"), name="products"
#         )],
#     "Materials",
# )
#     result = query.execute(context)
#     print(result)
#
# def test_simple_group_by():
#     # print(generated_materials)
#     query =   Query(
#         [Projection("id"),Projection("machine"),
#          Aggregation("count",
#                      Query(
#             [Projection("machine")],
#             "Materials"),name="Anzahl")],
#         "Materials")
#     result = query.execute(context)
#     print(result)
#
# def test_count_material_on_machine():
#     pass
# def test_sub_query():
#     query = SubQuery(Query([Projection("id"), Projection("start"), Projection("end"), Projection("machine")], "Cycles"),"Cycles")
#     query.execute(context)
# def test_sub_query2():
#     query = Query(
#     [
#         SubQuery(
#             Query(
#                 [
#                     Projection("material"),
#                     Aggregation(
#                         "count", Query([Projection("*")], "Cycles"), name="cycles"
#                     ),
#                 ],
#                 "Materials",
#             ),
#             name="material_and_count",
#         ),
#     ],
#     "Tools",
# )
#     results= query.execute(context)
#     print(results)
