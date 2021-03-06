import random
from time import sleep
from typing import List, Dict

from database_methods import (
    delete_all_rows_from_pql_entity,
    migrate,
    delete_all_rows_from_counter_saving,
)
from extract import (
    StateProcessor,
    SyncDatabase,
    Parameter,
    AutoGeneratedUUID,
    AutoGeneratedManyToOne,
    AutoGeneratedField,
)
from main import get_sorted_entitys, DbMemoryAssetRetriever
from pql import (
    RootContext,
    agg_functions,
    Query,
    Projection,
    EqPredicate,
    GreaterEqPredicate,
    GreaterPredicate,
    LowerEqPredicate,
    LowerPredicate,
)


def intiStates():
    material_id = 1
    tool_id = 1
    cycle_id = 1
    old_cycle_id = 1

    states = []

    random.seed(1)

    for t in range(0, 1000):

        if random.uniform(0.0, 100.0) < 10.0:
            cycle_id += 1
        current_cycle_number = cycle_id

        if random.uniform(0.0, 50.0) < 1.0:
            if old_cycle_id != current_cycle_number:
                tool_id += 1
        current_tool = f"Tool {tool_id}"
        if random.uniform(0.0, 100.0) < 1.0:
            if old_cycle_id != current_cycle_number:
                material_id = (material_id + 1) % 3
        current_material = f"Material {material_id}"

        old_cycle_id = current_cycle_number
        state = {
            "timestamp": t,
            "material_name": current_material,
            "material_type": current_material,
            "tool_name": current_tool,
            "cycle_number": current_cycle_number,
        }

        # print(f"Aktueller Zustand: {state}")

        states.append(state)
    return states


def get_config() -> dict:
    return {
        "Cycle": {
            "fields": {
                # "id": AutoGeneratedField(),
                "id": Parameter("cycle_number"),
                # "cycle": Parameter("cycle_number"),
                "uuid": AutoGeneratedUUID(),
                "material_equipped": AutoGeneratedManyToOne("MaterialEquipped", "id"),
            },
            "primary_key": "id",
        },
        # CREATE TABLE Cycle (id BIGINT SERIAL PRIMARY KEY, material_equipped BIGINT NON NULL)
        # CREATE FOREIGN KEY ...
        "ToolEquipped": {
            "fields": {
                "id": AutoGeneratedField(),
                "uuid": AutoGeneratedUUID(),
                "tool_name": Parameter("tool_name"),
            },
            "primary_key": "id",
        },
        "MaterialEquipped": {
            "fields": {
                "id": AutoGeneratedField(),
                "uuid": AutoGeneratedUUID(),
                "material_name": Parameter("material_name"),
                "material_typ": Parameter("material_type"),
            },
            "primary_key": "id",
        },
    }


def test_one():
    migrate()
    delete_all_rows_from_pql_entity()
    delete_all_rows_from_counter_saving()
    states = intiStates()
    processor: StateProcessor = StateProcessor(get_config())
    processor.init_context(states[0])
    db_sync: SyncDatabase = SyncDatabase(processor.get_result())
    db_sync.sync_database_with_index()
    for state in states:
        processor.process_state(state)
        db_sync.set_end_result(processor.get_result())
        db_sync.sync_database_with_index()
    end_result = processor.get_result()
    end_result_db_values = get_sorted_entitys()

    assert end_result_db_values == end_result

    generated_tools = end_result.get("ToolEquipped")
    generated_cycles = end_result.get("Cycle")
    generated_materials = end_result.get("MaterialEquipped")

    dbassetRetriever: DbMemoryAssetRetriever = DbMemoryAssetRetriever()
    context: RootContext = RootContext(
        dbassetRetriever.get_assest, lambda s: agg_functions.get(s)
    )

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
