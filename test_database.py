import pytest

from database_methods import migrate, delete_all_rows, get_all_entitys, PqlEntity
from extract import SyncDatabase


def run_database_processor(end_result):
    migrate()
    delete_all_rows()
    db_sync: SyncDatabase = SyncDatabase(end_result)
    db_sync.synchronize_database()
    return get_all_entitys()


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

    results: [PqlEntity] = run_database_processor(states)
    assert len(results) == 3
    assert results[0].value == {
        "id": 21,
        "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
        "start": 1,
        "end": 2,
        "material_equipped": 1,
    }
    assert results[1].value == {
        "id": 22,
        "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
        "start": 3,
        "end": 4,
        "material_equipped": 1,
    }
    assert results[2].value == {
        "id": 1,
        "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
        "material_name": "Material 1",
        "start": 1,
        "end": 4,
    }


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

    results: [PqlEntity] = run_database_processor(states)
    assert len(results) == 3
    assert results[0].value == {
        "id": 21,
        "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
        "start": 1,
        "end": 2,
        "material_equipped": 1,
    }
    assert results[1].value == {
        "id": 22,
        "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
        "start": 3,
        "end": 4,
        "material_equipped": 1,
    }
    assert results[2].value == {
        "id": 1,
        "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
        "material_name": "Material 1",
        "start": 1,
        "end": 4,
    }


def test_cycle_material_switch():
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
                "material_equipped": 2,
            },
        ],
        "MaterialEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "material_name": "Material 1",
                "start": 1,
                "end": 2,
            },
            {
                "id": 2,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "material_name": "Material 2",
                "start": 3,
                "end": 4,
            },
        ],
    }

    results = run_database_processor(states)

    material_dict: dict = {
        "id": 1,
        "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
        "material_name": "Material 1",
        "start": 1,
        "end": 2,
    }
    material_dict2: dict = {
        "id": 2,
        "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
        "material_name": "Material 2",
        "start": 3,
        "end": 4,
    }

    cycle_dict: dict = {
        "id": 21,
        "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
        "start": 1,
        "end": 2,
        "material_equipped": 1,
    }
    cycle_dict2: dict = {
        "id": 22,
        "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
        "start": 3,
        "end": 4,
        "material_equipped": 2,
    }

    assert len(results) == 4

    assert results[2].value == material_dict
    assert results[3].value == material_dict2

    assert results[0].value == cycle_dict
    assert results[1].value == cycle_dict2


def test_tool_and_others_not():
    states: dict = {
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "tool_name": "Tool1",
                "start": 1,
                "end": 4,
            }
        ]
    }

    results = run_database_processor(states)

    tool_dict: dict = {
        "id": 1,
        "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
        "tool_name": "Tool1",
        "start": 1,
        "end": 4,
    }

    assert len(results) == 1

    assert results[-1].value == tool_dict


def test_tool_change_and_relation_with_cycle():
    states: dict = {
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": 21,
            },
            {
                "id": 2,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "tool_name": "Tool2",
                "start": 3,
                "end": 5,
                "cycle": 22,
            },
        ],
        "Cycle": [
            {
                "id": 21,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "start": 1,
                "end": 2,
            },
            {
                "id": 22,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "start": 3,
                "end": 5,
            },
        ],
    }

    results = run_database_processor(states)

    tool_dict: dict = {
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": 21,
            },
            {
                "id": 2,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "tool_name": "Tool2",
                "start": 3,
                "end": 5,
                "cycle": 22,
            },
        ]
    }

    cycle_dict: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "start": 1,
                "end": 2,
            },
            {
                "id": 22,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "start": 3,
                "end": 5,
            },
        ]
    }

    assert len(results) == 4

    assert results[0].value == tool_dict.get("ToolEquipped")[0]
    assert results[1].value == tool_dict.get("ToolEquipped")[1]

    assert results[2].value == cycle_dict.get("Cycle")[0]
    assert results[3].value == cycle_dict.get("Cycle")[1]


def test_tool_change_and_relation_befor_entity():
    states: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 2,
            },
            {
                "id": 22,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "start": 3,
                "end": 5,
            },
        ],
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": 21,
            },
            {
                "id": 2,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "tool_name": "Tool2",
                "start": 3,
                "end": 5,
                "cycle": 22,
            },
        ],
    }

    results = run_database_processor(states)

    tool_dict: dict = {
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": 21,
            },
            {
                "id": 2,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "tool_name": "Tool2",
                "start": 3,
                "end": 5,
                "cycle": 22,
            },
        ]
    }

    cycle_dict: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 2,
            },
            {
                "id": 22,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "start": 3,
                "end": 5,
            },
        ]
    }

    assert len(results) == 4

    assert results[0].value == cycle_dict.get("Cycle")[0]
    assert results[1].value == cycle_dict.get("Cycle")[1]

    assert results[2].value == tool_dict.get("ToolEquipped")[0]
    assert results[3].value == tool_dict.get("ToolEquipped")[1]


def test_relation_one_to_one_true():
    states: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 2,
            },
            {
                "id": 22,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "start": 3,
                "end": 5,
            },
        ],
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": 21,
            },
            {
                "id": 2,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "tool_name": "Tool2",
                "start": 3,
                "end": 5,
                "cycle": 22,
            },
        ],
    }

    results = run_database_processor(states)

    tool_dict: dict = {
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": 21,
            },
            {
                "id": 2,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "tool_name": "Tool2",
                "start": 3,
                "end": 5,
                "cycle": 22,
            },
        ]
    }

    cycle_dict: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 2,
            },
            {
                "id": 22,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "start": 3,
                "end": 5,
            },
        ]
    }
    assert len(results) == 4

    assert results[0].value == cycle_dict.get("Cycle")[0]
    assert results[1].value == cycle_dict.get("Cycle")[1]

    assert results[2].value == tool_dict.get("ToolEquipped")[0]
    assert results[3].value == tool_dict.get("ToolEquipped")[1]


def test_relation_many_to_many_true():
    states: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 5,
            },
            {
                "id": 22,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "start": 6,
                "end": 9,
            },
            {
                "id": 23,
                "uuid": "e35fd989-b021-4ac8-8c31-76236de8809a",
                "start": 10,
                "end": 12,
            },
        ],
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": [21],
            },
            {
                "id": 2,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "tool_name": "Tool2",
                "start": 3,
                "end": 12,
                "cycle": [21, 22, 23],
            },
        ],
    }

    results = run_database_processor(states)
    tool_dict: dict = {
        "ToolEquipped": [
            {
                "id": 1,
                "uuid": "5bb7fb0f-d8d9-415b-8725-460b8ce504db",
                "tool_name": "Tool1",
                "start": 1,
                "end": 2,
                "cycle": [21],
            },
            {
                "id": 2,
                "uuid": "ce9c02e4-aafb-4062-97ff-bbaaeca05990",
                "tool_name": "Tool2",
                "start": 3,
                "end": 12,
                "cycle": [21, 22, 23],
            },
        ]
    }

    cycle_dict: dict = {
        "Cycle": [
            {
                "id": 21,
                "uuid": "b1cb1816-7d85-457c-9f32-2a1422d00e17",
                "start": 1,
                "end": 5,
            },
            {
                "id": 22,
                "uuid": "0583542f-4cd5-413d-8c6e-3438c63be5a9",
                "start": 6,
                "end": 9,
            },
            {
                "id": 23,
                "uuid": "e35fd989-b021-4ac8-8c31-76236de8809a",
                "start": 10,
                "end": 12,
            },
        ]
    }

    assert len(results) == 5

    assert results[0].value == cycle_dict.get("Cycle")[0]
    assert results[1].value == cycle_dict.get("Cycle")[1]
    assert results[2].value == cycle_dict.get("Cycle")[2]

    assert results[3].value == tool_dict.get("ToolEquipped")[0]
    assert results[4].value == tool_dict.get("ToolEquipped")[1]
