import json
import random
import uuid
from typing import Any
from faker import Faker
from database_classes import insert_entity, PqlEntity,\
    update_end_of_entity_and_in_json_with_id, delete_all_rows, migrate


faker1: Faker = Faker()
Faker.seed(4711)

def set_faker(faker):
   global faker1
   faker1= faker

class FieldType(object):
    def is_generated(self) -> bool:
        raise NotImplementedError

    def is_foreignkey(self) -> bool:
        raise NotImplementedError

    def apply(self, current_state: dict) -> Any:
        raise NotImplementedError

    def reset(sef) -> Any:
        raise NotImplementedError

    def reset_all(self) -> Any:
        raise NotImplementedError


class AutoGeneratedField(FieldType):
    def __init__(self) -> None:
        self.counter = 0

    def is_generated(self) -> bool:
        return True

    def is_foreignkey(self) -> bool:
        return False

    def apply(self, current_state: dict):
        self.counter += 1
        return self.counter

    def reset(self):
        self.counter = 0

    def reset_all(self):
        self.counter = 0

    def __str__(self) -> str:
        return f"AutoGenerated({self.counter + 1})"


class AutoGeneratedUUID(FieldType):
    def __init__(self) -> None:
        self.uuid = 0

    def is_generated(self) -> bool:
        return True

    def is_foreignkey(self) -> bool:
        return False

    def apply(self, current_state: dict):
        #TODO comment out when on prod this is only for testing!!!!
        self.uuid = str(faker1.uuid4())
        # self.uuid = str(uuid.uuid4())
        return self.uuid

    def reset(self):
        self.uuid = 0

    def reset_all(self):
        self.uuid = 0

    def __str__(self) -> str:
        return f"AutoGenerated({self.uuid})"


class ReferenceFieldType(FieldType):
    def __init__(self, config_entity: str, entity_id: str) -> None:
        self.config_entity: str = config_entity
        self.relation_id: str = entity_id

    def is_foreignkey(self) -> bool:
        return True

    def apply(
        self, parent_entity: str, relation_key: str, end_result: dict) -> Any:
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def reset_all(self):
        raise NotImplementedError

    def set_own_id(self):
        raise NotImplementedError


class AutoGeneratedOneToOne(ReferenceFieldType):
    def __init__(self, config_entity: str, entity_id: str) -> None:
        self.config_entity = config_entity
        self.relation_id = entity_id
        self.change_index = 0
        self.constraints = []
        self.own_id = None

    def is_generated(self) -> bool:
        return True

    def is_foreignkey(self) -> bool:
        return True

    def apply(
        self, parent_entity: str, relation_key: str, end_result: dict):
        """In this Method the Rleation ist set.
        First of al we loop through the entire Config
        Then we range through the loop where the last change therefore the last Element is
        Then we look up it there are The same dicts in the Constrains
        The Paramter the partent_entity ist the Entity which saves the Foreignkey
        The State is the actual array from the Stream
        End_Result is the sorted dict
        and key ist the Name of the Relation from the Config"""

        parent_entity_id = end_result[parent_entity][-1].get(self.own_id)
        foreignkey_id = end_result[self.config_entity][-1].get(self.relation_id)

        contstraint_entry: dict = {
            f"{parent_entity}_id": parent_entity_id,
            f"{self.config_entity}_{self.relation_id}": foreignkey_id,
        }
        if contstraint_entry in self.constraints:
            raise RuntimeError(f"Duplicated Set in Constrins{contstraint_entry}")
            return
        for element in self.constraints:
            if (
                parent_entity_id in element.values()
                or foreignkey_id in element.values()
            ):
                raise RuntimeError(
                    f"Duplicated Keys for {parent_entity} or {foreignkey_id}"
                )
                return
        self.constraints.append(contstraint_entry)
        return_dict: dict = {f"{relation_key}": foreignkey_id}
        return return_dict

    def reset(self):
        self.change_index = 0

    def reset_all(self):
        self.reset()
        self.constraints = {}
        self.relation_id = None
        self.config_entity = None

    def set_own_id(self,id:str):
        self.own_id = id

    def __str__(self) -> str:
        return f"AutoGenerated({self.config_entity,self.relation_id})"


class AutoGeneratedManyToOne(ReferenceFieldType):
    def __init__(self, config_entity: str, entity_id: str) -> None:
        self.config_entity = config_entity
        self.relation_id = entity_id
        self.change_index = 0
        self.constraints = []
        self.own_id = None

    def is_generated(self) -> bool:
        return True

    def is_foreignkey(self) -> bool:
        return True

    def apply(self, parent_entity: str, key, end_result: dict):
        """In this Method the Rleation ist set.
        First of al we loop through the entire Config
        Then we range through the loop where the last change therefore the last Element is
        Then we look up it there are The same dicts in the Constrains
        The Paramter the partent_entity ist the Entity which saves the Foreignkey
        The State is the actual array from the Stream
        End_Result is the sorted dict
        and key ist the Name of the Relation from the Config"""
        parent_entity_id = end_result[parent_entity][-1].get(self.own_id)
        foreignkey_id = end_result[self.config_entity][-1].get(self.relation_id)

        contstraint_entry: dict = {
            f"{parent_entity}_id": parent_entity_id,
            f"{self.config_entity}_{self.relation_id}": foreignkey_id,
        }
        if contstraint_entry in self.constraints:
            raise RuntimeError(f"The following: {contstraint_entry} is duplicated")
            return
        for element in self.constraints:
            if parent_entity_id in element.values():
                raise RuntimeError(
                    f"The following ID: {parent_entity_id} from {parent_entity} is duplicated!"
                )
                return
        self.constraints.append(contstraint_entry)
        return_dict: dict = {f"{key}": foreignkey_id}
        return return_dict

    def reset(self):
        self.change_index = 0
        self.constraints = []

    def reset_all(self):
        self.reset()
        self.relation_id = None
        self.config_entity = None

    def set_own_id(self,id:str):
        self.own_id = id

    def __str__(self) -> str:
        return f"AutoGenerated({self.config_entity,self.relation_id})"


class AutoGeneratedManyToMany(ReferenceFieldType):
    def __init__(self, config_entity: str, entity_id: str) -> None:
        self.config_entity = config_entity
        self.relation_id = entity_id
        self.change_index = 0
        self.constraints = []
        self.own_id = None

    def is_generated(self) -> bool:
        return True

    def apply(self, parent_entity: str, key: str, end_result: dict):
        """In this Method the Rleation ist set.
        First of al we loop through the entire Config
        Then we range through the loop where the last change therefore the last Element is
        Then we look up it there are The same dicts in the Constrains
        The Paramter the partent_entity ist the Entity which saves the Foreignkey
        The State is the actual array from the Stream
        End_Result is the sorted dict
        and key ist the Name of the Relation from the Config"""
        parent_entity_id = end_result[parent_entity][-1].get(self.own_id)
        foreignkey_id = end_result[self.config_entity][-1].get(self.relation_id)

        contstraint_entry: dict = {
            f"{parent_entity}": parent_entity_id,
            f"{self.config_entity}": foreignkey_id,
        }
        if contstraint_entry in self.constraints:
            raise RuntimeError(f"The following: {contstraint_entry} is duplicated")
            return
        self.constraints.append(contstraint_entry)
        fk_array: [] = []
        for elements in self.constraints:
            element_dict: dict = elements
            if element_dict.get(parent_entity) == parent_entity_id:
                fk_array.append(element_dict.get(self.config_entity))
        return_dict: dict = {f"{key}": fk_array}
        return return_dict

    def reset(self):
        self.change_index = 0
        self.constraints = []

    def reset_all(self):
        self.reset()
        self.config_entity = None
        self.relation_id = None

    def set_own_id(self, id:str):
        self.own_id = id

    def __str__(self) -> str:
        return f"AutoGenerated({self.config_entity,self.relation_id})"


class Parameter(FieldType):
    def __init__(self, field_name):
        self.field_name = field_name

    def is_generated(self) -> bool:
        return False

    def is_foreignkey(self) -> bool:
        return False

    def apply(self, current_state: dict):
        if self.field_name in current_state:
            return current_state.get(self.field_name)
        else:
            raise RuntimeError(f"No field {self.field_name} found!")

    def reset(self):
        self.change_index = None

    def reset_all(self):
        self.reset()

    def __str__(self) -> str:
        return f"-> {self.field_name}"


class StateProcessor(object):
    def __init__(self, config) -> None:
        super().__init__()
        self.config = config
        self.context = {}
        self.end_result = {}
        self.init_ids_in_foreignkeys()
        global  faker1
        faker1 = Faker()
        Faker.seed(4711)

    def __exctract_all_items__(self, entity, state):
        all_items: dict = {
            k: v.apply(state)
            for k, v in self.config.get(entity).get("fields").items()
            if not v.is_foreignkey()
        }
        all_items.update(
            {"start": state.get("timestamp"), "end": state.get("timestamp")}
        )
        return all_items

    def __extract_all_not_autogen_items__(self, entity, state):
        all_items: dict = {
            k: v.apply(state)
            for k, v in self.config.get(entity).get("fields").items()
            if not v.is_generated() and not v.is_foreignkey()
        }
        all_items.update(
            {"start": state.get("timestamp"), "end": state.get("timestamp")}
        )
        return all_items

    def reset_foreignkeys_and_constraints(self):
        """Needed for Tests if there are two ore more tests,
        the ID fk and constrians should be remove otherway there will be Errors"""
        for entity in self.config.keys():
            for k, v in self.config.get(entity).get("fields").items():
                if v.is_generated() and v.is_foreignkey():
                    element: ReferenceFieldType = v
                    element.reset()

    def reset_autogen_fields(self):
        """Needed if there is one config for many Tests"""
        for entity in self.config.keys():
            for k, v in self.config.get(entity).get("fields").items():
                if v.is_generated() and not v.is_foreignkey():
                    element: FieldType = v
                    element.reset()

    def reset_fk_constraints_and_autogen_fields(self):
        """Same thing needed for tests or if after nay brake down the Ids and so one shoulb be 0"""
        for entity in self.config.keys():
            for k, v in self.config.get(entity).get("fields").items():
                if v.is_generated():
                    element: FieldType = v
                    element.reset()

    def init_ids_in_foreignkeys(self):
        for entity in self.config.keys():
            for k,v in self.config.get(entity).get("fields").items():
                if v.is_generated() and v.is_foreignkey():
                    parameter:ReferenceFieldType = v
                    parameter.set_own_id(self.config.get(entity).get("primary_key"))


    def init_context(self, first_element_of_stream):
        """init Method for the Result dict
        The dict gets its Entitys here and get empty Arrays for appending each entry in the array later
        The first Element of the Stream will be append here, so we check if the acctual element is diffrent form this init  one"""
        self.changed = set()
        for entity in self.config.keys():
            x = {
                k: v.apply(first_element_of_stream)
                for k, v in self.config.get(entity).get("fields").items()
                if not v.is_foreignkey()
            }
            x.update(
                {
                    "start": first_element_of_stream.get("timestamp"),
                    "end": first_element_of_stream.get("timestamp"),
                }
            )
            self.context[entity] = {
                k: v.apply(first_element_of_stream)
                for k, v in self.config.get(entity).get("fields").items()
                if not v.is_generated() and not v.is_foreignkey()
            }
            self.end_result.update({f"{entity}": []})
            self.end_result[entity].append(x)
            insert_entity(PqlEntity(id=x.get("uuid"),number=x.get("id"),start=x.get("start"),end=x.get("end"),name=entity,value=json.dumps(x)))
            self.changed.add(entity)

        self.process_foreingkeys()

    def process_state(self, state):
        self.changed = set()
        for entity in self.config.keys():
            current_context = {
                k: v.apply(state)
                for k, v in self.config.get(entity).get("fields").items()
                if not v.is_generated() and not v.is_foreignkey()
            }
            if self.context[entity] != current_context:
                all_items = self.__exctract_all_items__(entity, state)
                self.end_result[entity].append(all_items)
                insert_entity(PqlEntity(id=all_items.get("uuid"),number=all_items.get("id"),start=all_items.get("start"),end=all_items.get("end"),name=entity,value=json.dumps(all_items)))
                # hier speiicherung in der db
                self.changed.add(entity)
            self.end_result[entity][-1]["end"] = state.get("timestamp")
            update_end_of_entity_and_in_json_with_id(self.end_result[entity][-1].get("uuid"),state.get("timestamp"), json.dumps(self.end_result[entity][-1])
                                                    )
            self.context[entity] = current_context

        self.process_foreingkeys()

    def process_foreingkeys(self):
        """First loop is for all Entitys which changed
        second loop for checking if the changed Entity is related to an Entity which not changed
        Then all fk were fetched
        After that the check is if this Entity is the same which has the foreinkey in the Config file
        If so it is applyed
        If not the other Entity has to be the Entity which has the Foreignky so it apllys
        If BothcChange the Entity it self or the Related Entity one em will be removed so per change the apply method will be call one time"""
        removed = set()
        for element in self.changed:
            if not element in removed:
                for entitys in self.config.keys():
                    for k, v in self.config.get(entitys).get("fields").items():
                        if v.is_generated() and v.is_foreignkey():
                            value: ReferenceFieldType = v
                            fk_entity = value.config_entity
                            if fk_entity == element:
                                if entitys in self.changed:
                                    removed.add(entitys)
                                result_set = value.apply(
                                    entitys, k, self.end_result)
                                if result_set is not None and result_set:
                                    self.end_result[entitys][-1].update(result_set)
                            else:
                                removed.add(fk_entity)
                                result_set = {
                                    k: v.apply(element, k, self.end_result)
                                    for k, v in self.config.get(element)
                                    .get("fields")
                                    .items()
                                    if v.is_generated() and v.is_foreignkey()
                                }
                                if result_set is not None and result_set:
                                    self.end_result[element][-1].update(
                                        result_set.get(k)
                                    )
            else:
                continue

    def get_result(self):
        return self.end_result


if __name__ == "__main__":
    """
    Entititys:
    * Cycle
    * MaterialEquipped
    * ToolEquipped

    Cycle:
    id -> cycle_number

    MaterialEquipped:
    id -> Auto generated
    material_name -> material_name

    ToolEquipped:
    id -> Auto generated
    tool_name -> tool_name

    """

    def intiStates():
        material_id = 1
        tool_id = 1
        cycle_id = 1

        states = []

        random.seed(1)

        for t in range(0, 1000):
            if random.uniform(0.0, 100.0) < 1.0:
                material_id = (material_id + 1) % 3
            current_material = f"Material {material_id}"

            if random.uniform(0.0, 100.0) < 1.0:
                tool_id += 1
            current_tool = f"Tool {material_id}"

            if random.uniform(0.0, 100.0) < 10.0:
                cycle_id += 1
            current_cycle_number = cycle_id

            state = {
                "timestamp": t,
                "material_name": current_material,
                "material_type": current_material,
                "tool_name": current_tool,
                "cycle_number": cycle_id,
            }

            # print(f"Aktueller Zustand: {state}")

            states.append(state)
        return states

    states = intiStates()
    # print(states,flush=True)
    # states = [{'timestamp': 0, 'material_name': 'Material 1','material_type': 'Material 1','tool_name': 'Tool 1', 'cycle_number': 1},
    #           {'timestamp': 1, 'material_name': 'Material 1', 'material_type': 'Material 1','tool_name': 'Tool 1', 'cycle_number': 1},
    #           {'timestamp': 2, 'material_name': 'Material 2', 'material_type': 'Material 2','tool_name': 'Tool 1', 'cycle_number': 2},
    #           {'timestamp': 3, 'material_name': 'Material 2', 'material_type': 'Material 2','tool_name': 'Tool 1', 'cycle_number': 3},
    #           {'timestamp': 4, 'material_name': 'Material 2','material_type': 'Material 2', 'tool_name': 'Tool 1', 'cycle_number': 4}]

    # Parameter: state -> scalar
    # AutoGenerated: state -> scalar

    # Reference:
    # AutoGeneratedOneToOne: state, history -> scalar

    # Tool (toolparameter) <- ToolEquipped (start, ende, tool)

    # Tool ID | Tool Name | Zykluszahl
    # 1 | W1 | 1
    # 1 | W1 | 2
    # 1 | W1 | 3
    # -> (Tool ID, Tool Name) = fix, Zyklszahjl = Änderbar

    # Materialfarbe | Materialtyp | länge
    # Rot | Baumwolle | 3m
    # Blau | Baumwolle | 3m
    # -> (Materialfarbe, Materialtyp) = fix, Länge = Änderbar


    # Cycle | Material
    # 1     | Material 1
    # 1     | Material 2
    def get_config() -> dict:
        return {
            "Cycle": {
                "fields": {
                    "id": Parameter("cycle_number"),
                    "uuid": AutoGeneratedUUID(),
                    "material_equipped": AutoGeneratedManyToOne(
                        "MaterialEquipped", "id"
                    ),
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
    migrate()
    delete_all_rows()
    processor = StateProcessor(get_config())
    processor.init_context(states[0])
    for state in states:
        processor.process_state(state)
    end_result = processor.get_result()

    # result_dict = {}
    # #
    # for k,v in config.get("ToolEquipped").items():
    #     v: FieldType
    #
    #     if v.is_generated():
    #         print(f"Is generated, skipping {k} for equality testing...")
    #
    #     res = v.apply(current_state)
    #
    #     result_dict.update({k: res})
    #
    # print(f"Result Dict: {result_dict}")
    #
    # for entity in config.keys():
    #     result_dict_2 = {k: v.apply(current_state) for k,v in config.get(entity).items() if not v.is_generated()}
    #
    #     print(f"Entity: {entity}")
    #     print(f"Result Dict 2: {result_dict_2}")

    result = {
        "Cycle": [
            {"cycle_id": 1, "start": 0, "end": 10},
            {"cycle_id": 2, "start": 11, "end": 25},
            # ...
        ],
        "ToolEquipped": [
            # ...
        ],
        "MaterialEquipped": [
            {"id": 1, "material_name": "Material 1", "start": 0, "end": 43}
            # ...
        ],
    }
    tool_dict: dict = {'id': 11, 'uuid': '8ae9a1b0-35b6-4c3a-aaa1-62672bf0aedc', 'tool_name': 'Tool 2', 'start': 835, 'end': 999}
    material_dict: dict = {'id': 11, 'uuid': '6887ea4e-9592-4853-b374-182d72c1fc54', 'material_name': 'Material 2', 'material_typ': 'Material 2', 'start': 835, 'end': 999}

    assert end_result["Cycle"][0] == {
        'id': 1, 'uuid': 'b1cb1816-7d85-457c-9f32-2a1422d00e17', 'start': 0, 'end': 1, 'material_equipped': 1}

    assert end_result["Cycle"][-1] == {'id': 114, 'uuid': '404e8909-1d5e-45d0-b0a8-734387bb1956', 'start': 993, 'end': 999, 'material_equipped': 11}

    assert end_result["ToolEquipped"][-1] == tool_dict
    assert end_result["MaterialEquipped"][-1] == material_dict


