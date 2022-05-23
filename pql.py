import uuid
from typing import Iterable, Callable, List, Dict


class Context:
    def __init__(self):
        super().__init__()

    def create_query_context(self, entity_type):
        return EntityContext(self, entity_type)

    def list_entity(self, entity_name: str, where_clause=None, group_by_clause=None):
        pass

    def get_aggregate_function(self, name: str):
        pass


class RootContext(Context):
    def __init__(self, entity_list_resolver, aggregate_functions):
        super().__init__()
        self.entity_list_resolver = entity_list_resolver
        self.aggregate_functions = aggregate_functions

    def list_entity(self, entity_name: str, where_clause=None, group_by_clause=None):
        return self.entity_list_resolver(
            entity_name, where_clause=where_clause, group_by_clause=group_by_clause
        )

    def get_aggregate_function(self, name: str):
        return self.aggregate_functions(name)


class ChildContext(Context):
    def __init__(self, parent: Context = None):
        super().__init__()
        self.parent: Context = parent

    def list_entity(self, entity_name: str, where_clause=None, group_by_clause=None):
        return self.parent.list_entity(entity_name, where_clause, group_by_clause)

    def get_aggregate_function(self, name: str):
        return self.parent.get_aggregate_function(name)


class EntityContext(ChildContext):
    def __init__(self, parent: Context, entity):
        super().__init__(parent)
        self.entity = entity


class AggFunction:
    def execute(self, result):
        pass


class SelectEntry:
    def __init__(self, name: str):
        self.name: str = name

    def execute(self, context: Context):
        pass


class Projection(SelectEntry):
    def __init__(self, field: str, name=None):
        super().__init__(name or field)
        self.field: str = field

    def execute(self, context: Context):
        entity = context.entity

        if isinstance(entity, Dict):
            if self.field == "*":
                # return entity
                return None

            if not self.field in entity:
                print(f"Missing {self.field}?")

            return entity.get(self.field)
        elif isinstance(entity, List):
            return self.execute(EntityContext(context, entity[0]))
        else:
            raise Exception("...")


class SubQuery(SelectEntry):
    def __init__(self, query: "Query", name: str):
        super().__init__(name)
        self.query: Query = query

    def execute(self, context: Context):
        entity = context.entity

        if isinstance(entity, Dict):
            self.query.where_clause = lambda o: entity.get("start") <= o.get(
                "start"
            ) and o.get("start") < entity.get("end")
            return self.query.execute(context)
        else:
            raise NotImplementedError("")


class Aggregation(SelectEntry):
    def __init__(self, agg_function_name: str, query: "Query", name: str = None):
        super().__init__(name or "agg")
        self.agg_function_name: str = agg_function_name
        self.query: Query = query

    def execute(self, context: Context):
        entity = context.entity
        if isinstance(entity, Dict):
            self.query.where_clause = lambda o: entity.get("start") <= o.get(
                "start"
            ) and o.get("start") < entity.get("end")
            result = self.query.execute(context)
            agg_function: AggFunction = context.get_aggregate_function(self.agg_function_name)
            return agg_function.execute(result)
        elif isinstance(entity, List):
            # Do what we want to do on all subcontexts
            results = []
            for e in entity:
                self.query.where_clause = lambda o: e.get("start") <= o.get(
                    "start"
                ) and o.get("start") < e.get("end")
                result = self.query.execute(context)
                results.extend(result)
            agg_function: AggFunction = context.get_aggregate_function(self.agg_function_name)
            return agg_function.execute(results)


class Predicate(object):

    def check(self, entity: dict) -> bool:
        raise NotImplementedError()


class EqPredicate(Predicate):

    def __init__(self, property:str, value):
        self.property: str = property
        self.value = value

    def check(self, entity: dict) -> bool:
        if self.property in entity:
            if type(self.value) == uuid:
                return str(entity.get(self.property)) == str(self.value)
            else:
                return entity.get(self.property) == self.value
        else:
            raise ValueError(f"Trying predicate check on {entity} field {self.property} but does not exist")


class GreaterPredicate(Predicate):

    def __init__(self, property:str, value):
        self.property: str = property
        self.value = value

    def check(self, entity: dict) -> bool:
        if self.property in entity:
            if type(self.value) == uuid:
                return str(entity.get(self.property)) > str(self.value)
            else:
                return entity.get(self.property) > self.value
        else:
            raise ValueError(f"Trying predicate check on {entity} field {self.property} but does not exist")

class GreaterEqPredicate(Predicate):

    def __init__(self, property:str, value):
        self.property: str = property
        self.value = value

    def check(self, entity: dict) -> bool:
        if self.property in entity:
            if type(self.value) == uuid:
                return str(entity.get(self.property)) >= str(self.value)
            else:
                return entity.get(self.property) >= self.value
        else:
            raise ValueError(f"Trying predicate check on {entity} field {self.property} but does not exist")


class LowerPredicate(Predicate):

    def __init__(self, property:str, value):
        self.property: str = property
        self.value = value

    def check(self, entity: dict) -> bool:
        if self.property in entity:
            if type(self.value) == uuid:
                return str(entity.get(self.property)) < str(self.value)
            else:
                return entity.get(self.property) < self.value
        else:
            raise ValueError(f"Trying predicate check on {entity} field {self.property} but does not exist")


class LowerEqPredicate(Predicate):

    def __init__(self, property: str, value):
        self.property: str = property
        self.value = value

    def check(self, entity: dict) -> bool:
        if self.property in entity:
            if type(self.value) == uuid:
                return str(entity.get(self.property)) <= str(self.value)
            else:
                return entity.get(self.property) <= self.value
        else:
            raise ValueError(f"Trying predicate check on {entity} field {self.property} but does not exist")


class Query:
    def __init__(
        self,
        selects: List[SelectEntry],
        entity_type: str,
        where_clause:Predicate = None,
        group_by_clause: List[Projection] = None,
    ):
        self.selects: List[SelectEntry] = selects
        self.entity: str = entity_type
        self.where_clause = where_clause
        self.group_by_clause = group_by_clause

    def execute(self, context: Context) -> List[Dict]:
        objects = context.list_entity(
            self.entity, self.where_clause, self.group_by_clause
        )

        results = []
        for o in objects:
            ctx = context.create_query_context(o)
            single_result:dict = dict([(s.name, s.execute(ctx)) for s in self.selects])
            results.append(single_result)

        return results


class CountFunction(AggFunction):
    def execute(self, result):
        return len(result)


class Flatten(AggFunction):
    def execute(self, result):
        """
        We assume here that we only have one key
        """
        if len(result) > 0 and len(list(result)[0].keys()) == 1:
            return [res.get(list(res.keys())[0]) for res in result]
        else:
            return result


agg_functions: dict = {"count": CountFunction(), "flatten": Flatten()}
