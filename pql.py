from typing import Iterable, Callable, List, Dict


class Context:

    def __init__(self):
        super().__init__()

    def create_query_context(self, entity_type):
        return EntityContext(self, entity_type)

    def list_entity(self, entity_name: str):
        pass

    def get_aggregate_function(self, name: str):
        pass


class RootContext(Context):

    def __init__(self, entity_list_resolver, aggregate_functions):
        super().__init__()
        self.entity_list_resolver = entity_list_resolver
        self.aggregate_functions = aggregate_functions

    def list_entity(self, entity_name: str):
        return self.entity_list_resolver(entity_name)

    def get_aggregate_function(self, name: str):
        return self.aggregate_functions(name)


class ChildContext(Context):

    def __init__(self, parent: Context = None):
        super().__init__()
        self.parent = parent

    def list_entity(self, entity_name: str):
        return self.parent.list_entity(entity_name)

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
        self.name = name

    def execute(self, context: Context):
        pass


class Projection(SelectEntry):

    def __init__(self, field, name=None):
        super().__init__(name or field)
        self.field = field

    def execute(self, context: Context):
        entity = context.entity

        if self.field == "*":
            return None

        if not self.field in entity:
            print(f"Missing {self.field}?")

        return entity.get(self.field)


class SubQuery(SelectEntry):

    def __init__(self, query: "Query", name: str):
        super().__init__(name)
        self.query = query

    def execute(self, context: Context):
        entity = context.entity
        self.query.where_clause = lambda o: entity.get("start") <= o.get("start") and o.get("start") < entity.get("end")
        return self.query.execute(context)


class Aggregation(SelectEntry):

    def __init__(self, agg_function_name: str, query: "Query", name: str = None):
        super().__init__(name or "agg")
        self.agg_function_name = agg_function_name
        self.query = query

    def execute(self, context: Context):
        entity = context.entity
        self.query.where_clause = lambda o: entity.get("start") <= o.get("start") and o.get("start") < entity.get("end")
        result = self.query.execute(context)
        agg_function = context.get_aggregate_function(self.agg_function_name)
        return agg_function.execute(result)


class Query:

    def __init__(self, selects: List[SelectEntry], entity: str, where_clause=None):
        self.selects = selects
        self.entity = entity
        self.where_clause = where_clause

    def execute(self, context: Context) -> Iterable[Dict]:
        if not self.where_clause:
            objects = context.list_entity(self.entity)
        elif isinstance(self.where_clause, Callable):
            objects = [o for o in context.list_entity(self.entity) if self.where_clause(o)]
        else:
            raise Exception("")

        results = []
        for o in objects:
            ctx = context.create_query_context(o)
            single_result = dict([(s.name, s.execute(ctx)) for s in self.selects])

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

agg_functions = {
    "count": CountFunction(),
    "flatten": Flatten()
}
