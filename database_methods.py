from json import loads

from sqlalchemy import Column, String, BigInteger, JSON, and_
from sqlalchemy import create_engine
from sqlalchemy.engine import row, Engine, Connection
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()


class PqlEntity(Base):
    __tablename__ = "entitys"
    id = Column(String(36), primary_key=True, unique=True)
    name = Column(String(256))
    start = Column(BigInteger)
    end = Column(BigInteger)
    value = Column(JSON)

    def __repr__(self):
        return "<Entity(id='%s', name='%s', start='%s', end='%s', value='%s')>" % (
            self.id,
            self.name,
            self.start,
            self.end,
            self.value,
        )


class ExtractCounter(Base):
    __tablename__ = "extract_counter"
    name = Column(String(36), primary_key=True, unique=True)
    counter_value = Column(BigInteger)


def select_counter_from_name(name: str):
    sql_str: str = f"SELECT counter_value FROM extract_counter WHERE name ='{name}';"
    session, connection, engine = connect_to_db()
    result = session.execute(sql_str)
    names: [dict] = [row[0] for row in result]
    # if nix drin dann leeres array names: [dict] = [row[0] for row in result]
    return names


def update_counter_by_name(name: str, counter: int):
    from sqlalchemy import update

    upd = update(ExtractCounter)
    val = upd.values({"counter_value": counter})
    cond = val.where(ExtractCounter.name == name)

    session, connection, engine = connect_to_db()
    engine.execute(cond)
    session.commit()
    session.close()
    connection.close()


def insert_save_extract(entity: ExtractCounter):
    session, connection, engine = connect_to_db()
    try:
        session.add(entity)
        session.commit()
        session.close()
        connection.close()
    except KeyboardInterrupt:
        session.close()
        connection.close()


def connect_to_db():
    engine: Engine = create_engine("sqlite:///sqlite.db", echo=False)
    connection: Connection = engine.connect()
    Session = sessionmaker(bind=engine)
    session: Session = Session()
    return session, connection, engine


def migrate():
    session, connection, engine = connect_to_db()
    Base.metadata.create_all(engine)
    session.close()
    connection.close()


def fetch_data_from_query(sql_query: str):
    session, connection, engine = connect_to_db()
    with engine.connect().execution_options(autocommit=True) as conn:
        fetched_data = conn.execute(sql_query)
    session.close()
    return fetched_data


def delete_all_rows_from_pql_entity():
    fetch_data_from_query("DELETE FROM entitys WHERE TRUE;")
    # delete_all_rows_from_counter_saving()


def delete_all_rows_from_counter_saving():
    fetch_data_from_query("DELETE FROM extract_counter WHERE TRUE;")


def insert_entity(insert_entity: PqlEntity):
    session, connection, engine = connect_to_db()
    try:
        session.add(insert_entity)
        session.commit()
        session.close()
        connection.close()
    except KeyboardInterrupt:
        session.close()
        connection.close()


def update_end_of_entity_with_id(id, end: int):
    session, connection, engine = connect_to_db()
    session.query(PqlEntity).filter(PqlEntity.id == id).update(
        {PqlEntity.end: end}, synchronize_session=False
    )
    session.commit()
    session.close()
    connection.close()


def update_end_of_entity_and_in_json_with_id(id: str, updated_dict: dict):
    session, connection, engine = connect_to_db()
    session.query(PqlEntity).filter(PqlEntity.id == id).update(
        {PqlEntity.end: updated_dict.get("end")}, synchronize_session=False
    )
    session.query(PqlEntity).filter(PqlEntity.id == id).update(
        {PqlEntity.value: updated_dict}, synchronize_session=False
    )
    session.commit()
    session.close()
    connection.close()


def get_entity_with_name(name: str):
    session, connection, engine = connect_to_db()
    list_entitys: [PqlEntity] = session.query(PqlEntity).filter_by(name=name).all()
    session.close()
    connection.close()
    return list_entitys


def get_entity_with_name_and_predicate(value_filter: dict):
    ### property geht nicht. es muss ein value sein. hier dann switchcase oder so
    session, connection, engine = connect_to_db()
    list_entitys: [row] = (
        session.query(PqlEntity.value).filter_by(and_(value_filter)).all()
    )
    session.close()
    connection.close()
    return list_entitys


def get_all_entitys():
    session, connection, engine = connect_to_db()
    list_entitys: [PqlEntity] = session.query(PqlEntity).all()
    session.close()
    connection.close()
    return list_entitys


def get_all_names():
    session, connection, engine = connect_to_db()
    list_entitys: [row] = session.query(PqlEntity.name).distinct().all()
    session.close()
    connection.close()
    return list_entitys


def get_values_from_name(name: str):
    session, connection, engine = connect_to_db()
    list_entitys: [row] = session.query(PqlEntity.value).filter_by(name=name).all()
    session.close()
    connection.close()
    return list_entitys


def get_values_from_all_entitys_as_dict(name: str):
    dict_array: [dict] = []
    el: row
    for el in get_values_from_name(name):
        dict_array.append(el._mapping.get("value"))
    return dict_array


def execute_raw_sql_query(name: str, value_json, groupBy: str):
    session, connection, engine = connect_to_db()
    # result = session.execute(f"SELECT value FROM entitys WHERE name = '{name}' AND {value}")
    # result=session.execute(f"SELECT * FROM entitys WHERE name = 'Cycle' AND json_extract(value, '$.start') = 0 {groupBy}")
    result = session.execute(
        f"SELECT json_extract(value, '$')  FROM entitys WHERE entitys.name ='{name}' AND {value_json} {groupBy}"
    )
    # session.query(PqlEntity.value).filter(PqlEntity.value['start'] == '129', name == 'Cycle').all()
    # session.query(PqlEntity.value).filter(and_(PqlEntity.name == "Cycle", PqlEntity.start == 129)).all()
    # session.query(PqlEntity.value).filter(and_(PqlEntity.name == "Cycle", PqlEntity.value["start"] == "993")).all()
    # ({'id': 114, 'uuid': '404e8909-1d5e-45d0-b0a8-734387bb1956', 'start': 993, 'end': 999, 'material_equipped': 11},)
    # session.query(PqlEntity).filter_by(PqlEntity.name == "Cycle", PqlEntity.value['id'] == "5").all()
    names: [dict] = [loads(row[0]) for row in result]
    session.close()
    connection.close()
    return names
