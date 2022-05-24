from sqlalchemy import Column, String, BigInteger, JSON
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base


Base = declarative_base()


class PqlEntity(Base):
    __tablename__ = "entitys"
    id = Column(String(36), primary_key=True, unique=True)
    start = Column(BigInteger)
    end = Column(BigInteger)
    value = Column(JSON)

    def __repr__(self):
        return "<Entity(id='%s', start='%s', end='%s', value='%s')>" % (
            self.id,
            self.start,
            self.end,
            self.value,
        )


def connect_to_db():
    engine = create_engine("sqlite:///sqlite.db", echo=True)
    connection = engine.connect()
    return connection, engine


def get_session():
    connection, engine = connect_to_db()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session, connection, engine


def migrate():
    session, connection, engine = get_session()
    Base.metadata.create_all(engine)
    session.close()


def fetch_data_from_query(sql_query: str):
    session, connection, engine = get_session()
    with engine.connect().execution_options(autocommit=True) as conn:
        fetched_data = conn.execute(sql_query)
    session.close()
    return fetched_data


def delete_all_rows():
    fetch_data_from_query("delete  from entitys where TRUE;")


def insert_entity(insert_entity: PqlEntity):
    session, connection, engine = get_session()
    session.add(insert_entity)
    session.commit()
    session.close()


def update_end_of_entity_with_id(id, end: int):
    session, connection, engine = get_session()
    session.query(PqlEntity).filter(PqlEntity.id == id).update(
        {PqlEntity.end: end}, synchronize_session=False
    )
    session.commit()
    session.close()


def update_end_of_entity_and_in_json_with_id(id, end: int, updated_dict):
    session, connection, engine = get_session()
    session.query(PqlEntity).filter(PqlEntity.id == id).update(
        {PqlEntity.end: end}, synchronize_session=False
    )
    session.query(PqlEntity).filter(PqlEntity.id == id).update(
        {PqlEntity.value: updated_dict}, synchronize_session=False
    )
    session.commit()
    session.close()


def get_all_entitys():
    session, connection, engine = get_session()
    list_enttiys: [PqlEntity] = session.query(PqlEntity).all()
    session.close()
    return list_enttiys
