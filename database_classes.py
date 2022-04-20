import json

from sqlalchemy import Column, Integer, String, BigInteger, JSON
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import sessionmaker, declarative_base


Base = declarative_base()

class PqlEntity(Base):
    __tablename__ = 'entitys'
    id = Column(String(36), primary_key=True,unique=True)
    number = Column(Integer)
    start = Column(BigInteger)
    end = Column(BigInteger)
    name = Column(String)
    value= Column(String)

    def __repr__(self):
        return "<Entity(id='%s',number='%s', start='%s', end='%s', name='%s', value='%s')>" % (
            self.id,self.number,self.start,self.end,self.name,self.value)

def connect_to_db():
    engine = create_engine('sqlite:///sqlite.db', echo=True)
    connection = engine.connect()
    return connection , engine

def get_session():
    connection ,engine = connect_to_db()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session, connection, engine

def migrate():
    session ,connection, engine = get_session()
    Base.metadata.create_all(engine)
    session.close()

def fetch_data_from_query(sql_query:str):
    session ,connection, engine = get_session()
    with engine.connect().execution_options(autocommit=True) as conn:
        fetched_data=conn.execute(sql_query)
       # print(fetched_data)
    session.close()
    return fetched_data

def delete_all_rows():
    fetch_data_from_query("delete  from entitys where TRUE;")


def insert_entity(insert_entity:PqlEntity):
# User1 = User(username='mayersimm',email='mayersim@hs-albsig.de')
    session,connection,engine = get_session()
    session.add(insert_entity)
    session.commit()
    session.close()
#
#migrate()
# entity1 = PqlEntity(id=2,start=1,end=3,name="Cycle",value= f{
#     "Cycle": [{"id": 21, "start": 1, "end": 2}, {"id": 22, "start": 3, "end": 5}]})
# insert_entity(entity1)

def update_end_of_entity_with_id(id,end:int):
    session,connection, engine = get_session()
    session.query(PqlEntity).filter(PqlEntity.id == id).update({PqlEntity.end:end}, synchronize_session = False)
    session.commit()
    session.close()

def update_end_of_entity_and_in_json_with_id(id, end:int, updated_dict):
    session,connection, engine = get_session()
    session.query(PqlEntity).filter(PqlEntity.id == id).update({PqlEntity.end:end}, synchronize_session = False)
    session.query(PqlEntity).filter(PqlEntity.id == id).update({PqlEntity.value:updated_dict}, synchronize_session = False)
    session.commit()
    session.close()

# update_end_of_entity_with_id(1,4)
def get_all_entitys():
    session,connection, engine = get_session()
    list_enttiys:[PqlEntity] = session.query(PqlEntity).all()
    session.close()
    return list_enttiys

# list_of_entitys:[PqlEntity] = get_all_entitys()
# for el in list_of_entitys:
#     value_dict:dict = json.loads(el.value)
#     print(value_dict)
