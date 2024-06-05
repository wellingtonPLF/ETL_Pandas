from geoalchemy2 import Geometry
from sqlalchemy import TIMESTAMP, BigInteger, Boolean, DateTime, Float, String, Text, create_engine, Column, Integer, CHAR, VARCHAR, Numeric, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
schema = "public"

class MyTable(Base):
    __tablename__ = 'myTableName'
    __table_args__ = {'schema': schema}
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    integerColumn = Column(Integer)
    stringColumn = Column(VARCHAR(80))
    dateColumn = Column(Date, nullable=True)
    geomColumn = Column(Geometry('MULTIPOLYGON', srid=4674), nullable=True)
    floatColumn = Column(Numeric)
    textoColumn = Column(Text)
    boolColumn = Column(Boolean)
    date_timeColumn = Column(DateTime)

# ==============================================================================================================================
