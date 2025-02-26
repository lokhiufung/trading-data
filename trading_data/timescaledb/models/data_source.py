from sqlalchemy import Column, Integer, String

from trading_data.timescaledb.models.base import Base


class DataSource(Base):
    __tablename__ = 'data_source'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)