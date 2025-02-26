from sqlalchemy import Column, Integer, Float, ForeignKey, DateTime, String
from sqlalchemy.orm import relationship

from trading_data.timescaledb.models.base import Base


class OhlcvBar(Base):
    __tablename__ = 'ohlcv_bar'
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float)
    bar_type = Column(String, nullable=False)
    product_id = Column(Integer, ForeignKey('product.id'))
    data_source_id = Column(Integer, ForeignKey('data_source.id'))
    product = relationship("Product")
    data_source = relationship("DataSource")