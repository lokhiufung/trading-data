from sqlalchemy import Column, Integer, String, UniqueConstraint

from trading_data.timescaledb.models.base import Base


class Product(Base):
    __tablename__ = 'product'
    id = Column(Integer, primary_key=True)
    # the name of product does not neccessarily unique
    name = Column(String)
    # # TODO
    # product_type = Column(String)
    # # Add a unique constraint for the (name, product_type) combination
    # __table_args__ = (UniqueConstraint('name', 'product_type', name='_name_product_type_uc'),)