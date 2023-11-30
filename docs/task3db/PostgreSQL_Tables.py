# Example using SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, ...

engine = create_engine('postgresql://username:password@localhost:5432/your_database_name')

# Define your table schema
class MLFeatures(Base):
    __tablename__ = 'ml_features'
    id = Column(Integer, primary_key=True)
    feature_name = Column(String)
    feature_value = Column(Float)
    # Add other columns as needed

# Create tables
Base.metadata.create_all(engine)
