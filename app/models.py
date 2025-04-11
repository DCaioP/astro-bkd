from app import db

class Aggregate_data(db.Model):
    __tablename__ = 'aggregate_data'
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False)
    ho = db.Column(db.Float, nullable=False)
    ho_acumulado = db.Column(db.Float, nullable=False)
    ho_meta_diaria = db.Column(db.Float, nullable=False)
    ho_meta_diaria_acumulado = db.Column(db.Float, nullable=False)
