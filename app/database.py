from sqlmodel import create_engine, SQLModel

sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

engine = create_engine(sqlite_url) # el engine es un objeto que gestiona la comunicación con la base de datos
# echo = True hace que el engine imprima todos los comandos SQL que ejecuta

def create_db_and_tables(): # crea el archivo database.db
    SQLModel.metadata.create_all(engine) # la clase SQLModel tiene un atributo metadata que es una instancia de la clase MetaData y que contiene información de todas las clases que se heredan de SQLModel (Match, Team)
    # esta instancia de MetaData tiene un método create_all() que toma un engine y crea una base de datos con todas las tablas registradas (en este caso, solo la tabla Match) 