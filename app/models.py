from sqlmodel import Field, Relationship, SQLModel

# un modelo es una clase que representa datos de la tabla
# debe crearse un modelo para cada tabla

class League(SQLModel, table = True):
    id: int | None = Field(default = None, primary_key = True)
    name: str
    code: str # identificador que usa football-data.co.uk (LaLiga -> SP1, Premier League -> E0, etc.)
    country: str
    level: int

class Team(SQLModel, table = True):
    id: int | None = Field(default = None, primary_key = True) # uso Field para establecer el primary key de la tabla, aunque también permite asignar el valor por defecto
    name: str = Field(unique = True)
    country: str

class Match(SQLModel, table = True):
    id: int | None = Field(default = None, primary_key = True) # puedo crear un Match sin especificar el id y este se establecerá automáticamente al guardar los datos en la base de datos
    league_id: int = Field(foreign_key = "league.id")
    season: str
    date: str
    home_team_id: int = Field(foreign_key = "team.id")
    away_team_id: int = Field(foreign_key = "team.id")
    home_goals: int
    away_goals: int

    home_team: Team = Relationship(sa_relationship_kwargs = {"foreign_keys": "Match.home_team_id"}) # las relaciones permiten hacer Match.home_team o Match.away_team aunque no sean columnas
    away_team: Team = Relationship(sa_relationship_kwargs = {"foreign_keys": "Match.away_team_id"})
    league: League = Relationship(sa_relationship_kwargs = {"foreign_keys": "Match.league_id"})

class Standings(SQLModel, table = True):
    id: int | None = Field(default = None, primary_key = True)
    league_id: int = Field(foreign_key = "league.id")
    season: str
    position: int
    team_id: int = Field(foreign_key = "team.id")
    points: int
    matches_played: int
    wins: int
    draws: int
    losses: int
    goals_for: int
    goals_against: int
    goal_difference: int

    league: League = Relationship(sa_relationship_kwargs = {"foreign_keys": "Standings.league_id"})
    team: Team = Relationship(sa_relationship_kwargs = {"foreign_keys": "Standings.team_id"})