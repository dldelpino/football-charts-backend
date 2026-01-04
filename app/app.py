# ejecutar este archivo crea la base de datos en database.db
# tarda en ejecutarse un par de minutos

import pandas as pd
from collections import defaultdict
from sqlmodel import Session, select

if __name__ == "__main__":
    from database import create_db_and_tables, engine
    from models import League, Match, Standings, Team
else:
    from app.database import create_db_and_tables, engine
    from app.models import League, Match, Standings, Team

# una sesión es un objeto que realiza un grupo de operaciones en una base de datos
# cada vez que quiera añadir datos a la base de datos, tengo que crear una nueva sesión utilizando el engine
# la sesión debe cerrarse una vez los datos hayan sido añadidos
# para cerrar la sesión, puedo usar session.close() o puedo realizar las operaciones dentro de un with

seasons = []
for i in range(25):
    if i < 9:
        seasons.append(f"0{i}/0{i+1}")
    elif i == 9:
        seasons.append(f"09/10")
    else:
        seasons.append(f"{i}/{i+1}")

fixed_names = {
    "Alaves": "Alavés", # España
    "Alcorcon": "Alcorcón",
    "Almeria": "Almería",
    "Ath Bilbao": "Athletic Bilbao",
    "Ath Bilbao B": "Bilbao Athletic",
    "Ath Madrid": "Atlético Madrid",
    "Barcelona B": "Barça Atlètic",
    "Cadiz": "Cádiz",
    "Castellon": "Castellón",
    "Cordoba": "Córdoba",
    "Espanol": "Espanyol",
    "Extremadura": "CF Extremadura",
    "Ferrol": "Racing Ferrol",
    "Gimnastic": "Gimnàstic",
    "Hercules": "Hércules",
    "Jaen": "Real Jaén",
    "La Coruna": "Deportivo",
    "Leganes": "Leganés",
    "Leonesa": "Cultural Leonesa",
    "Logrones": "Logroñés",
    "Lorca": "Lorca Deportiva",
    "Malaga": "Málaga",
    "Malaga B": "Atlético Malagueño",
    "Mirandes": "Mirandés",
    "Real Madrid B": "Real Madrid Castilla",
    "Real Union": "Real Unión",
    "Reus Deportiu": "Reus",
    "Santander": "Racing Santander",
    "Sevilla B": "Sevilla Atlético",
    "Sociedad": "Real Sociedad",
    "Sociedad B": "Real Sociedad B",
    "Sp Gijon": "Sporting Gijón",
    "U.Las Palmas": "Universidad Las Palmas",
    "Vallecano": "Rayo Vallecano",
    "Man City": "Manchester City", # Inglaterra
    "Man United": "Manchester United",
    "Nott'm Forest": "Nottingham Forest",
    "Sheffield United": "Sheffield",
    "Spal": "SPAL", # Italia
    "Bayern Munich": "Bayern München", # Alemania
    "Bielefeld": "Arminia",
    "Braunschweig": "Eintracht Braunschweig",
    "Cottbus": "Energie Cottbus",
    "Ein Frankfurt": "Eintracht Frankfurt",
    "FC Koln": "Köln",
    "Fortuna Dusseldorf": "Fortuna Düsseldorf",
    "Greuther Furth": "Greuther Fürth",
    "Hamburg": "Hamburger",
    "Karlsruhe": "Karlsruher",
    "M'gladbach": "Mönchengladbach",
    "Munich 1860": "1860 München",
    "Nurnberg": "Nürnberg",
    "Schalke 04": "Schalke",
    "St Pauli": "St. Pauli",
    "Ajaccio GFCO": "Gazélec Ajaccio", # Francia
    "Arles": "Arles-Avignon",
    "Brest": "Stade Brestois",
    "Evian Thonon Gaillard": "Evian",
    "Lyon": "Olympique Lyonnais",
    "Nimes": "Nîmes",
    "Paris SG": "PSG",
    "Rennes": "Stade Rennais",
    "St Etienne": "Saint-Étienne",
}

def load_dataframe(league, season): # ejemplo: league = SP1, season = 2425
    url = f"https://www.football-data.co.uk/mmz4281/{season}/{league}.csv"
    df = pd.read_csv(url, usecols = ["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"], on_bad_lines = "warn", encoding = "utf-8")
    df = df.dropna() # a veces aparecen filas vacías
    df["FTHG"] = df["FTHG"].astype(int) # a veces los resultados se muestran con ceros decimales
    df["FTAG"] = df["FTAG"].astype(int)
    df.insert(1, "Season", season)
    return df

def head_to_head(teams, season): # teams es una lista con ID de equipos
    stats = {team: {"points": 0, "goal_difference": 0} for team in teams}
    with Session(engine) as session:
        matches = session.exec(
            select(Match).where(Match.season == season, Match.home_team_id.in_(teams), Match.away_team_id.in_(teams))
        ).all()
        for match in matches:
            home_team = match.home_team_id
            away_team = match.away_team_id
            stats[home_team]["goal_difference"] += match.home_goals - match.away_goals
            stats[away_team]["goal_difference"] += match.away_goals - match.home_goals
            if match.home_goals > match.away_goals:
                stats[home_team]["points"] += 3
            elif match.home_goals < match.away_goals:
                stats[away_team]["points"] += 3
            else:
                stats[home_team]["points"] += 1
                stats[away_team]["points"] += 1
    return stats

def tie_breaker(standings, season): # standings es una lista con elementos de la forma (team.id, {"points": 0, "matches_played": 0, ...})
    i = 0
    result = []
    while i < len(standings):
        tied_teams = [standings[i]]
        j = i+1
        while j < len(standings) and standings[i][1]["points"] == standings[j][1]["points"]:
            tied_teams.append(standings[j])
            j += 1
        if len(tied_teams) > 1: 
            h2h = head_to_head([x[0] for x in tied_teams], season)
            tied_teams = sorted(
                tied_teams,
                key = lambda x: (h2h[x[0]]["points"], h2h[x[0]]["goal_difference"], x[1]["goal_difference"]),
                reverse = True,
            )
        result += tied_teams
        i = j
    return result

def set_status(s: Standings): # ver boot.js en el fronted para los identificadores de status
    if s.league.code == "SP1":
        if s.season == "24/25":
            if s.position <= 5:
                s.status = 0
            elif s.position in [6, 7]:
                s.status = 2
            elif s.position == 8:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season in ["23/24", "21/22"]:
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "22/23":
            if s.position in [1, 2, 3, 4, 12]:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "20/21":
            if s.position in [1, 2, 3, 4, 7]:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position >= 18:
                s.status = 7
        elif s.season in ["17/18", "18/19", "19/20"]:
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "16/17":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "15/16":
            if s.position in [1, 2, 3, 7]:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position >= 18:
                s.status = 7
        elif s.season == "14/15":
            if s.position in [1, 2, 3, 5]:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 6:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position in [13, 19, 20]:
                s.status = 7
        elif s.season == "13/14":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position in [6, 7]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "12/13":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position in [7, 9]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "11/12":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position in [6, 10]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "10/11":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "09/10":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [6, 7]:
                s.status = 3
            elif s.position == 9:
                s.status = 2
            elif s.position >= 18:
                s.status = 7
        elif s.season == "08/09":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6, 13]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "07/08":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 10]:
                s.status = 3
            elif s.position == 9:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season in "06/07":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 9]:
                s.status = 3
            elif s.position == 7:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "05/06":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 15]:
                s.status = 3
            elif s.position == 7:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "04/05":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 15]:
                s.status = 3
            elif s.position in [7, 8, 9]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "03/04":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 12]:
                s.status = 3
            elif s.position in [7, 8]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "02/03":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 9]:
                s.status = 3
            elif s.position in [15, 16]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "01/02":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position in [10, 15]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "00/01":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 17]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
    elif s.league.code == "SP2":
        if s.season in ["24/25", "23/24", "22/23", "21/22", "20/21", "19/20", "18/19", "17/18", "16/17", "15/16", "14/15"]:
            if s.position <= 2:
                s.status = 9
            elif s.position in [3, 4, 5, 6]:
                s.status = 10
            elif s.position >= 19:
                s.status = 7
        if s.season == "13/14":
            if s.position <= 2:
                s.status = 9
            elif s.position in [5, 6, 7]:
                s.status = 10
            elif s.position in [4, 20, 21, 22]:
                s.status = 7
        if s.season == "12/13":
            if s.position <= 2:
                s.status = 9
            elif s.position in [3, 4, 5, 6]:
                s.status = 10
            elif s.position in [18, 20, 21, 22]:
                s.status = 7
        if s.season == "11/12":
            if s.position <= 2:
                s.status = 9
            elif s.position in [3, 4, 5, 6]:
                s.status = 10
            elif s.position in [12, 20, 21, 22]:
                s.status = 7
        if s.season == "10/11":
            if s.position <= 2:
                s.status = 9
            elif s.position in [4, 5, 6, 7]:
                s.status = 10
            elif s.position >= 19:
                s.status = 7
        if s.season in ["09/10", "08/09", "07/08", "06/07", "05/06", "04/05", "03/04", "01/02"]:
            if s.position <= 3:
                s.status = 9
            elif s.position >= 19:
                s.status = 7
        if s.season == "02/03":
            if s.position <= 3:
                s.status = 9
            elif s.position in [9, 20, 21, 22]:
                s.status = 7
        if s.season == "00/01":
            if s.position <= 3:
                s.status = 9
            elif s.position in [16, 20, 21, 22]:
                s.status = 7
    elif s.league.code == "E0":
        if s.season == "24/25":
            if s.position in [1, 2, 3, 4, 5, 17]:
                s.status = 0
            elif s.position in [6, 7]:
                s.status = 2
            elif s.position == 12:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "23/24":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 8]:
                s.status = 2
            elif s.position == 6:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "22/23":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6, 14]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season in ["21/22", "20/21"]:
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "19/20":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 8]:
                s.status = 2
            elif s.position == 6:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season in ["18/19", "17/18"]:
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "16/17":
            if s.position in [1, 2, 3, 6]:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "15/16":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "14/15":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position in [7, 12]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "13/14":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position >= 18:
                s.status = 7
        elif s.season == "12/13":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 18:
                s.status = 11
            elif s.position in [5, 9]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "11/12":
            if s.position in [1, 2, 3, 6]:
                s.status = 0
            elif s.position == 4:
                s.status = 2
            elif s.position in [5, 8]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "10/11":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 8, 13]:
                s.status = 3
            elif s.position == 18:
                s.status = 12
            elif s.position > 18:
                s.status = 7
        elif s.season in ["09/10", "08/09"]:
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "07/08":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 8, 9, 11]:
                s.status = 3
            elif s.position == 6:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "06/07":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position == 10:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "05/06":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 9]:
                s.status = 3
            elif s.position == 7:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "04/05":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4, 5]:
                s.status = 1
            elif s.position in [6, 7]:
                s.status = 3
            elif s.position == 14:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "03/04":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 11]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "02/03":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 8, 9]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "01/02":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 10]:
                s.status = 3
            elif s.position in [8, 13]:
                s.status = 6
            elif s.position == 18:
                s.status = 12
            elif s.position > 18:
                s.status = 7
        elif s.season == "00/01":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 6]:
                s.status = 3
            elif s.position in [8, 11]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
    elif s.league.code == "I1":
        if s.season == "24/25":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 9]:
                s.status = 2
            elif s.position == 6:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "23/24":
            if s.position <= 5:
                s.status = 0
            elif s.position in [6, 7]:
                s.status = 2
            elif s.position == 8:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "22/23":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 8:
                s.status = 5
            elif s.position in [17, 18]:
                s.status = 8
            elif s.position >= 19:
                s.status = 7
        elif s.season in ["21/22", "20/21"]:
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position >= 18:
                s.status = 7
        elif s.season == "19/20":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 7]:
                s.status = 2
            elif s.position == 6:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "18/19":
            if s.position <= 4:
                s.status = 0
            elif s.position in [6, 8]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "17/18":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season in ["16/17", "15/16"]:
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5]:
                s.status = 2
            elif s.position == 6:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "14/15":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "13/14":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 4:
                s.status = 2
            elif s.position in [5, 7]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "12/13":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 7:
                s.status = 2
            elif s.position in [4, 5]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "11/12":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position in [4, 6]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season in ["10/11", "09/10"]:
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "08/09":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6, 10]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "07/08":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position == 8:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "06/07":
            if s.position in [1, 2, 4]:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position == 9:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "05/06":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "04/05":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 8]:
                s.status = 3
            elif s.position == 13:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "03/04":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 7]:
                s.status = 3
            elif s.position == 15:
                s.status = 8
            elif s.position >= 16:
                s.status = 7
        elif s.season == "02/03":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6, 8]:
                s.status = 3
            elif s.position in [9, 10]:
                s.status = 6
            elif s.position in [14, 15]:
                s.status = 8
            elif s.position >= 16:
                s.status = 7
        elif s.season == "01/02":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 10]:
                s.status = 3
            elif s.position in [7, 8, 11]:
                s.status = 6
            elif s.position >= 15:
                s.status = 7
        elif s.season == "00/01":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6, 9]:
                s.status = 3
            elif s.position == 8:
                s.status = 6
            elif s.position in [14, 15]:
                s.status = 8
            elif s.position >= 16:
                s.status = 7
    elif s.league.code == "D1":
        if s.season == "24/25":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 9]:
                s.status = 2
            elif s.position == 6:
                s.status = 5
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "23/24":
            if s.position <= 5:
                s.status = 0
            elif s.position in [6, 7]:
                s.status = 2
            elif s.position == 8:
                s.status = 5
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season in ["22/23", "20/21"]:
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "21/22":
            if s.position in [1, 2, 3, 4, 11]:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season in ["19/20", "18/19"]:
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "17/18":
            if s.position <= 4:
                s.status = 0
            elif s.position in [5, 8]:
                s.status = 2
            elif s.position == 6:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season in ["16/17", "15/16", "14/15"]:
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "13/14":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position in [6, 7]:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "12/13":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position in [6, 12]:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "11/12":
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position == 5:
                s.status = 2
            elif s.position in [6, 7]:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "10/11":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 14]:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "09/10":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 6]:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "08/09":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 10]:
                s.status = 3
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "07/08":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 10, 13]:
                s.status = 3
            elif s.position == 6:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
        elif s.season == "06/07":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 6]:
                s.status = 3
            elif s.position == 7:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
        elif s.season == "05/06":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 14]:
                s.status = 3
            elif s.position == 6:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
        elif s.season == "04/05":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 6, 11]:
                s.status = 3
            elif s.position in [7, 8, 9]:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
        elif s.season == "03/04":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5]:
                s.status = 3
            elif s.position in [6, 7, 8, 10]:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
        elif s.season == "02/03":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 14]:
                s.status = 3
            elif s.position in [6, 7, 8]:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
        elif s.season == "01/02":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 6]:
                s.status = 3
            elif s.position in [7, 8, 9]:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
        elif s.season == "00/01":
            if s.position <= 2:
                s.status = 0
            elif s.position in [3, 4]:
                s.status = 1
            elif s.position in [5, 6]:
                s.status = 3
            elif s.position in [7, 9, 11]:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
    elif s.league.code == "F1":
        if s.season in ["24/25", "23/24"]:
            if s.position <= 3:
                s.status = 0
            elif s.position == 4:
                s.status = 1
            elif s.position in [5, 6]:
                s.status = 2
            elif s.position == 7:
                s.status = 5
            elif s.position == 16:
                s.status = 8
            elif s.position >= 17:
                s.status = 7
        elif s.season == "22/23":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 13]:
                s.status = 2
            elif s.position == 5:
                s.status = 5
            elif s.position >= 17:
                s.status = 7
        elif s.season == "21/22":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 9]:
                s.status = 2
            elif s.position == 5:
                s.status = 5
            elif s.position == 18:
                s.status = 8
            elif s.position >= 19:
                s.status = 7
        elif s.season == "20/21":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5]:
                s.status = 2
            elif s.position == 6:
                s.status = 5
            elif s.position == 18:
                s.status = 8
            elif s.position >= 19:
                s.status = 7
        elif s.season == "19/20":
            if s.position <= 3:
                s.status = 0
            elif s.position in [4, 5]:
                s.status = 2
            elif s.position == 6:
                s.status = 3
            elif s.position >= 19:
                s.status = 7
        elif s.season == "18/19":
            if s.position <= 3:
                s.status = 0
            elif s.position in [4, 10]:
                s.status = 2
            elif s.position == 11:
                s.status = 3
            elif s.position == 18:
                s.status = 8
            elif s.position >= 19:
                s.status = 7
        elif s.season == "17/18":
            if s.position <= 3:
                s.status = 0
            elif s.position in [4, 5]:
                s.status = 2
            elif s.position == 6:
                s.status = 3
            elif s.position == 18:
                s.status = 8
            elif s.position >= 19:
                s.status = 7
        elif s.season == "16/17":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 4:
                s.status = 2
            elif s.position in [5, 6]:
                s.status = 3
            elif s.position == 18:
                s.status = 8
            elif s.position >= 19:
                s.status = 7
        elif s.season in ["15/16", "14/15"]:
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 4:
                s.status = 2
            elif s.position in [5, 6]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "13/14":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 16:
                s.status = 2
            elif s.position in [4, 5]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "12/13":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 7:
                s.status = 2
            elif s.position in [4, 5]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "11/12":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position == 4:
                s.status = 2
            elif s.position in [5, 10]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "10/11":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 6]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "09/10":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 13]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "08/09":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5]:
                s.status = 3
            elif s.position >= 18:
                s.status = 7
        elif s.season == "07/08":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 16]:
                s.status = 3
            elif s.position == 6:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "06/07":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 6, 7]:
                s.status = 3
            elif s.position == 5:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "05/06":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 9, 12]:
                s.status = 3
            elif s.position in [5, 6]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "04/05":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 8, 11]:
                s.status = 3
            elif s.position in [5, 6, 7]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "03/04":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5]:
                s.status = 3
            elif s.position in [6, 10, 11]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "02/03":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 6, 8]:
                s.status = 3
            elif s.position in [7, 9, 10]:
                s.status = 6
            elif s.position >= 18:
                s.status = 7
        elif s.season == "01/02":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 6]:
                s.status = 3
            elif s.position in [5, 7, 8]:
                s.status = 6
            elif s.position == 17:
                s.status = 7
            elif s.position == 18:
                s.status = 12
        elif s.season == "00/01":
            if s.position <= 2:
                s.status = 0
            elif s.position == 3:
                s.status = 1
            elif s.position in [4, 5, 18]:
                s.status = 3
            elif s.position in [6, 7, 8, 9]:
                s.status = 6
            elif s.position >= 16:
                s.status = 7
    return s

def create_leagues():
    with Session(engine) as session:
        leagues = [
            League(name = "LaLiga", code = "SP1", country = "Spain", level = 1),
            League(name = "LaLiga2", code = "SP2", country = "Spain", level = 2),
            League(name = "Premier League", code = "E0", country = "England", level = 1),
            League(name = "Serie A", code = "I1", country = "Italy", level = 1),
            League(name = "Bundesliga", code = "D1", country = "Germany", level = 1),
            League(name = "Ligue 1", code = "F1", country = "France", level = 1)
        ]
        session.add_all(leagues)
        session.commit()

def create_matches_and_teams():
    with Session(engine) as session:
        leagues = session.exec(
            select(League)
        ).all()
        teams_added = {}
        for league in leagues:
            for season in seasons:
                df = load_dataframe(league.code, season.replace("/", ""))
                for index, row in df.iterrows():
                    home_team_name = row["HomeTeam"]
                    away_team_name = row["AwayTeam"]                    
                    if home_team_name in fixed_names:
                        home_team_name = fixed_names[home_team_name]
                    if away_team_name in fixed_names:
                        away_team_name = fixed_names[away_team_name]
                    if home_team_name not in teams_added:
                        home_team = Team(name = home_team_name, country = league.country)
                        session.add(home_team)
                        teams_added[home_team_name] = home_team
                    else:
                        home_team = teams_added[home_team_name]
                    if away_team_name not in teams_added:
                        away_team = Team(name = away_team_name, country = league.country)
                        session.add(away_team)
                        teams_added[away_team_name] = away_team
                    else:
                        away_team = teams_added[away_team_name]
                    match = Match(
                        league = league,
                        season = season,
                        date = row["Date"],
                        home_team = home_team,
                        away_team = away_team,
                        home_goals = row["FTHG"],
                        away_goals = row["FTAG"]
                    )
                    session.add(match)
        session.commit() # realiza las operaciones guardadas en memoria (en este caso, añadir los partidos)

def create_standings():
    with Session(engine) as session:
        leagues = session.exec(select(League)).all()
        for league in leagues:
            for season in seasons:
                matches = session.exec(
                    select(Match).where(Match.league_id == league.id, Match.season == season)
                ).all() # all() me devuelve la ejecución como una lista con todas las filas
                stats = {}
                for match in matches:
                    home_team = match.home_team_id
                    away_team = match.away_team_id
                    if home_team not in stats:
                        stats[home_team] = {
                            "points": 0,
                            "matches_played": 0,
                            "wins": 0,
                            "draws": 0,
                            "losses": 0,
                            "goals_for": 0,
                            "goals_against": 0,
                            "goal_difference": 0
                        }
                    if away_team not in stats:
                        stats[away_team] = {
                            "points": 0,
                            "matches_played": 0,
                            "wins": 0,
                            "draws": 0,
                            "losses": 0,
                            "goals_for": 0,
                            "goals_against": 0,
                            "goal_difference": 0
                        }            
                    stats[home_team]["matches_played"] += 1
                    stats[home_team]["goals_for"] += match.home_goals
                    stats[home_team]["goals_against"] += match.away_goals
                    stats[home_team]["goal_difference"] += match.home_goals - match.away_goals
                    stats[away_team]["matches_played"] += 1
                    stats[away_team]["goals_for"] += match.away_goals
                    stats[away_team]["goals_against"] += match.home_goals
                    stats[away_team]["goal_difference"] += match.away_goals - match.home_goals
                    if match.home_goals > match.away_goals:
                        stats[home_team]["points"] += 3
                        stats[home_team]["wins"] += 1
                        stats[away_team]["losses"] += 1
                    elif match.home_goals < match.away_goals:
                        stats[away_team]["points"] += 3
                        stats[away_team]["wins"] += 1
                        stats[home_team]["losses"] += 1
                    else:
                        stats[home_team]["points"] += 1
                        stats[away_team]["points"] += 1
                        stats[home_team]["draws"] += 1
                        stats[away_team]["draws"] += 1
                if league.name in ["LaLiga", "LaLiga2", "Serie A"]:
                    standings = sorted( # parámetros: objeto iterable, key (función que decide el orden), reverse)
                        stats.items(), # lista de pares con los elementos del diccionario
                        key = lambda x: (x[1]["points"]), # (criterio de orden, primer criterio de desempate, segundo criterio de desempate)
                        reverse = True
                    )
                    standings = tie_breaker(standings, season)
                else: 
                    standings = sorted(
                        stats.items(),
                        key = lambda x: (x[1]["points"], x[1]["goal_difference"], x[1]["goals_for"]), 
                        reverse = True
                    )
                position = 1
                for key, value in standings:
                    session.add(set_status(Standings(
                        league = league,
                        season = season,
                        position = position,
                        team_id = key,
                        points = value["points"],
                        matches_played = value["matches_played"],
                        wins = value["wins"],
                        draws = value["draws"],
                        losses = value["losses"],
                        goals_for = value["goals_for"],
                        goals_against = value["goals_against"],
                        goal_difference = value["goal_difference"]
                    ))) 
                    position += 1
        session.commit()

def main():
    create_db_and_tables() # tengo que ejecutarlo cada vez que cree nuevas tablas (las tablas creadas no se eliminan)
    create_leagues()
    create_matches_and_teams()
    create_standings()

if __name__ == "__main__":
    main() # no quiero ejecutar la función main cada vez que se ejecute un archivo en el que importe este