# uvicorn main:app --reload

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select

from app.app import seasons, tie_breaker, engine
from app.models import Match, Standings, Team, League

app = FastAPI()

app.add_middleware( # evita problemas cuando Quasar haga las peticiones
    CORSMiddleware,
    allow_origins = ["*"],
    allow_methods = ["*"],
    allow_headers = ["*"],
)

ppm = ["Serie A", "Ligue 1"] # ligas en las que a veces hay que crear la columna adicional PPM

#########################################################################################################################################################

# TABLES

#########################################################################################################################################################

@app.get("/average-stats")
def average_stats(league_name: str):
    with Session(engine) as session:
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        teams = session.exec(
            select(Team)
            .join(Standings, Team.id == Standings.team_id)
            .where(Standings.league_id == league.id)
            .distinct()
        ).all()
        data = []
        for team in teams:
            team_standings = session.exec(
                select(Standings).where(Standings.team_id == team.id, Standings.league_id == league.id)
            ).all()
            n = len(team_standings)
            if league_name in ppm:
                sort_by = "avg_ppm"
                data.append({
                    "seasons_played": n,
                    "avg_position": "%.3f" % round(sum(s.position for s in team_standings) / n, 3),
                    "team": team.name,
                    "avg_ppm": "%.3f" % round(sum(s.points/s.matches_played for s in team_standings) / n, 3),
                    "avg_points": "%.3f" % round(sum(s.points for s in team_standings) / n, 3),
                    "avg_wins": "%.3f" % round(sum(s.wins for s in team_standings) / n, 3),
                    "avg_draws": "%.3f" % round(sum(s.draws for s in team_standings) / n, 3),
                    "avg_losses": "%.3f" % round(sum(s.losses for s in team_standings) / n, 3),
                    "avg_goals_for": "%.3f" % round(sum(s.goals_for for s in team_standings) / n, 3),
                    "avg_goals_against": "%.3f" % round(sum(s.goals_against for s in team_standings) / n, 3),
                    "avg_goal_difference": "%.3f" % round(sum(s.goal_difference for s in team_standings) / n, 3),
                    "logo": f"icons/teams/{team.country}/{team.name}.png"
                })
            else:
                sort_by = "avg_points"
                data.append({
                    "seasons_played": n,
                    "avg_position": "%.3f" % round(sum(s.position for s in team_standings) / n, 3),
                    "team": team.name,
                    "avg_points": "%.3f" % round(sum(s.points for s in team_standings) / n, 3),
                    "avg_wins": "%.3f" % round(sum(s.wins for s in team_standings) / n, 3),
                    "avg_draws": "%.3f" % round(sum(s.draws for s in team_standings) / n, 3),
                    "avg_losses": "%.3f" % round(sum(s.losses for s in team_standings) / n, 3),
                    "avg_goals_for": "%.3f" % round(sum(s.goals_for for s in team_standings) / n, 3),
                    "avg_goals_against": "%.3f" % round(sum(s.goals_against for s in team_standings) / n, 3),
                    "avg_goal_difference": "%.3f" % round(sum(s.goal_difference for s in team_standings) / n, 3),
                    "logo": f"icons/teams/{team.country}/{team.name}.png"
                })
    data = sorted(data, key = lambda x: x[sort_by], reverse = True)
    return data

@app.get("/position-history")
def position_history(league_name: str, position: int):
    with Session(engine) as session:
        league_id = session.exec(
            select(League.id).where(League.name == league_name)
        ).first()
        standings = session.exec(
            select(Standings).where(Standings.league_id == league_id, Standings.position == position)
        ).all()
        data = []
        for s in standings:
            if league_name in ppm:
                sort_by = "ppm"
                data.append({
                    "status": s.status,
                    "season": s.season,
                    "position": s.position,
                    "team": s.team.name,
                    "ppm": "%.3f" % round(s.points/s.matches_played, 3),
                    "points": s.points,
                    "matches_played": s.matches_played,
                    "wins": s.wins,
                    "draws": s.draws,
                    "losses": s.losses,
                    "goals_for": s.goals_for,
                    "goals_against": s.goals_against,
                    "goal_difference": s.goal_difference,
                    "logo": f"icons/teams/{s.team.country}/{s.team.name}.png",
                })
            else:
                sort_by = "points"
                data.append({
                    "status": s.status,
                    "season": s.season,
                    "position": s.position,
                    "team": s.team.name,
                    "points": s.points,
                    "matches_played": s.matches_played,
                    "wins": s.wins,
                    "draws": s.draws,
                    "losses": s.losses,
                    "goals_for": s.goals_for,
                    "goals_against": s.goals_against,
                    "goal_difference": s.goal_difference,
                    "logo": f"icons/teams/{s.team.country}/{s.team.name}.png",
                })
    data = sorted(data, key = lambda x: x[sort_by], reverse = True)
    return data

@app.get("/promoted-teams")
def promotead_teams(league_name: str):
    with Session(engine) as session:
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        data = []
        for i in range(len(seasons)):
            if i == 0:
                season1 = seasons[0]
                if league_name == "LaLiga":
                    promoted_teams = [19, 11, 17] # Villarreal, Las Palmas, Osasuna
                elif league_name == "LaLiga2":
                    promoted_teams = [46, 27, 47, 50] # Real Jaén, Murcia, Racing Ferrol, Universidad Las Palmas
                elif league_name == "Premier League":
                    promoted_teams = [107, 90, 91] # Ipswich, Charlton, Manchester City
                elif league_name == "Serie A":
                    promoted_teams = [140, 143, 138] # Atalanta, Vicenza, Napoli
                elif league_name == "Bundesliga":
                    promoted_teams = [202, 198, 194] # Köln, Energie Cottbus, Bochum
                elif league_name == "Ligue 1":
                    promoted_teams = [234, 232, 240] # Lille, Guingamp, Toulouse
            else:
                season0 = seasons[i-1]
                season1 = seasons[i]
                if league.level == 1:
                    teams0 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                    ).all()
                    teams1 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                    ).all()
                    promoted_teams = list(set(teams1) - set(teams0))
                else:
                    top_league = session.exec(
                        select(League).where(League.country == league.country, League.level == 1)
                    ).first()
                    teams0 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                    ).all()
                    teams1 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == top_league.id, Standings.season == season0)
                    ).all()
                    teams2 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                    ).all()
                    promoted_teams = list(set(teams2) - set(teams1) - set(teams0))
            for team_id in promoted_teams:
                team = session.exec(
                    select(Team).where(Team.id == team_id)
                ).first()
                standings = session.exec(
                    select(Standings).where(Standings.team_id == team.id, Standings.season == season1)
                ).first()
                if league_name in ppm:
                    sort_by = "ppm"
                    data.append({
                        "status": standings.status,
                        "season": standings.season,
                        "position": standings.position,
                        "team": standings.team.name,
                        "ppm": "%.3f" % round(standings.points/standings.matches_played, 3),
                        "points": standings.points,
                        "matches_played": standings.matches_played,
                        "wins": standings.wins,
                        "draws": standings.draws,
                        "losses": standings.losses,
                        "goals_for": standings.goals_for,
                        "goals_against": standings.goals_against,
                        "goal_difference": standings.goal_difference,
                        "logo": f"icons/teams/{standings.team.country}/{standings.team.name}.png",
                    })
                else:
                    sort_by = "points"
                    data.append({
                        "status": standings.status,
                        "season": standings.season,
                        "position": standings.position,
                        "team": standings.team.name,
                        "points": standings.points,
                        "matches_played": standings.matches_played,
                        "wins": standings.wins,
                        "draws": standings.draws,
                        "losses": standings.losses,
                        "goals_for": standings.goals_for,
                        "goals_against": standings.goals_against,
                        "goal_difference": standings.goal_difference,
                        "logo": f"icons/teams/{standings.team.country}/{standings.team.name}.png",
                    })
    data = sorted(data, key = lambda x: (x["position"], -float(x[sort_by]))) # el - cambiar el orden de ordenación; tengo que convertir a float porque ppm es un string
    return data

@app.get("/season-standings")
def season_standings(league_name: str, season: str):
    with Session(engine) as session:
        league_id = session.exec(
            select(League.id).where(League.name == league_name)
        ).first() # first() devuelve el contenido de la ejecución
        results = session.exec(
            select(Standings, Team) # devolverá una lista de duplas
            .join(Team, Standings.team_id == Team.id)
            .where(Standings.league_id == league_id, Standings.season == season)
        ).all()
        data = []
        for standings, team in results:
            data.append({
                "status": standings.status,
                "position": standings.position,
                "team": team.name,
                "points": standings.points,
                "matches_played": standings.matches_played,
                "wins": standings.wins,
                "draws": standings.draws,
                "losses": standings.losses,
                "goals_for": standings.goals_for,
                "goals_against": standings.goals_against,
                "goal_difference": standings.goal_difference,
                "logo": f"icons/teams/{team.country}/{team.name}.png",
            })
    return data

@app.get("/team-trajectory")
def team_trajectory(league_name: str, team_name: str):
    with Session(engine) as session:
        team = session.exec(
            select(Team).where(Team.name == team_name)
        ).first()
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        standings = session.exec(
            select(Standings).where(Standings.team_id == team.id, Standings.league_id == league.id)
        ).all()
        data = []
        for s in standings:
            if league.name in ppm:
                sort_by = "ppm"
                data.append({
                    "status": s.status,
                    "season": s.season,
                    "position": s.position,
                    "team": s.team.name,
                    "ppm": "%.3f" % round(s.points/s.matches_played, 3),
                    "points": s.points,
                    "matches_played": s.matches_played,
                    "wins": s.wins,
                    "draws": s.draws,
                    "losses": s.losses,
                    "goals_for": s.goals_for,
                    "goals_against": s.goals_against,
                    "goal_difference": s.goal_difference,
                    "logo": f"icons/teams/{s.team.country}/{s.team.name}.png",
                })
            else:
                sort_by = "points"
                data.append({
                    "status": s.status,
                    "season": s.season,
                    "position": s.position,
                    "team": s.team.name,
                    "points": s.points,
                    "matches_played": s.matches_played,
                    "wins": s.wins,
                    "draws": s.draws,
                    "losses": s.losses,
                    "goals_for": s.goals_for,
                    "goals_against": s.goals_against,
                    "goal_difference": s.goal_difference,
                    "logo": f"icons/teams/{s.team.country}/{s.team.name}.png",
                })
    data = sorted(data, key = lambda x: (x["position"], -float(x[sort_by])))
    return data

@app.get("/threshold-standings")
def threshold_standings(league_name: str, matches_played: int, threshold: int):
    with Session(engine) as session:
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        data = []
        for season in seasons:
            points = {}
            team_ids = session.exec(
                select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season)
            ).all()
            for team_id in team_ids:
                points[team_id] = 0
                matches = session.exec(
                    select(Match).where(Match.league_id == league.id, Match.season == season, (Match.home_team_id == team_id) | (Match.away_team_id == team_id))
                ).all()
                i = 0
                while i <= min(matches_played-1, len(matches)-1):
                    match = matches[i]
                    if match.home_team_id == team_id:
                        if match.home_goals > match.away_goals:
                            points[team_id] += 3
                        elif match.home_goals == match.away_goals:
                            points[team_id] += 1
                    else:
                        if match.home_goals < match.away_goals:
                            points[team_id] += 3
                        elif match.home_goals == match.away_goals:
                            points[team_id] += 1
                    i += 1
            standings = sorted( # parámetros: objeto iterable, key (función que decide el orden), reverse)
                points.items(), # lista de pares con los elementos del diccionario
                key = lambda x: (x[1]), # (criterio de orden, primer criterio de desempate, segundo criterio de desempate)
                reverse = True
            )
            for team_id, team_points in standings:
                if team_points >= threshold:
                    final_standings = session.exec(
                        select(Standings).where(Standings.league_id == league.id, Standings.season == season, Standings.team_id == team_id)
                    ).first()
                    if league_name in ppm:
                        sort_by = "ppm"
                        sort_by_mw = "ppm_mw"
                        data.append({
                            "status": final_standings.status,
                            "season": season,
                            "position": final_standings.position,
                            "team": final_standings.team.name,
                            "ppm_mw": "%.3f" % round(team_points/matches_played, 3),
                            "ppm": "%.3f" % round(final_standings.points/final_standings.matches_played, 3),
                            "points_mw": team_points,
                            "points": final_standings.points,
                            "matches_played": final_standings.matches_played,
                            "wins": final_standings.wins,
                            "draws": final_standings.draws,
                            "losses": final_standings.losses,
                            "goals_for": final_standings.goals_for,
                            "goals_against": final_standings.goals_against,
                            "goal_difference": final_standings.goal_difference,
                            "logo":  f"icons/teams/{final_standings.team.country}/{final_standings.team.name}.png"
                        })
                    else:
                        sort_by = "points"
                        sort_by_mw = "points_mw"
                        data.append({
                            "status": final_standings.status,
                            "season": season,
                            "position": final_standings.position,
                            "team": final_standings.team.name,
                            "points_mw": team_points,
                            "points": final_standings.points,
                            "matches_played": final_standings.matches_played,
                            "wins": final_standings.wins,
                            "draws": final_standings.draws,
                            "losses": final_standings.losses,
                            "goals_for": final_standings.goals_for,
                            "goals_against": final_standings.goals_against,
                            "goal_difference": final_standings.goal_difference,
                            "logo":  f"icons/teams/{final_standings.team.country}/{final_standings.team.name}.png"
                        })
    data = sorted(data, key = lambda x: (x["position"], -float(x[sort_by]), -float(x[sort_by_mw])))
    return data

#########################################################################################################################################################

# PIE CHARTS

#########################################################################################################################################################

@app.get("/position-frequency")
def position_frequency(league_name: str, position: int):
    with Session(engine) as session:
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        standings = session.exec(
            select(Standings).where(Standings.league_id == league.id, Standings.position == position)
        ).all()
        data = {}
        teams_added = []
        for s in standings:
            team = s.team
            if team not in teams_added:
                teams_added.append(team)
                data[team.name] = 1
            else:
                data[team.name] += 1
    return data

#########################################################################################################################################################

# BAR CHARTS

#########################################################################################################################################################

@app.get("/promotion-frequency")
def promotion_frequency(league_name: str):
    with Session(engine) as session:
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        data = {}
        teams_added = []
        for i in range(len(seasons)):
            if i == 0:
                if league_name == "LaLiga":
                    promoted_teams = [19, 11, 17] # Villarreal, Las Palmas, Osasuna
                elif league_name == "LaLiga2":
                    promoted_teams = [46, 27, 47, 50] # Real Jaén, Murcia, Racing Ferrol, Universidad Las Palmas
                elif league_name == "Premier League":
                    promoted_teams = [107, 90, 91] # Ipswich, Charlton, Manchester City
                elif league_name == "Serie A":
                    promoted_teams = [140, 143, 138] # Atalanta, Vicenza, Napoli
                elif league_name == "Bundesliga":
                    promoted_teams = [202, 198, 194] # Köln, Energie Cottbus, Bochum
                elif league_name == "Ligue 1":
                    promoted_teams = [234, 232, 240] # Lille, Guingamp, Toulouse
            else:
                season0 = seasons[i-1]
                season1 = seasons[i]
                if league.level == 1:
                    teams0 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                    ).all()
                    teams1 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                    ).all()
                    promoted_teams = list(set(teams1) - set(teams0))
                else:
                    top_league = session.exec(
                        select(League).where(League.country == league.country, League.level == 1)
                    ).first()
                    teams0 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                    ).all()
                    teams1 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == top_league.id, Standings.season == season0)
                    ).all()
                    teams2 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                    ).all()
                    promoted_teams = list(set(teams2) - set(teams1) - set(teams0))
            for team_id in promoted_teams:
                team = session.exec(
                    select(Team).where(Team.id == team_id)
                ).first()
                if team not in teams_added:
                    teams_added.append(team)
                    data[team.name] = 1
                else:
                    data[team.name] += 1      
    return data

@app.get("/relegation-frequency")
def relegation(league_name: str):
    with Session(engine) as session:
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        data = {}
        teams_added = []
        for i in range(len(seasons)):
            if i == len(seasons)-1:
                if league_name == "LaLiga":
                    relegated_teams = [40, 11, 14] # Leganés, Las Palmas, Valladolid
                elif league_name == "LaLiga2":
                    relegated_teams = [89, 23, 47, 66] # Eldense, Tenerife, Racing Ferrol, Cartagena
                elif league_name == "Premier League":
                    relegated_teams = [100, 107, 97] # Leicester, Ipswich, Southampton
                elif league_name == "Serie A":
                    relegated_teams = [159, 155, 183] # Empoli, Venezia, Monza
                elif league_name == "Bundesliga":
                    relegated_teams = [223, 194] # Holstein Kiel, Bochum
                elif league_name == "Ligue 1":
                    relegated_teams = [259, 233, 244] # Reims, Saint-Étienne, Montpellier
            else:
                season0 = seasons[i]
                season1 = seasons[i+1]
                if league.level == 1:
                    teams0 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                    ).all()
                    teams1 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                    ).all()
                    relegated_teams = list(set(teams0) - set(teams1))
                else:
                    top_league = session.exec(
                        select(League).where(League.country == league.country, League.level == 1)
                    ).first()
                    teams0 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                    ).all()
                    teams1 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == top_league.id, Standings.season == season1)
                    ).all()
                    teams2 = session.exec(
                        select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                    ).all()
                    relegated_teams = list(set(teams0) - set(teams1) - set(teams2))
            for team_id in relegated_teams:
                team = session.exec(
                    select(Team).where(Team.id == team_id)
                ).first()
                if team not in teams_added:
                    teams_added.append(team)
                    data[team.name] = 1
                else:
                    data[team.name] += 1          
    return data

#########################################################################################################################################################

# OTHER

#########################################################################################################################################################

@app.get("/team-streaks")
def team_streaks(league_name: str, team_name: str):
    with Session(engine) as session:
        team = session.exec(
            select(Team).where(Team.name == team_name)
        ).first()
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        matches = session.exec(
            select(Match).where(Match.league_id == league.id, (Match.home_team_id == team.id) | (Match.away_team_id == team.id))
        ).all()
        data = []
        # result = {
        #     "record": 0,  
        #     "matches": [],
        # }
        for k in range(10):
            i = 0
            aux = []
            data.append({
                "record": 0,  
                "matches": [],
            })
            for m in matches:
                if k == 0: # most consecutive wins
                    criteria = (m.home_goals > m.away_goals and m.home_team_id == team.id) or (m.home_goals < m.away_goals and m.away_team_id == team.id)
                elif k == 1: # most consecutive draws
                    criteria = m.home_goals == m.away_goals
                elif k == 2: # most consecutive losses
                    criteria = (m.home_goals < m.away_goals and m.home_team_id == team.id) or (m.home_goals > m.away_goals and m.away_team_id == team.id)
                elif k == 3: # most consecutive matches scoring
                    criteria = (m.home_goals > 0 and m.home_team_id == team.id) or (m.away_goals > 0 and m.away_team_id == team.id)
                elif k == 4: # most consecutive matches conceding
                    criteria = (m.away_goals > 0 and m.home_team_id == team.id) or (m.home_goals > 0 and m.away_team_id == team.id)
                elif k == 5: # most consecutive matches without winning
                    criteria = (m.home_goals <= m.away_goals and m.home_team_id == team.id) or (m.home_goals >= m.away_goals and m.away_team_id == team.id)
                elif k == 6: # most consecutive matches without drawing
                    criteria = (m.home_goals != m.away_goals and m.home_team_id == team.id) or (m.home_goals != m.away_goals and m.away_team_id == team.id)
                elif k == 7: # most consecutive matches without losing
                    criteria = (m.home_goals >= m.away_goals and m.home_team_id == team.id) or (m.home_goals <= m.away_goals and m.away_team_id == team.id)
                elif k == 8: # most consecutive matches without scoring
                    criteria = (m.home_goals == 0 and m.home_team_id == team.id) or (m.away_goals == 0 and m.away_team_id == team.id)
                elif k == 9: # most consecutive matches without conceding
                    criteria = (m.away_goals == 0 and m.home_team_id == team.id) or (m.home_goals == 0 and m.away_team_id == team.id)

                if criteria: 
                    i += 1
                    aux.append((
                        m, 
                        m.home_team.name, 
                        f"icons/teams/{m.home_team.country}/{m.home_team.name}.png",
                        m.away_team.name, 
                        f"icons/teams/{m.away_team.country}/{m.away_team.name}.png",
                    )) # necesito pasar al frontend los nombres de los equipos y la ubicación del icono del escudo
                else:
                    if i > data[k]["record"]:
                        data[k]["record"] = i
                        data[k]["matches"] = aux
                    i = 0
                    aux = []
    return data