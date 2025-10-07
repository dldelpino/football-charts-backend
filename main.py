# ejecutar con uvicorn main:app --reload

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, select

from app.app import seasons
from app.database import engine
from app.models import Standings, Team, League

app = FastAPI()

app.add_middleware( # evita problemas cuando Quasar haga las peticiones
    CORSMiddleware,
    allow_origins = ["*"],
    allow_methods = ["*"],
    allow_headers = ["*"],
)

ppm = ["Serie A", "Ligue 1"] # ligas en las que a veces hay que crear la columna adicional PPM (Points Per Match)

################################################################################################################################################################################

# TABLES

################################################################################################################################################################################

@app.get("/average-stats")
def average_stats(league_name: str):
    with Session(engine) as session:
        league = session.exec(
            select(League).where(League.name == league_name)
        ).first()
        teams_list = session.exec(
            select(Team).where(Team.country == league.country)
        ).all()
        data = []
        for team in teams_list:
            team_standings = session.exec(
                select(Standings).where(Standings.team_id == team.id)
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
    return sorted(data, key = lambda x: x[sort_by], reverse = True)

@app.get("/position-history")
def position_history(league_name: str, position: int):
    with Session(engine) as session:
        league_id = session.exec(
            select(League.id).where(League.name == league_name)
        ).first()
        results = session.exec(
            select(Standings).where(Standings.league_id == league_id, Standings.position == position)
        ).all()
        data = []
        for standings in results:
            if league_name in ppm:
                sort_by = "ppm"
                data.append({
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
    return sorted(data, key = lambda x: x[sort_by], reverse = True)

@app.get("/promoted-teams")
def promotead_teams(league_name: str):
    with Session(engine) as session:
        league_id = session.exec(
            select(League.id).where(League.name == league_name)
        ).first()
        data = []
        for i in range(len(seasons)):
            if i == 0:
                season1 = seasons[0]
                if league_name == "LaLiga":
                    promoted_teams = [19, 11, 17] # Villarreal, Las Palmas, Osasuna
                elif league_name == "Premier League":
                    promoted_teams = [60, 43, 44] # Ipswich, Charlton, Manchester City
                elif league_name == "Serie A":
                    promoted_teams = [93, 96, 91] # Atalanta, Vicenza, Napoli
                elif league_name == "Bundesliga":
                    promoted_teams = [155, 151, 147] # Köln, Energie Cottbus, Bochum
                elif league_name == "Ligue 1":
                    promoted_teams = [187, 185, 193] # Lille, Guingamp, Toulouse
            else:
                season0 = seasons[i-1]
                season1 = seasons[i]
                teams0 = session.exec(
                    select(Standings.team_id).where(Standings.league_id == league_id, Standings.season == season0)
                ).all()
                teams1 = session.exec(
                    select(Standings.team_id).where(Standings.league_id == league_id, Standings.season == season1)
                ).all()
                promoted_teams = list(set(teams1) - set(teams0))
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
    return sorted(data, key = lambda x: (x["season"], -float(x[sort_by]))) # ojito con ese - (tengo que convertir a float porque ppm es un string)

@app.get("/season-standings")
def season_standings(league_name: str, season: str):
    with Session(engine) as session:
        league_id = session.exec(
            select(League.id).where(League.name == league_name)
            ).first() # first() devuelve el contenido de la ejecución
        results = session.exec(
            select(Standings, Team)
            .join(Team, Standings.team_id == Team.id)
            .where(Standings.league_id == league_id, Standings.season == season)
        ).all()
        data = []
        for standings, team in results:
            data.append({
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
def team_trajectory(team_name: str):
    with Session(engine) as session:
        team = session.exec(
            select(Team).where(Team.name == team_name)
        ).first()
        team_league = session.exec(
            select(League.name).where(League.country == team.country)
        ).first()
        standings = session.exec(
            select(Standings).where(Standings.team_id == team.id)
        )
        data = []
        for s in standings:
            if team_league in ppm:
                sort_by = "ppm"
                data.append({
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
    return sorted(data, key = lambda x: x[sort_by], reverse = True)

################################################################################################################################################################################

# PIE CHARTS

################################################################################################################################################################################

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

################################################################################################################################################################################

# BAR CHARTS

################################################################################################################################################################################

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
                season1 = seasons[0]
                if league_name == "LaLiga":
                    promoted_teams = [19, 11, 17] # Villarreal, Las Palmas, Osasuna
                elif league_name == "Premier League":
                    promoted_teams = [60, 43, 44] # Ipswich, Charlton, Manchester City
                elif league_name == "Serie A":
                    promoted_teams = [93, 96, 91] # Atalanta, Vicenza, Napoli
                elif league_name == "Bundesliga":
                    promoted_teams = [155, 151, 147] # Köln, Energie Cottbus, Bochum
                elif league_name == "Ligue 1":
                    promoted_teams = [187, 185, 193] # Lille, Guingamp, Toulouse
            else:
                season0 = seasons[i-1]
                season1 = seasons[i]
                teams0 = session.exec(
                    select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                ).all()
                teams1 = session.exec(
                    select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                ).all()
                promoted_teams = list(set(teams1) - set(teams0))
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
                season1 = seasons[-1]
                if league_name == "LaLiga":
                    relegated_teams = [40, 11, 14] # Leganés, Las Palmas, Valladolid
                elif league_name == "Premier League":
                    relegated_teams = [53, 60, 50] # Leicester, Ipswich, Southampton
                elif league_name == "Serie A":
                    relegated_teams = [112, 108, 136] # Empoli, Venezia, Monza
                elif league_name == "Bundesliga":
                    relegated_teams = [176, 147] # Holstein Kiel, Bochum
                elif league_name == "Ligue 1":
                    relegated_teams = [212, 186, 197] # Reims, Saint-Étienne, Montpellier
            else:
                season0 = seasons[i]
                season1 = seasons[i+1]
                teams0 = session.exec(
                    select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season0)
                ).all()
                teams1 = session.exec(
                    select(Standings.team_id).where(Standings.league_id == league.id, Standings.season == season1)
                ).all()
                relegated_teams = list(set(teams0) - set(teams1))
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