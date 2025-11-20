from sqlmodel import Session, select
from app.app import engine
from app.models import Match, Standings, Team, League

with Session(engine) as session:
    league = session.exec(
        select(League).where(League.name == "LaLiga")
    ).first()
    teams = session.exec(
        select(Team)
        .join(Standings, Team.id == Standings.team_id)
        .where(Standings.league_id == league.id)
        .distinct()
    ).all()

print(teams)