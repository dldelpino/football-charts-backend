# ejecutar con python -m app.random_stats para que los paquetes se importan correctamente

import json, os, random, sys
from collections import defaultdict
from sqlmodel import Session, select

from main import max_positions, position_frequency, threshold_standings, promotion_frequency, relegation_frequency
from app.database import create_db_and_tables, engine
from app.models import League, Match, Standings, Team

def join_winners(l: list[str], scope: int):
    if scope == 0:
        if len(l[0]) == 2:
            l = [str(a) for (a, b) in l]
        elif len(l[0]) == 3:
            l = [str(a) + " (" + str(c) + ")" for (a, b, c) in l]
    else:
        if len(l[0]) == 2:
            l = [str(a) + " (" + str(b) + ")" for (a, b) in l]
        elif len(l[0]) == 3:
            l = [str(a) + " (" + str(b) + ", " + str(c) + ")" for (a, b, c) in l]
    if len(l) == 1:
        result = l[0]
    elif len(l) == 2:
        result = l[0] + " and " + l[1]
    else:
        result = ", ".join(l[:-1]) + " and " + l[-1]
    return result

def random_stats_generator(scope: int, stat: int, league_id: int = 1, position: int = 2, matches_played: int = 1):
    with Session(engine) as session:
        if 0 <= stat <= 9:
            if scope == 0:
                league = session.exec(
                    select(League).where(League.id == league_id)
                ).first()
                matches = session.exec(
                    select(Match).where(Match.league_id == league.id)
                ).all()
            else:
                matches = session.exec(
                    select(Match)
                ).all()

            data = defaultdict(int) # equipo y récord
            aux = defaultdict(int)

            for m in matches:
                home_team = session.exec(
                    select(Team).where(Team.id == m.home_team_id)
                ).first()
                away_team = session.exec(
                    select(Team).where(Team.id == m.away_team_id)
                ).first()
                h = (home_team.name, m.league.name)
                a = (away_team.name, m.league.name)
                if stat == 0: # ganando
                    string = "winning"
                    if m.home_goals > m.away_goals:
                        aux[h] += 1
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                    elif m.home_goals == m.away_goals:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                    elif m.home_goals < m.away_goals:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        aux[a] += 1
                if stat == 1: # empatando
                    string = "drawing"
                    if m.home_goals == m.away_goals:
                        aux[h] += 1
                        aux[a] += 1
                    else:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                if stat == 2: # perdiendo
                    string = "losing"
                    if m.home_goals < m.away_goals:
                        aux[h] += 1
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                    elif m.home_goals == m.away_goals:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                    elif m.home_goals > m.away_goals:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        aux[a] += 1
                if stat == 3: # marcando
                    string = "scoring"
                    if m.home_goals > 0:
                        aux[h] += 1
                    else:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                    if m.away_goals > 0:
                        aux[a] += 1
                    else:
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                if stat == 4: # encajando
                    string = "conceding"
                    if m.away_goals > 0:
                        aux[h] += 1
                    else:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                    if m.home_goals > 0:
                        aux[a] += 1
                    else:
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                if stat == 5: # sin ganar
                    string = "without winning"
                    if m.home_goals > m.away_goals:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        aux[a] += 1
                    elif m.home_goals == m.away_goals:
                        aux[h] += 1
                        aux[a] += 1
                    elif m.home_goals < m.away_goals:
                        aux[h] += 1
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                if stat == 6: # sin empatar
                    string = "without drawing"
                    if m.home_goals != m.away_goals:
                        aux[h] += 1
                        aux[a] += 1
                    else:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                if stat == 7: # sin perder
                    string = "without losing"
                    if m.home_goals < m.away_goals:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                        aux[a] += 1
                    elif m.home_goals == m.away_goals:
                        aux[h] += 1
                        aux[a] += 1
                    elif m.home_goals > m.away_goals:
                        aux[h] += 1
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                if stat == 8: # sin marcar
                    string = "without scoring"
                    if m.home_goals == 0:
                        aux[h] += 1
                    else:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                    if m.away_goals == 0:
                        aux[a] += 1
                    else:
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
                if stat == 9: # sin encajar
                    string = "without conceding"
                    if m.away_goals == 0:
                        aux[h] += 1
                    else:
                        if aux[h] > data[h]:
                            data[h] = aux[h]
                        aux[h] = 0
                    if m.home_goals == 0:
                        aux[a] += 1
                    else:
                        if aux[a] > data[a]:
                            data[a] = aux[a]
                        aux[a] = 0
            winners = [key for key, value in data.items() if value == max(data.values())]
            if scope == 0:
                if len(winners) == 1:
                    result = join_winners(winners, scope) + " is the team in " + league.name + " with most consecutive matches " + string + " (" + str(data[winners[0]]) + ")."
                else:
                    result = join_winners(winners, scope) + " are the teams in " + league.name + " with most consecutive matches " + string + " (" + str(data[winners[0]]) + ")."
            else:
                if len(winners) == 1:
                    result = join_winners(winners, scope) + " is the team in all leagues with most consecutive matches " + string + " (" + str(data[winners[0]]) + ")."
                else:
                    result = join_winners(winners, scope) + " are the teams in all leagues with most consecutive matches " + string + " (" + str(data[winners[0]]) + ")."
        if 10 <= stat <= 13:
            if scope == 0:
                league = session.exec(
                    select(League).where(League.id == league_id)
                ).first()
                if stat == 10:
                    data = relegation_frequency(league.name)
                    i = max(data.values())
                    winners = [(key, league.name) for key, value in data.items() if value == i]
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in " + league.name + " with most relegations (" + str(i) +")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in " + league.name + " with most relegations (" + str(i) +")."
                elif stat == 11:
                    data = promotion_frequency(league.name)
                    i = max(data.values())
                    winners = [(key, league.name) for key, value in data.items() if value == i]
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in " + league.name + " with most promotions (" + str(i) +")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in " + league.name + " with most promotions (" + str(i) +")."
                elif stat == 12:
                    data = position_frequency(league.name, 1)
                    i = max(data.values())
                    winners = [(key, league.name) for key, value in data.items() if value == i]
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team with most " + league.name + " titles (" + str(i) +")."
                    else:
                        result = join_winners(winners, scope) + " are the teams with most " + league.name + " titles (" + str(i) +")."
                elif stat == 13:
                    data = position_frequency(league.name, position)
                    i = max(data.values())
                    winners = [(key, league.name) for key, value in data.items() if value == i]
                    if position == 21:
                        if len(winners) == 1:
                            result = join_winners(winners, scope) + " is the team in " + league.name + " that has finished in " + str(position) + "st place most times (" + str(i) +")."
                        else:
                            result = join_winners(winners, scope) + " are the teams in " + league.name + " that have finished in " + str(position) + "st place most times (" + str(i) +")."
                    elif position == 2 or position == 22:
                        if len(winners) == 1:
                            result = join_winners(winners, scope) + " is the team in " + league.name + " that has finished in " + str(position) + "nd place most times (" + str(i) +")."
                        else:
                            result = join_winners(winners, scope) + " are the teams in " + league.name + " that have finished in " + str(position) + "nd place most times (" + str(i) +")."
                    elif position == 3:
                        if len(winners) == 1:
                            result = join_winners(winners, scope) + " is the team in " + league.name + " that has finished in " + str(position) + "rd place most times (" + str(i) +")."
                        else:
                            result = join_winners(winners, scope) + " are the teams in " + league.name + " that have finished in " + str(position) + "rd place most times (" + str(i) +")."
                    else:
                        if len(winners) == 1:
                            result = join_winners(winners, scope) + " is the team in " + league.name + " that has finished in " + str(position) + "th place most times (" + str(i) +")."
                        else:
                            result = join_winners(winners, scope) + " are the teams in " + league.name + " that have finished in " + str(position) + "th place most times (" + str(i) +")."
            else:
                leagues = session.exec(
                    select(League)
                ).all()
                i = 0
                winners = []
                for league in leagues: 
                    if stat == 10:
                        data = relegation_frequency(league.name)
                        j = max(data.values())
                        league_winners = [key for key, value in data.items() if value == j]
                        if j >= i:
                            i = j
                            winners += [(league_winner, league.name, i) for league_winner in league_winners]
                    elif stat == 11:
                        data = promotion_frequency(league.name)
                        j = max(data.values())
                        league_winners = [key for key, value in data.items() if value == j]
                        if j >= i:
                            i = j
                            winners += [(league_winner, league.name, i) for league_winner in league_winners]
                    elif stat == 12:
                        data = position_frequency(league.name, 1)
                        j = max(data.values())
                        league_winners = [key for key, value in data.items() if value == j]
                        if j >= i:
                            i = j
                            winners += [(league_winner, league.name, i) for league_winner in league_winners]
                    elif stat == 13 and position <= max_positions[league.id]:
                        data = position_frequency(league.name, position)
                        j = max(data.values())
                        league_winners = [key for key, value in data.items() if value == j]
                        if j >= i:
                            i = j
                            winners += [(league_winner, league.name, i) for league_winner in league_winners]
                i = max([c for (a, b, c) in winners])
                winners = [(a, b) for (a, b, c) in winners if c == i]
                if stat == 10:
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in all leagues with most relegations (" + str(i) +")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in all leagues with most relegations (" + str(i) +")."
                elif stat == 11:
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in all leagues with most promotions (" + str(i) +")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in all leagues with most promotions (" + str(i) +")."
                elif stat == 12:
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in all leagues with most league titles (" + str(i) +")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in all leagues with most league titles (" + str(i) +")."
                elif stat == 13 and position == 21:
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in all leagues that has finished in " + str(position) + "st place most times (" + str(i) + ")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in all leagues that have finished in " + str(position) + "st place most times (" + str(i) + ")."
                elif stat == 13 and position in [2, 22]:
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in all leagues that has finished in " + str(position) + "nd place most times (" + str(i) + ")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in all leagues that have finished in " + str(position) + "nd place most times (" + str(i) + ")."
                elif stat == 13 and position == 3:
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in all leagues that has finished in " + str(position) + "rd place most times (" + str(i) + ")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in all leagues that have finished in " + str(position) + "rd place most times (" + str(i) + ")."
                else:
                    if len(winners) == 1:
                        result = join_winners(winners, scope) + " is the team in all leagues that has finished in " + str(position) + "th place most times (" + str(i) + ")."
                    else:
                        result = join_winners(winners, scope) + " are the teams in all leagues that have finished in " + str(position) + "th place most times (" + str(i) + ")."
        if 14 <= stat <= 41:
            if scope == 0:
                league = session.exec(
                    select(League).where(League.id == league_id)
                ).first()
                standings = session.exec(
                    select(Standings).where(Standings.league_id == league.id)
                ).all()
            else:
                standings = session.exec(
                    select(Standings)
                ).all()

            data = defaultdict(lambda: [0, 0]) # (equipo, liga): [stat, partidos jugados]

            for s in standings:
                if (stat - 14) // 4 == 0: # 14, 15, 16, 17
                    data[(s.team.name, s.league.name)][0] += s.points
                elif (stat - 14) // 4 == 1: # 18, 19, 20, 21
                    data[(s.team.name, s.league.name)][0] += s.wins
                elif (stat - 14) // 4 == 2: # 22, 23, 24, 25
                    data[(s.team.name, s.league.name)][0] += s.draws
                elif (stat - 14) // 4 == 3: # 26, 27, 28, 29
                    data[(s.team.name, s.league.name)][0] += s.losses
                elif (stat - 14) // 4 == 4: # 30, 31, 32, 33
                    data[(s.team.name, s.league.name)][0] += s.goals_for
                elif (stat - 14) // 4 == 5: # 34, 35, 36, 37
                    data[(s.team.name, s.league.name)][0] += s.goals_against
                elif (stat - 14) // 4 == 6: # 38, 39, 40, 41
                    data[(s.team.name, s.league.name)][0] += s.goal_difference
                data[(s.team.name, s.league.name)][1] += s.matches_played

            if (stat - 14) % 4 == 0:
                i = max([value[0] for value in data.values()])
                winners = [key for key, value in data.items() if value[0] == i]
            elif (stat - 14) % 4 == 1:
                i = max([value[0]/value[1] for value in data.values()])
                winners = [key for key, value in data.items() if value[0]/value[1] == i]
            elif (stat - 14) % 4 == 2:
                i = min([value[0] for value in data.values()])
                winners = [key for key, value in data.items() if value[0] == i]
            elif (stat - 14) % 4 == 3:
                i = min([value[0]/value[1] for value in data.values()])
                winners = [key for key, value in data.items() if value[0]/value[1] == i]

            if ((stat - 14) % 4) % 2 == 0:
                string_stat = str(i)
            elif stat in [19, 21, 23, 25, 27, 29]:
                string_stat = str(round(i * 100, 2)) + "%"
            else:
                string_stat = str(round(i, 4))

            if stat == 14:
                string = "more total points"
            if stat == 15:
                string = "more points per match"
            if stat == 16:
                string = "less total points"
            if stat == 17:
                string = "less points per match"

            if stat == 18:
                string = "more total wins"
            if stat == 19:
                string = "more win percentage"
            if stat == 20:
                string = "less total wins"
            if stat == 21:
                string = "less win percentage"

            if stat == 22:
                string = "more total draws"
            if stat == 23:
                string = "more draw percentage"
            if stat == 24:
                string = "less total draws"
            if stat == 25:
                string = "less draw percentage"

            if stat == 26:
                string = "more total losses"
            if stat == 27:
                string = "more loss percentage"
            if stat == 28:
                string = "less total losses"
            if stat == 29:
                string = "less loss percentage"

            if stat == 30:
                string = "more total goals for"
            if stat == 31:
                string = "more goals for per match"
            if stat == 32:
                string = "less total goals for"
            if stat == 33:
                string = "less goals for per match"

            if stat == 34:
                string = "more total goals against"
            if stat == 35:
                string = "more goals against per match"
            if stat == 36:
                string = "less total goals against"
            if stat == 37:
                string = "less goals against per match"

            if stat == 38:
                string = "more total goal difference"
            if stat == 39:
                string = "more goal difference per match"
            if stat == 40:
                string = "less total goal difference"
            if stat == 41:
                string = "less goal difference per match"

            if scope == 0:
                if len(winners) == 1:
                    result = join_winners(winners, scope) + " is the team in " + league.name + " with " + string + " (" + string_stat + ")."
                else:
                    result = join_winners(winners, scope) + " are the teams in " + league.name + " with " + string + " (" + string_stat + ")."
            else:
                if len(winners) == 1:
                    result = join_winners(winners, scope) + " is the team in all leagues with " + string + " (" + string_stat + ")."
                else:
                    result = join_winners(winners, scope) + " are the teams in all leagues with " + string + " (" + string_stat + ")."
        if 42 <= stat <= 55:
            if scope == 0:
                league = session.exec(
                    select(League).where(League.id == league_id)
                ).first()
                standings = threshold_standings(league.name, matches_played, 0)
                if stat == 42:
                    i = max([s["points_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["points_mw"] == i]
                    string = "more points"
                if stat == 43:
                    i = min([s["points_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["points_mw"] == i]
                    string = "less points"
                if stat == 44:
                    i = max([s["wins_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["wins_mw"] == i]
                    string = "more wins"
                if stat == 45:
                    i = min([s["wins_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["wins_mw"] == i]
                    string = "less wins"
                if stat == 46:
                    i = max([s["draws_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["draws_mw"] == i]
                    string = "more draws"
                if stat == 47:
                    i = min([s["draws_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["draws_mw"] == i]
                    string = "less draws"
                if stat == 48:
                    i = max([s["losses_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["losses_mw"] == i]
                    string = "more losses"
                if stat == 49:
                    i = min([s["losses_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["losses_mw"] == i]
                    string = "less losses"
                if stat == 50:
                    i = max([s["goals_for_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_for_mw"] == i]
                    string = "more goals for"
                if stat == 51:
                    i = min([s["goals_for_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_for_mw"] == i]
                    string = "less goals for"
                if stat == 52:
                    i = max([s["goals_against_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_against_mw"] == i]
                    string = "more goals against"
                if stat == 53:
                    i = min([s["goals_against_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_against_mw"] == i]
                    string = "less goals against"
                if stat == 54:
                    i = max([s["goal_difference_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["goal_difference_mw"] == i]
                    string = "more goal difference"
                if stat == 55:
                    i = min([s["goal_difference_mw"] for s in standings])
                    winners = [(s["team"], league.name, s["season"]) for s in standings if s["goal_difference_mw"] == i]
                    string = "less goal difference"
                if len(winners) == 1:
                    result = join_winners(winners, scope) + " is the team in " + league.name + " with " + string + " after " + str(matches_played) + " matches played (" + str(i) + ")."
                else:
                    result = join_winners(winners, scope) + " are the teams in " + league.name + " with " + string + " after " + str(matches_played) + " matches played (" + str(i) + ")."
            else:
                leagues = session.exec(
                    select(League)
                ).all()
                i = -1
                winners = []
                for league in leagues:
                    if matches_played <= (max_positions[league.id] - 1)*2:
                        standings = threshold_standings(league.name, matches_played, 0)
                        if stat == 42:
                            j = max([s["points_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["points_mw"] == j]
                            if j >= i:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "more points"
                        if stat == 43:
                            j = min([s["points_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["points_mw"] == j]
                            if j <= i or i == -1:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "less points"
                        if stat == 44:
                            j = max([s["wins_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["wins_mw"] == j]
                            if j >= i:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "more wins"
                        if stat == 45:
                            j = min([s["wins_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["wins_mw"] == j]
                            if j <= i or i == -1:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "less wins"
                        if stat == 46:
                            j = max([s["draws_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["draws_mw"] == j]
                            if j >= i:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "more draws"
                        if stat == 47:
                            j = min([s["draws_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["draws_mw"] == j]
                            if j <= i or i == -1:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "less draws"
                        if stat == 48:
                            j = max([s["losses_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["losses_mw"] == j]
                            if j >= i:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "more losses"
                        if stat == 49:
                            j = min([s["losses_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["losses_mw"] == j]
                            if j <= i or i == -1:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "less losses"
                        if stat == 50:
                            j = max([s["goals_for_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_for_mw"] == j]
                            if j >= i:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "more goals for"
                        if stat == 51:
                            j = min([s["goals_for_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_for_mw"] == j]
                            if j <= i or i == -1:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "less goals for"
                        if stat == 52:
                            j = max([s["goals_against_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_against_mw"] == j]
                            if j >= i:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "more goals against"
                        if stat == 53:
                            j = min([s["goals_against_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["goals_against_mw"] == j]
                            if j <= i or i == -1:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "less goals against"
                        if stat == 54:
                            j = max([s["goal_difference_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["goal_difference_mw"] == j]
                            if j >= i:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "more goal difference"
                        if stat == 55:
                            j = min([s["goal_difference_mw"] for s in standings])
                            league_winners = [(s["team"], league.name, s["season"]) for s in standings if s["goal_difference_mw"] == j]
                            if j <= i or i == -1:
                                i = j
                                winners += [(a, b, c, i) for (a, b, c) in league_winners]
                            string = "less goal difference"
                i = max([d for (a, b, c, d) in winners])
                winners = [(a, b, c) for (a, b, c, d) in winners if d == i]
                if len(winners) == 1:
                    result = join_winners(winners, scope) + " is the team in all leagues with " + string + " after " + str(matches_played) + " matches played (" + str(i) + ")."
                else:
                    result = join_winners(winners, scope) + " are the teams in all leagues with " + string + " after " + str(matches_played) + " matches played (" + str(i) + ")."
    return result

random_stats = defaultdict(list)

for stat in range(0, 56): # random_stats_generator con 0 <= stat <= 9 tarda mucho en ejecutarse
    if 0 <= stat <= 12 or 14 <= stat <= 41:
        for league_id in range(1, 7):
            random_stats[stat].append(random_stats_generator(0, stat, league_id = league_id))
            print("SCOPE = 0, STAT = " + str(stat) + ", LEAGUE ID = " + str(league_id))
        random_stats[stat].append(random_stats_generator(1, stat))
        print("SCOPE = 1, STAT = " + str(stat))
    elif stat == 13:
        for league_id in range(1, 7):
            for position in range(2, max_positions[league_id] + 1):
                random_stats[stat].append(random_stats_generator(0, stat, league_id = league_id, position = position))
                print("SCOPE = 0, STAT = " + str(stat) + ", LEAGUE ID = " + str(league_id) + ", POSITION = " + str(position))
        for position in range(2, max(max_positions.values()) + 1):
            random_stats[stat].append(random_stats_generator(1, stat, position = position))
            print("SCOPE = 1, STAT = " + str(stat) + ", POSITION = " + str(position))
    elif 42 <= stat <= 55:
        for league_id in range(1, 7):
            if stat in [45, 47, 49]:
                min_matches_played = 11
            else:
                min_matches_played = 6
            for matches_played in range(min_matches_played, (max_positions[league_id] - 1)*2 + 1):
                random_stats[stat].append(random_stats_generator(0, stat, league_id = league_id, matches_played = matches_played))
                print("SCOPE = 0, STAT = " + str(stat) + ", LEAGUE ID = " + str(league_id) + ", MATCHES PLAYED = " + str(matches_played))
        if stat in [47, 49]:
            min_matches_played = 21
        elif stat == 45:
            min_matches_played = 16
        else:
            min_matches_played = 6
        for matches_played in range(min_matches_played, (max(max_positions.values()) - 1)*2 + 1):
            random_stats[stat].append(random_stats_generator(1, stat, matches_played = matches_played))
            print("SCOPE = 1, STAT = " + str(stat) + ", MATCHES PLAYED = " + str(matches_played))

with open("random_stats.json", "w", encoding = "utf-8") as file:
    file.write(json.dumps(random_stats, ensure_ascii = False, indent = 2))

'''
RACHAS DE PARTIDOS

0 -> partidos consecutivos ganando
1 -> partidos consecutivos empatando
2 -> partidos consecutivos perdiendo
3 -> partidos consecutivos marcando
4 -> partidos consecutivos encajando
5 -> partidos consecutivos sin ganar
6 -> partidos consecutivos sin empatar
7 -> partidos consecutivos sin perder
8 -> partidos consecutivos sin marcar
9 -> partidos consecutivos sin encajar

DATOS DE OTRAS SECCIONES

10 -> más descensos
11 -> más ascensos
12 -> más campeonatos
13 -> más veces ha quedado en X posición

DATOS QUE SE OBTIENEN SUMANDO CLASIFICACIONES

14 -> más puntos
15 -> más puntos por partido
16 -> menos puntos
17 -> menos puntos por partido
18 -> más victorias
19 -> mayor porcentaje de victorias
20 -> menos victorias
21 -> menor porcentaje de victorias
22 -> más empates
23 -> mayor porcentaje de empates
24 -> menos empates
25 -> menor porcentaje de empates
26 -> más derrotas
27 -> mayor porcentaje de derrotas
28 -> menos derrotas
29 -> menor porcentaje de derrotas
30 -> más goles a favor
31 -> más goles a favor por partido
32 -> menos goles a favor
33 -> menos goles a favor por partido
34 -> más goles en contra
35 -> más goles en contra por partido
36 -> menos goles en contra
37 -> menos goles en contra por partido
38 -> mayor diferencia de goles
39 -> mayor diferencia de goles por partido
40 -> menor diferencia de goles
41 -> menor diferencia de goles por partido

ESTADÍSTICAS TRAS X JORNADAS

42 -> más puntos tras X jornadas
43 -> menos puntos tras X jornadas
44 -> más victorias tras X jornadas
45 -> menos victorias tras X jornadas
46 -> más empates tras X jornadas
47 -> menos empates tras X jornadas
48 -> más derrotas tras X jornadas
49 -> menos derrotas tras X jornadas
50 -> más goles a favor tras X jornadas
51 -> menos goles a favor tras X jornadas
52 -> más goles en contra X jornadas
53 -> menos goles en contra X jornadas
54 -> mayor diferencia de goles X jornadas
55 -> menor diferencia de goles X jornadas
'''