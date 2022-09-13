import pandas as pd
from nba_api.stats.static import teams
from nba_api.stats.endpoints import playerdashptpass, commonplayerinfo, teamdashptpass, commonteamroster, leaguedashlineups,playerdashboardbyyearoveryear, leaguedashteamstats
import time
import networkx as nx

# Section 1 - getting and preparing data from the API
def get_ids(season, team):
    # get team id from user-provided team name
    team_id = teams.find_teams_by_full_name(team)[0]['id']
    # get player IDs - note this is only as of end of season
    player_ids = list(commonteamroster.CommonTeamRoster(season=season,team_id=team_id).get_data_frames()[0]['PLAYER_ID'].values)
    return team_id, player_ids

def get_passes(season,team,player_ids):
    full_passes_made = pd.DataFrame()
    trade_list = []

    # get pass data for each player on the team
    for player in player_ids:
        passes = playerdashptpass.PlayerDashPtPass(last_n_games='0',league_id='00',month='0',opponent_team_id='0',
                                                  per_mode_simple='Totals',player_id=str(player),season=season,
                                                  season_type_all_star='Regular Season',team_id=team,
                                                  vs_division_nullable='',vs_conference_nullable='',
                                                  season_segment_nullable='',outcome_nullable='',
                                                  location_nullable='',date_to_nullable='',date_from_nullable='')
        temp_made = passes.passes_made.get_data_frame()
        temp_made['season'] = season
        full_passes_made = pd.concat([full_passes_made,temp_made])
        time.sleep(1)

    [trade_list.append(receiver) if receiver not in list(full_passes_made['PLAYER_ID'].unique()) else 0 for receiver in list(full_passes_made['PASS_TEAMMATE_PLAYER_ID'].unique())]
    [player_ids.append(receiver) if receiver not in list(full_passes_made['PLAYER_ID'].unique()) else 0 for receiver in list(full_passes_made['PASS_TEAMMATE_PLAYER_ID'].unique())]

    # get additional players who were traded mid season
    for player in trade_list:
        passes = playerdashptpass.PlayerDashPtPass(last_n_games='0',league_id='00',month='0',opponent_team_id='0',
                                                  per_mode_simple='Totals',player_id=str(player),season=season,
                                                  season_type_all_star='Regular Season',team_id=team,
                                                  vs_division_nullable='',vs_conference_nullable='',
                                                  season_segment_nullable='',outcome_nullable='',
                                                  location_nullable='',date_to_nullable='',date_from_nullable='')
        temp_made = passes.passes_made.get_data_frame()
        temp_made['season'] = season
        full_passes_made = pd.concat([full_passes_made,temp_made])
        time.sleep(1)

    return full_passes_made, player_ids

def get_player_stats(season,team,player_ids):
    minutes_df = pd.DataFrame()

    # get stats for each player on the team
    for player in player_ids:

        player_stats = playerdashboardbyyearoveryear.PlayerDashboardByYearOverYear(last_n_games='0',measure_type_detailed='Base',
                                                                             month='0',opponent_team_id='0',pace_adjust='N',
                                                                             per_mode_detailed='Totals',period='0',
                                                                             player_id=str(player),plus_minus='N',rank='N',
                                                                             season=season,season_type_playoffs='Regular Season',
                                                                             vs_division_nullable='',vs_conference_nullable='',
                                                                             season_segment_nullable='',outcome_nullable='',
                                                                             location_nullable='',game_segment_nullable='',
                                                                             date_to_nullable='',date_from_nullable='')
        temp_minutes = player_stats.get_data_frames()[0]
        temp_minutes['season'] = season
        temp_minutes['PLAYER_ID'] = player

        minutes_df = pd.concat([minutes_df,temp_minutes])
        time.sleep(1)

    minutes_df['EFG'] = ((minutes_df['FGM']-minutes_df['FG3M'])+minutes_df['FG3M']*1.5)/minutes_df['FGA']

    return minutes_df

def make_lookup_lineup(x):
    if x['player1'] < x['player2']:
        return (x['player1']+' - '+x['player2'])
    else:
        return (x['player2']+' - '+x['player1'])

def get_lineups(season, team):

    # get lineup data to know how many minutes each player pair played
    lineups = leaguedashlineups.LeagueDashLineups(group_quantity='2',last_n_games='0',measure_type_detailed_defense='Base',
                                                  month='0',opponent_team_id='0',pace_adjust='N', per_mode_detailed='Totals',
                                                  period='0',plus_minus='N',rank='N',season=season,
                                                  season_type_all_star='Regular Season',vs_division_nullable='',
                                                  vs_conference_nullable='',team_id_nullable=team,
                                                  season_segment_nullable='',outcome_nullable='',location_nullable='',
                                                  game_segment_nullable='',division_simple_nullable='',conference_nullable='',
                                                  date_to_nullable='',date_from_nullable='')

    lineup_df = lineups.get_data_frames()[0]

    lineup_df['player1']=lineups.get_data_frames()[0]['GROUP_NAME'].apply(lambda x: x.split(' - ')[0].split('. ')[1])
    lineup_df['player2']=lineups.get_data_frames()[0]['GROUP_NAME'].apply(lambda x: x.split(' - ')[1].split('. ')[1])

    lineup_df['lookup'] = lineup_df.apply(make_lookup_lineup,axis=1)

    return lineup_df[lineup_df['MIN']>0]

def make_lookup_passes(x):
    if x['passer'] < x['receiver']:
        return (x['passer']+' - '+x['receiver'])
    else:
        return (x['receiver']+' - '+x['passer'])

def prep_full_passes(full_passes_made,lineup_df,minutes_df):
    # extract last names
    full_passes_made['passer'] = full_passes_made['PLAYER_NAME_LAST_FIRST'].apply(lambda x: x.split(',')[0])
    full_passes_made['receiver'] = full_passes_made['PASS_TO'].apply(lambda x: x.split(',')[0])

    # create lookup across tables
    full_passes_made['lookup'] = full_passes_made.apply(make_lookup_passes,axis=1)
    full_passes_made = full_passes_made.merge(lineup_df[['lookup','MIN']],how='left',on='lookup')

    # calculate adjusted passing metrics
    full_passes_made.rename(columns={'MIN':'lineup_min'},inplace=True)
    full_passes_made['pass_per_36'] = (full_passes_made['PASS']/full_passes_made['lineup_min'])*36

    full_passes_made = full_passes_made.merge(minutes_df[['PLAYER_ID','MIN','FGA','EFG']],how='left',on='PLAYER_ID',
                                              suffixes=('','_player'))
    full_passes_made['lineup_pct'] = full_passes_made['lineup_min']/full_passes_made['MIN']
    full_passes_made['freq_per_lineup_pct'] = full_passes_made['FREQUENCY']/full_passes_made['lineup_pct']

    return full_passes_made

def call_api(season, team):
    # get team id and player ids
    team_id, player_ids = get_ids(season,team)
    # updated player IDs returned - end of year and those who were traded
    full_passes_made, player_ids = get_passes(season,team_id,player_ids)
    # get player stats
    minutes_df = get_player_stats(season,team_id,player_ids)
    # get lineup data to calculate minutes played per player pair
    lineup_df = get_lineups(season,team_id)
    # additional data prep and metric calculation
    full_passes_made = prep_full_passes(full_passes_made,lineup_df,minutes_df)
    return full_passes_made

# Section 2 - data prep to support analysis
def filter_players(full_passes_made, shared_minutes_threshold=50):

    # get sorted list of players by passing volume
    players = list(full_passes_made.groupby(['passer'])['PASS'].sum().sort_values(ascending=False).index)
    droplist = []
    pass_df = full_passes_made.copy()

    # iterate through potential edges to remove low-minute pairs
    for player in players:
        temp_df = pass_df[pass_df['passer']==player].copy()
        droplist = list(set(droplist + list(temp_df[temp_df['lineup_min']<shared_minutes_threshold]['receiver'].values)))
        pass_df['keep1'] = pass_df['passer'].apply(lambda x: 0 if x in droplist else 1)
        pass_df['keep2'] = pass_df['receiver'].apply(lambda x: 0 if x in droplist else 1)
        pass_df = pass_df[(pass_df['keep1']==1)&(pass_df['keep2']==1)].copy()

    pass_df['edge'] = pass_df.apply(lambda x: (x['passer'],x['receiver']),axis=1)

    return pass_df

def make_analysis_df(pass_df, full_passes_made):

    # start analysis df
    analysis_df = pd.DataFrame(list(pass_df.groupby('passer')['PASS'].sum().sort_values(ascending=False).index),columns=['player'])

    # add reception data
    analysis_df = analysis_df.merge(pd.DataFrame(full_passes_made.groupby('receiver')['PASS'].sum()).reset_index(),
                                    how='left',left_on='player',right_on='receiver')
    analysis_df.rename(columns={'PASS':'receptions'},inplace=True)
    analysis_df.drop('receiver',axis=1,inplace=True)

    # add consistency of target
    temp_df = pass_df.groupby('receiver').agg({'freq_per_lineup_pct':['min','max']}).reset_index()
    temp_df.columns = ['receiver','rec_min','rec_max']
    temp_df['rec_range'] = temp_df['rec_max'] - temp_df['rec_min']

    analysis_df = analysis_df.merge(temp_df,how='left',left_on='player',right_on='receiver')
    analysis_df.drop('receiver',axis=1,inplace=True)

    # add FGA off of receptions
    analysis_df = analysis_df.merge(pd.DataFrame(full_passes_made.groupby('receiver').\
                                    agg({'FGA':'sum','FG2M':'sum','FG3M':'sum'})).\
                                    reset_index(),how='left',left_on='player',right_on='receiver')
    analysis_df.rename(columns={'FGA':'fga_off_reception','FG2M':'fg2m_off_reception','FG3M':'fg3m_off_reception'},inplace=True)
    analysis_df.drop('receiver',axis=1,inplace=True)

    # calculate propensity to shoot off of reception, and EFG off of reception
    analysis_df['fga_rate'] = analysis_df['fga_off_reception'] / analysis_df['receptions']
    analysis_df['efg_off_reception'] = (analysis_df['fg3m_off_reception'] * 1.5 + analysis_df['fg2m_off_reception']) / analysis_df['fga_off_reception']

    # add general FGA volume, general EFG, pass volume, receiver's fg rate/efg
    temp_df = pd.DataFrame(full_passes_made.groupby('passer').agg({'MIN':'mean','FGA_player':'mean','EFG':'mean','PASS':'sum',
                                                        'FGA':'sum','FG2M':'sum','FG3M':'sum'})).reset_index()
    temp_df.columns=['passer','minutes','fga_total','efg_total','passes','fga_receiver','fg2m_receiver','fg3m_receiver']

    # calculate metrics
    temp_df['fga_per_36'] = (temp_df['fga_total'] / temp_df['minutes']) * 36
    temp_df['pass_per_36'] = (temp_df['passes'] / temp_df['minutes']) * 36
    temp_df['fga_rate_receiver'] = temp_df['fga_receiver'] / temp_df['passes']
    temp_df['efg_receiver'] = (temp_df['fg3m_receiver'] * 1.5 + temp_df['fg2m_receiver']) / temp_df['fga_receiver']

    analysis_df = analysis_df.merge(temp_df,how='left',left_on='player',right_on='passer')
    analysis_df.drop('passer',axis=1,inplace=True)

    # add range of frequency of passing
    temp_df = pd.DataFrame(pass_df.groupby('passer').agg({'freq_per_lineup_pct':['min','max']})).reset_index()
    temp_df.columns=['passer','freq_min','freq_max']
    temp_df['freq_range'] = temp_df['freq_max'] - temp_df['freq_min']

    analysis_df = analysis_df.merge(temp_df,how='left',left_on='player',right_on='passer')

    analysis_df['rec_per_36'] = (analysis_df['receptions'] / analysis_df['minutes']) * 36
    analysis_df.drop('passer',axis=1,inplace=True)

    analysis_df['pts_from_field_shooting_per_36'] = ((analysis_df['fga_total']*analysis_df['efg_total'])/analysis_df['minutes'])*36
    analysis_df['pts_from_field_passing_per_36'] = ((analysis_df['fga_receiver']*analysis_df['efg_receiver'])/analysis_df['minutes'])*36

    return analysis_df

def prep_cluster_df(analysis_df, prediction=False, mean_dict=None,sd_dict=None):

    # grab data
    cluster_df = analysis_df[['player','rec_per_36','pass_per_36','freq_range','fga_rate_receiver','efg_receiver','fga_rate','fga_per_36',
                 'efg_off_reception','efg_total']].copy()

    x_values = cluster_df.copy()
    x_values.set_index('player',inplace=True)

    # normalize variables
    if prediction == False:
        mean_dict = {}
        sd_dict = {}

        for column in x_values.columns:
            mean_dict[column] = x_values[column].mean()
            sd_dict[column] = x_values[column].std()
            x_values[column] = (x_values[column] - x_values[column].mean())/x_values[column].std()

    # if prediction, use training mean/sd
    else:
        for column in x_values.columns:
            x_values[column] = (x_values[column] - mean_dict[column])/sd_dict[column]

    return cluster_df, x_values, mean_dict, sd_dict

def network_data_prep(pass_df,analysis_df):

    # calculate additional metrics
    pass_df['enabled_pts_per_pass'] = (pass_df['FG3M']*3+pass_df['FG2M']*2)/pass_df['PASS']
    pass_df['enabled_pts_per_36'] = ((pass_df['FG3M']*3+pass_df['FG2M']*2)/pass_df['lineup_min'])*36
    pass_df = pass_df.merge(analysis_df[['player','cluster1']],how='left',left_on='passer',right_on='player')

    pass_df['fga_rate_receiver'] = pass_df['FGA'] / pass_df['PASS']
    pass_df['efg_receiver'] = (pass_df['FG3M'] * 1.5 + pass_df['FG2M']) / pass_df['FGA']
    for var in ['edge','lineup_min','pass_per_36','freq_per_lineup_pct','enabled_pts_per_pass','enabled_pts_per_36',
          'fga_rate_receiver','efg_receiver']:
        pass_df[var+'_rank']=pass_df[var].rank(ascending=False)

    return pass_df

def build_network(small_df, pass_df, var='pass_per_36', high_threshold=16):
    # calculate threshold
    small_df[var+'_grp'] = small_df[var].apply(lambda x: 1 if x>high_threshold else 0)

    # grab colors for nodes
    temp_df = pd.DataFrame(pass_df.groupby(['passer','cluster1'])['PASS'].count()).reset_index()
    cluster_dict = dict(zip(temp_df['passer'],temp_df['cluster1']))

    # build graph
    g = nx.DiGraph()

    for passer in pass_df['passer'].unique():
        g.add_node(passer, node_color=cluster_dict[passer])

    for i in range(len(small_df)):
        g.add_edge(small_df.iloc[i]['passer'],small_df.iloc[i]['receiver'],weight=small_df.iloc[i][var],
                   line_color=small_df.iloc[i][var+'_grp'])

    edges, line_grps = zip(*nx.get_edge_attributes(g, 'line_color').items())
    line_grps = ['cornflowerblue' if x == 0 else 'navy' for x in line_grps]

    nodes, node_grps = zip(*nx.get_node_attributes(g, 'node_color').items())
    node_color_dict = {'1. Hub':'olivedrab','2. Scorer':'gold','3. Specialist':'firebrick'}
    node_grps = [node_color_dict[node] for node in node_grps]

    return small_df, g, line_grps, node_grps

def degree_centrality(g, analysis_df, var='pass_per_36'):
    # calculate degree centrality
    net_df = pd.DataFrame(data=nx.degree_centrality(g).items()).sort_values(by=1,ascending=False).round(2)
    net_df[2] = net_df[1].rank()
    net_df.columns = ['player',var+'_degree',var+'_degree_rank']

    return analysis_df.merge(net_df,how='left',on='player')

def team_stats(season,team,analysis_df,full_passes_made, var):
    # get team id
    team_id, player_ids = get_ids(season,team)
    # get team stats
    league_df = leaguedashteamstats.LeagueDashTeamStats(last_n_games='0',measure_type_detailed_defense='Advanced',month='0',
                                                    opponent_team_id='0',pace_adjust='N',per_mode_detailed='Totals',
                                                    period='0',plus_minus='N',rank='N',season=season,
                                                    season_type_all_star='Regular Season',vs_division_nullable='',
                                                    vs_conference_nullable='',season_segment_nullable='',outcome_nullable='',
                                                    location_nullable='',game_segment_nullable='',date_to_nullable='',
                                                    date_from_nullable='').get_data_frames()[0]

    team_df = league_df[league_df['TEAM_ID']==team_id][['PACE','OFF_RATING','AST_PCT']]

    # create output dataframe
    data = []
    data += (list(team_df.values[0]))
    data += list(pd.DataFrame(analysis_df['cluster1'].value_counts()).transpose().values[0])
    data += [pd.DataFrame(analysis_df['cluster1'].value_counts()).transpose().sum().sum()]
    data += [(full_passes_made['PASS'].sum()/team_df['PACE']).values[0]]
    data += [analysis_df[var].mean()]

    return pd.DataFrame(dict(zip(['PACE','OFF_RATINGS','AST_PCT','Hubs','Scorers','Specialists',
                                    'Players','Pass_Adjusted_Pace','pass_per_36_degree'],
                     data)),index=[team+' '+season])
