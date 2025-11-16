import pandas as pd
import nflreadpy as nfl
from pathlib import Path
import numpy as np

#define paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
ANALYZE_DIR = PROJECT_ROOT/"data" / "processed"

def read_raw_contracts_data():
    '''
    Read in the raw contracts data from the nflreadr repository., grab the columns we want and get rid of rows where the cols column is blank. 
    '''
    contracts_data_with_cols = (nfl.load_contracts()
                    .to_pandas()
                    [['player','gsis_id','otc_id','position','cols']]
                    .query('~cols.isnull()')
                        .reset_index(drop = True).reset_index()
                        )
    return contracts_data_with_cols

def process_cols_column_into_dataframe(contracts_data_with_cols):
    '''
    Take the cols column and process it into a dataframe
    '''

    big_concat = []
    for row in range(contracts_data_with_cols.shape[0]):
        little_concat = []
        for idx,item in enumerate(contracts_data_with_cols['cols'][row]):
            #We only need one per row
            little_concat.append(pd.DataFrame(item, index = [idx]).assign(merge_id = row))
            
        big_concat.append(pd.concat(little_concat))

    all_with_cols_concat = pd.concat(big_concat).query('team != "Total"')

    return all_with_cols_concat



def process_contracts_data(contracts_data_with_cols, all_with_cols_concat, contracts_position_mapping):
    '''
    
    combine the contracts data with the data pulled from the cols column. apply some preliminary cleaning
    '''
    contracts_merged_with_cols = (contracts_data_with_cols
                    .merge(all_with_cols_concat, how = 'left', left_on = 'index', right_on = 'merge_id')
                    .drop(['cols'],axis = 1)
                    .drop_duplicates()
                    [['player','otc_id','position','year','team','cap_percent']]
                    .assign(year_int = lambda df: df['year'].astype(int))
                    .query('year_int >= 2013 & year_int <= 2024')
                    .drop(['year_int'],axis = 1)
                    #account for washington team name change
                    .replace({'team' : {'Redskins' : 'Commanders','Washington' : 'Commanders'}})
                    #standardize position names
                    .merge(contracts_position_mapping, how = 'left', left_on = 'position', right_on = 'contracts_data_position')
                    .drop(['position','contracts_data_position'],axis = 1)
                    .rename({'position_mapping': 'position'}, axis = 1)
                    ).drop_duplicates()
    number_of_positions_per_player_year = contracts_merged_with_cols.groupby(['year','otc_id']).count().reset_index()[['year','otc_id','cap_percent']].drop_duplicates().rename({'cap_percent' : 'num_positions'},axis = 1)

    contracts_pos_std = (contracts_merged_with_cols
                     .merge(number_of_positions_per_player_year, how = 'left', on = ['year', 'otc_id'])
                     .assign(cap_percent_std = lambda df: df['cap_percent']/df['num_positions'])
                    )
    
    
    vet_dead_other_correction = (contracts_pos_std
                             .drop(['player','otc_id','position'],axis = 1)
                             .groupby(['year','team'])
                             .sum()
                             .reset_index()
                             .assign(cap_percent_std = lambda df: 1 -df['cap_percent_std'])
                             .assign(player = 'vet_deadcap_other_alloc')
                             .assign(num_positions = 1)
                             #VDU = Vet, Dead cap, unused
                             .assign(position = 'VDU')
                   
                             )

    contracts_pos_std_corrected = (pd.concat([contracts_pos_std, vet_dead_other_correction]).drop(['cap_percent'],axis = 1)
                                .assign(cap_pct_team = lambda df: df['cap_percent_std'],
                                        cap_pct_lg = lambda df: df['cap_pct_team']/32
                                        
                                        )
                                .drop(['cap_percent_std'],axis =1)
                                )

    return contracts_pos_std_corrected

def export_data_without_cols_column():
    '''
    Look at the data that does not have the cols column to make sure that it is not needed
    
    '''
    without_cols_column = (nfl.load_contracts()
                  .to_pandas()
                  #[['player','gsis_id','otc_id','position','cols']]
                  .query('cols.isnull()')
                    .reset_index(drop = True).reset_index()
                    )

    inspection = without_cols_column[(without_cols_column['year_signed'] != 2025) & (without_cols_column['year_signed'] >= 2013)].sort_values(by = 'guaranteed')

    inspection.to_excel(ANALYZE_DIR / "data_without_cols.xlsx",index = False)

def process_drafts_data(draft_team_mapping, draft_position_mapping):
    '''
    Load in the raw drafts data, standardize the team and positions, and create the columns we need
    
    '''
    drafts_data = (nfl.load_draft_picks().to_pandas()
               [['season','round','pick','team','gsis_id','pfr_player_name','position','category','side']]
               .query('season >= 2013 & season < 2025')
                )
    #look up table for teams

    fs_val = pd.read_html(DATA_DIR / r"raw" / r"fitz_spieldberg_trade_values.html", flavor = 'lxml')[0]
    pv_concat_lst = [fs_val[['Pick','Value']]] + [fs_val[[f'Pick.{i}',f'Value.{i}']].rename({f'Pick.{i}': 'Pick', f'Value.{i}':'Value'},axis = 1).dropna() for i in range(1,8)] 

    fs_val_full = pd.concat(pv_concat_lst).rename({'Value' : 'fs_val', 'Pick' : 'pick'}, axis = 1).dropna().astype({'pick':int})

    drafts_w_fs_val = (drafts_data
                    .merge(fs_val_full, how = 'left', on = 'pick')
                    .fillna({'fs_val' : 190.0})
                    .merge(draft_team_mapping, how = 'left', left_on = 'team', right_on = 'DraftTeamAbv')
                    .rename({'season' : 'year'}, axis = 1)
                    .merge(draft_position_mapping, how = 'left', left_on = 'position',right_on = 'draft_data_position')
                        .drop(['draft_data_position','position','category','side_x','side_y'],axis = 1)
                        .rename({'position_mapping':'position'},axis = 1)
                    )
    #Compute how much draft capital the nfl spent each year
    nfl_total_capital_year = drafts_w_fs_val[['year','pick','fs_val']].groupby('year').sum().reset_index()[['year','fs_val']].rename({'fs_val': 'total_nfl_draft_value'},axis = 1)
    #calculate how much draft capital each team had each year
    team_total_capital_year = drafts_w_fs_val[['year','pick','fs_val','TeamID']].groupby(['year','TeamID']).sum().reset_index()[['year','TeamID','fs_val']].rename({'fs_val': 'total_team_draft_value'},axis = 1)

    drafts_val_normalized = (drafts_w_fs_val
                            .merge(nfl_total_capital_year, how = 'left', on = 'year')
                            .merge(team_total_capital_year, how = 'left', on = ['year','TeamID'])
                            .assign(draft_pct_lg = lambda df: df['fs_val']/df['total_nfl_draft_value'])
                            .assign(draft_pct_team = lambda df: df['fs_val']/df['total_team_draft_value'])
                            .astype({'year' : str})
                            )

    
    return drafts_val_normalized

def export_analysis_data_sets(draft_team_mapping, cleaned_contracts, drafts_value, path):
    '''
    export final data sets for analysis
    
    '''
    cap_percent_by_position_year = cleaned_contracts.drop(['player','otc_id','team','num_positions'],axis = 1).groupby(['year','position']).sum().reset_index()


    cap_percent_by_position_team_year = cleaned_contracts.drop(['player','otc_id','num_positions'],axis = 1).groupby(['year','team','position']).sum().reset_index()

    draft_pct_by_position_year = drafts_value[['year','position','draft_pct_lg']].groupby(['year','position']).sum().reset_index()

    draft_pct_by_position_team_year = drafts_value[['year','position','TeamID','draft_pct_lg','draft_pct_team']].groupby(['year','position','TeamID']).sum().reset_index()

    #joined datasets fr actual analysis:
    capital_by_position_year = draft_pct_by_position_year.merge(cap_percent_by_position_year, how = 'left', on = ['year','position'])
    capital_by_position_team_year = (draft_pct_by_position_team_year
                                    .merge(cap_percent_by_position_team_year.merge(draft_team_mapping[['CapTeam','TeamID']].drop_duplicates(), how = 'left', left_on = 'team', right_on = 'CapTeam')
                                            , how = 'outer', on = ['year','position','TeamID'],indicator = False)
                                    .fillna({'draft_pct_lg' : 0,'draft_pct_team' : 0})
                                    .drop(['TeamID','CapTeam'],axis = 1)
                                    )
    
    capital_by_position_year.to_csv(path / 'capital_by_position_year.csv',index = False)
    capital_by_position_team_year.to_csv(path / 'capital_by_position_team_year.csv', index = False)

def get_result_count(df, col, result_name):
        '''
        Helper function to get results for each team
        
        '''
        return (df
                .groupby([col, 'season'])
                .count()
                .reset_index()[[col,'season', 'week']]
                .rename({col:'team', 'week':result_name},axis = 1)
                .query('team != "TIE"')
                )

def create_wins_data(team_mapping_wins, path):
    '''
    Process the wins data
    '''
    wins_data = (nfl.load_schedules().to_pandas()
            .query('season >= 2013 & season < 2025 & game_type == "REG"')
            # .assign(home_team = lambda df: df.merge(team_mapping_wins, how = 'inner', left_on = 'home_team', right_on = 'WinsTeamAbv')['Team'].values.tolist())
            # .assign(away_team = lambda df: df.merge(team_mapping_wins, how = 'inner', left_on = 'away_team', right_on = 'WinsTeamAbv')['Team'].values.tolist())
            .assign(winning_team = lambda df: np.where(df['result'] > 0, df['home_team'],
                                                       np.where(df['result'] < 0, df['away_team'],
                                                       'TIE')
                                                       )
            )
            .assign(losing_team = lambda df: np.where(df['result'] > 0, df['away_team'],
                                                       np.where(df['result'] < 0, df['home_team'],
                                                       'TIE')
                                                       ))

            .assign(tie_team_1 = lambda df: np.where((df['winning_team'] == 'TIE'), df['home_team'], np.nan))
            .assign(tie_team_2 = lambda df: np.where((df['losing_team'] == 'TIE'), df['away_team'], np.nan))

            )[['season','week','winning_team','losing_team','tie_team_1','tie_team_2']]

    

    #have to do losses outer because of the browns
    win_pct_df = (get_result_count(wins_data, 'winning_team', 'wins')
                .merge(get_result_count(wins_data, 'losing_team', 'losses'), how = 'outer', on = ['team','season'])
                .merge(get_result_count(wins_data,'tie_team_1', 'tie_1'), how = 'left', on = ['team','season'])
                .merge(get_result_count(wins_data,'tie_team_2', 'tie_2'), how = 'left', on = ['team','season'])
                .fillna(0)
                .assign(tie = lambda df: df['tie_1'] + df['tie_2'])
                .drop(['tie_1','tie_2'],axis = 1)
                .assign(win_pct = lambda df: (df['wins']+ 0.5*df['tie'])/(df['wins']+ df['losses']+ df['tie']))
                .merge(team_mapping_wins, how = 'left', left_on = 'team', right_on = 'WinsTeamAbv')
                .drop(['WinsTeamAbv', 'team', 'wins','losses','tie'],axis = 1)
                .rename({'season':'year'}, axis = 1)
                )
    
    win_pct_df.to_csv(path / 'win_pct_season.csv',index = False)

def main():
    #read in manually created data sets
    position_mapping_dict = pd.read_excel(DATA_DIR / "helper_tables" / "position_mapping.xlsx", sheet_name=None)
    draft_position_mapping = position_mapping_dict['drafts_data']
    contracts_position_mapping = position_mapping_dict['contracts_data']
    draft_team_mapping = pd.read_excel(DATA_DIR / "helper_tables" /'draft_team_mapping.xlsx')
    team_mapping_wins = pd.read_excel(DATA_DIR / "helper_tables" /'team_mapping_wins.xlsx')

    raw_contracts_data = read_raw_contracts_data()
    processed_cols_column = process_cols_column_into_dataframe(raw_contracts_data)
    cleaned_contracts = process_contracts_data(raw_contracts_data, processed_cols_column, contracts_position_mapping)
    #Export data without cols column for analysis
    export_data_without_cols_column()
    drafts_value = process_drafts_data(draft_team_mapping, draft_position_mapping)
    export_analysis_data_sets(draft_team_mapping, cleaned_contracts, drafts_value, ANALYZE_DIR)
    create_wins_data(team_mapping_wins, ANALYZE_DIR)

if __name__ == "__main__":
    main()




