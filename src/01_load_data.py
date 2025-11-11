import pandas as pd
import nflreadpy as nfl
from pathlib import Path

#define paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PROCESS_DIR = PROJECT_ROOT /"data" / "processed"

#read in manually created data sets
position_mapping_dict = pd.read_excel(DATA_DIR / "helper_tables" / "position_mapping.xlsx", sheet_name=None)
draft_position_mapping = position_mapping_dict['drafts_data']
contracts_position_mapping = position_mapping_dict['contracts_data']

def read_raw_contracts_data():
    '''
    
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



def process_contracts_data(contracts_data_with_cols, all_with_cols_concat):
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




if __name__ == '__main__':
    raw_contracts_data = read_raw_contracts_data()
    processed_cols_column = process_cols_column_into_dataframe(raw_contracts_data)
    cleaned_contracts = process_contracts_data(raw_contracts_data, processed_cols_column)
    