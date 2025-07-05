# Simple Supabase Connection and Data Loading Function
import os
import pandas as pd
import numpy as np
from supabase import create_client, Client
from dotenv import load_dotenv
import re
from collections import Counter

def load_player_data(table_name="big5_players_comprehensive"):
    """
    Load and process Big 5 leagues player data from Supabase.
    
    Args:
        table_name (str): Name of the Supabase table to load data from
        
    Returns:
        pd.DataFrame: Processed dataframe with cleaned column names
    """
    # Load environment variables
    load_dotenv()
    
    # Get Supabase credentials
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    # Create Supabase client
    supabase: Client = create_client(url, key)
    
    # Load data from Supabase
    try:
        # First, get the total count
        count_response = supabase.table(table_name).select("*", count="exact").execute()
        total_count = count_response.count
        print(f"📊 Total records in '{table_name}': {total_count}")
        
        # Use pagination if more than 1000 records
        if total_count > 1000:
            print("🔄 Using pagination to load all data...")
            all_data = []
            batch_size = 1000
            
            for start in range(0, total_count, batch_size):
                end = min(start + batch_size - 1, total_count - 1)
                print(f"   Loading rows {start} to {end}...")
                
                batch_response = supabase.table(table_name).select("*").range(start, end).execute()
                all_data.extend(batch_response.data)
            
            df = pd.DataFrame(all_data)
        else:
            # Load all data with high limit
            response = supabase.table(table_name).select("*").limit(total_count).execute()
            df = pd.DataFrame(response.data)
            
            # If we got less than expected, retry with pagination
            if len(df) < total_count:
                print(f"⚠️  Only loaded {len(df)} of {total_count} rows. Retrying with pagination...")
                all_data = []
                batch_size = 1000
                
                for start in range(0, total_count, batch_size):
                    end = min(start + batch_size - 1, total_count - 1)
                    print(f"   Loading rows {start} to {end}...")
                    
                    batch_response = supabase.table(table_name).select("*").range(start, end).execute()
                    all_data.extend(batch_response.data)
                
                df = pd.DataFrame(all_data)
        
        print(f"✅ Loaded {len(df)} rows from '{table_name}'")
        
    except Exception as e:
        print(f"❌ Error loading from '{table_name}': {e}")
        return pd.DataFrame()
    
    if df.empty:
        print("❌ No data loaded. Check your Supabase connection and table name.")
        return pd.DataFrame()
    
    print(f"\nTable shape: {df.shape}")
    
    # Clean up unnamed columns - but avoid creating duplicates
    rename_dict = {}
    new_names_used = set()
    
    for col in df.columns:
        if "unnamed" in col.lower():
            original_col = col
            new_col = col.strip('_')
            new_col = new_col.split('_')[-1]
            
            # If this new name would create a duplicate, make it unique
            if new_col in new_names_used or new_col in df.columns:
                new_col = f"{new_col}_{col.split('_')[2]}"  # Use the number part to make it unique
            
            rename_dict[original_col] = new_col
            new_names_used.add(new_col)
    
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)
    
    # Apply FBRef column tidying - simple rename using dictionary
    COLUMN_MAPPING = {
        # identifiers & meta
        "player_id": "player_id", "player": "player", "pos": "position", "squad": "club", 
        "comp": "league", "scraped_at": "loaded_utc",
        
        # playing-time
        "playing_time_mp": "matches_played", "playing_time_starts": "starts", 
        "playing_time_min": "minutes_played", "playing_time_mn_mp": "minutes_per_match",
        "playing_time_min_pct": "minutes_share%", "playing_time_90s": "90s",
        
        # starts & subs
        "starts_starts": "started", "starts_mn_start": "minutes_per_start", 
        "starts_compl": "complete_matches", "subs_subs": "subbed_on", 
        "subs_mn_sub": "minutes_per_sub", "subs_unsub": "unused_sub",
        
        # keeper performance
        "performance_ga": "goals_allowed", "performance_ga90": "goals_allowed/90",
        "performance_sota": "shots_on_target_against", "performance_saves": "saves_made",
        "performance_save_pct": "save%", "performance_w": "wins", "performance_d": "draws",
        "performance_l": "losses", "performance_cs": "clean_sheets", 
        "performance_cs_pct": "clean_sheet%",
        
        # keeper penalties
        "penalty_kicks_pkatt": "pens_faced", "penalty_kicks_pka": "pens_conceded",
        "penalty_kicks_pksv": "pens_saved", "penalty_kicks_pkm": "pens_missed_by_opponents",
        "penalty_kicks_save_pct": "pen_save%",
        
        # keeper advanced xG
        "expected_psxg": "post_shot_xg", "expected_psxg_sot": "psxg_per_shot_on_target",
        "expected_psxgplus__": "psxg_minus_goals", "expected__90": "psxg_minus_goals/90",
        
        # keeper distribution
        "launched_cmp": "launched_passes_completed", "launched_att": "long_passes_attempted",
        "launched_cmp_pct": "long_pass_completion%", "passes_att_gk": "passes_attempted_gk",
        "passes_thr": "throws_attempted", "passes_launch_pct": "launch_pass%",
        "passes_avglen": "avg_pass_length", "goal_kicks_att": "goal_kicks",
        "goal_kicks_launch_pct": "goal_kick_launch%", "goal_kicks_avglen": "avg_goal_kick_length",
        "crosses_opp": "opponent_crosses", "crosses_stp": "crosses_stopped",
        "crosses_stp_pct": "cross_stop%", "sweeper_opa": "sweeper_actions_outside_area",
        "sweeper_opa_90": "sweeper_actions/90", "sweeper_avgdist": "avg_sweeper_distance",
        
        # outfield shooting
        "standard_gls": "goals", "standard_sh": "shots", "standard_sot": "shots_on_target",
        "standard_sot_pct": "shot_ot%", "standard_sh_90": "shots/90", 
        "standard_sot_90": "shots_ot/90", "standard_g_sh": "goals_per_shot",
        "standard_g_sot": "goals_per_shot_ot", "standard_dist": "avg_shot_distance",
        "standard_fk": "free_kick_goals", "standard_pk": "penalty_goals", 
        "standard_pkatt": "pens_taken",
        
        # expected goals/assists
        "expected_xg": "xG", "expected_npxg": "NPxG", "expected_npxg_sh": "NPxG_per_shot",
        "expected_g_xg": "G_minus_xG", "expected_np_g_xg": "NPG_minus_NPxG",
        "xag": "xA", "expected_xa": "xA_chance", "expected_a_xag": "A_minus_xA",
        
        # passing
        "total_cmp": "passes_completed", "total_att": "passes_attempted", 
        "total_cmp_pct": "pass_completion%", "total_totdist": "total_pass_distance",
        "total_prgdist": "progressive_pass_distance", "short_cmp": "short_passes_completed",
        "short_att": "short_passes_attempted", "short_cmp_pct": "short_pass_completion%",
        "medium_cmp": "medium_passes_completed", "medium_att": "medium_passes_attempted",
        "medium_cmp_pct": "medium_pass_completion%", "long_cmp": "long_passes_completed",
        "long_att": "long_passes_attempted", "long_cmp_pct": "long_pass_completion%",
        "ast": "assists", "kp": "key_passes", "3": "passes_into_final_third",
        "ppa": "passes_into_pen_area", "crspa": "crosses_into_pen_area", 
        "prgp": "progressive_passes",
        
        # tackling & challenges
        "tackles_tkl": "tackles", "tackles_tklw": "tackles_won", "tackles_def_3rd": "tackles_def_3rd",
        "tackles_mid_3rd": "tackles_mid_3rd", "tackles_att_3rd": "tackles_att_3rd",
        "challenges_tkl": "dribbles_tackled", "challenges_att": "dribbles_challenged_against",
        "challenges_tkl_pct": "dribbles_tackled%", "challenges_lost": "tackles_lost",
        
        # blocking & interceptions
        "blocks_blocks": "blocks", "blocks_sh": "shot_blocks", "blocks_pass": "pass_blocks",
        "int": "interceptions", "tklplusint": "tackles+interceptions", "clr": "clearances",
        "err": "errors_leading_to_shot",
        
        # ball control & carrying
        "touches_touches": "touches", "touches_def_pen": "touches_def_pen_area",
        "touches_def_3rd": "touches_def_3rd", "touches_mid_3rd": "touches_mid_3rd",
        "touches_att_3rd": "touches_att_3rd", "touches_att_pen": "touches_att_pen_area",
        "touches_live": "touches_live", "take_ons_att": "take_ons_attempted",
        "take_ons_succ": "take_ons_successful", "take_ons_succ_pct": "take_ons_success%",
        "take_ons_tkld": "times_tackled", "take_ons_tkld_pct": "tackled%",
        "carries_carries": "carries", "carries_totdist": "carry_distance",
        "carries_prgdist": "carry_progressive_distance", "carries_prgc": "progressive_carries",
        "carries_1_3": "carries_into_final_third", "carries_cpa": "carries_into_pen_area",
        "carries_mis": "miscontrols", "carries_dis": "dispossessed", 
        "receiving_rec": "passes_received", "receiving_prgr": "progressive_passes_received",
        
        # shot/goal-creating actions
        "sca_sca": "sca_total", "sca_sca90": "sca/90", "sca_types_passlive": "sca_live_pass",
        "sca_types_passdead": "sca_dead_pass", "sca_types_to": "sca_take_on",
        "sca_types_sh": "sca_shot", "sca_types_fld": "sca_fouled", "sca_types_def": "sca_defensive",
        "gca_gca": "gca_total", "gca_gca90": "gca/90", "gca_types_passlive": "gca_live_pass",
        "gca_types_passdead": "gca_dead_pass", "gca_types_to": "gca_take_on",
        "gca_types_sh": "gca_shot", "gca_types_fld": "gca_fouled", "gca_types_def": "gca_defensive",
        
        # disciplinary & misc
        "performance_crdy": "yellow_cards", "performance_crdr": "red_cards",
        "performance_2crdy": "second_yellow_cards", "performance_fls": "fouls_committed",
        "performance_fld": "fouls_drawn", "performance_off": "offsides",
        "performance_crs": "crosses", "performance_int": "interceptions_misc",
        "performance_tklw": "tackles_won_misc", "performance_pkwon": "pens_won",
        "performance_pkcon": "pens_conceded", "performance_og": "own_goals",
        "performance_recov": "ball_recoveries",
        
        # aerial duels
        "aerial_duels_won": "aerials_won", "aerial_duels_lost": "aerials_lost",
        "aerial_duels_won_pct": "aerials_won%",
        
        # team impact
        "team_success_ppm": "team_points_per_match", "team_success_ong": "team_goals_scored_on_pitch",
        "team_success_onga": "team_goals_allowed_on_pitch", "team_success_plus__": "team_goals_plus_minus",
        "team_success_plus__90": "team_plus_minus_per90", "team_success_on_off": "team_on_off_goals_diff",
        "team_success_xg_onxg": "team_xg_scored_on_pitch", "team_success_xg_onxga": "team_xg_allowed_on_pitch",
        "team_success_xg_xgplus__": "team_xg_plus_minus", "team_success_xg_xgplus__90": "team_xg_plus_minus_per90",
        "team_success_xg_on_off": "team_on_off_xg_diff"
    }
    
    # Simple rename - only rename columns that exist in the mapping
    df = df.rename(columns=COLUMN_MAPPING, errors="ignore")
    
    # Handle duplicate column names - but be smart about which ones to keep
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        
        # Show all occurrences of duplicate column names
        col_counts = Counter(df.columns)
        duplicated_names = {name: count for name, count in col_counts.items() if count > 1}
        print(f"Column name frequencies: {duplicated_names}")
        
        # For 90s column specifically, keep the one that came from playing_time_90s
        if '90s' in duplicated_names:
            # Get all 90s column positions
            nineties_positions = [i for i, col in enumerate(df.columns) if col == '90s']
            
            # Find the one with the most non-zero values (likely the correct one)
            best_90s_idx = None
            max_non_zero = 0
            
            for pos in nineties_positions:
                non_zero_count = (df.iloc[:, pos] > 0).sum()
                if non_zero_count > max_non_zero:
                    max_non_zero = non_zero_count
                    best_90s_idx = pos
            
            if best_90s_idx is not None:
                # Keep only the best 90s column
                cols_to_keep = []
                nineties_kept = False
                for i, col in enumerate(df.columns):
                    if col == '90s':
                        if i == best_90s_idx and not nineties_kept:
                            cols_to_keep.append(True)
                            nineties_kept = True
                        else:
                            cols_to_keep.append(False)
                    else:
                        cols_to_keep.append(True)
                
                df = df.loc[:, cols_to_keep]
        
        # Remove remaining duplicate columns (keep first occurrence)
        df = df.loc[:, ~df.columns.duplicated()]
    else:
        print("\nNo duplicate column names found.")
    
    # Fill missing values more comprehensively
    # First, replace various representations of missing values
    df = df.replace(['', 'NaN', 'nan', 'NULL', 'null', 'None', 'none'], np.nan)
    
    # Then fill remaining NaN/NA values with 0 for numeric columns, empty string for text columns
    for col in df.columns:
        if df[col].dtype in ['object', 'string']:
            # For text columns, fill with empty string
            df[col] = df[col].fillna('')
        else:
            # For numeric columns, fill with 0
            df[col] = df[col].fillna(0)
    
    return df