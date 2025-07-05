# Simple Supabase Connection
import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import re

def load_data(table_name, limit=None, use_pagination=None):
    """Load all data from a Supabase table"""
    # Load environment variables
    load_dotenv()
    
    # Get Supabase credentials
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    
    # Create Supabase client
    supabase: Client = create_client(url, key)
    
    try:
        if limit is None:
            # First, get the total count
            count_response = supabase.table(table_name).select("*", count="exact").execute()
            total_count = count_response.count
            print(f"📊 Total records in '{table_name}': {total_count}")
            
            # Auto-enable pagination if more than 1000 records (unless explicitly disabled)
            if use_pagination is None:
                use_pagination = total_count > 1000
            
            if use_pagination and total_count > 1000:
                # Use pagination for large datasets
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
                # Try to load all data with a high limit (but Supabase may still cap it)
                response = supabase.table(table_name).select("*").limit(total_count).execute()
                df = pd.DataFrame(response.data)
                
                # If we got less than expected, automatically retry with pagination
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
        else:
            response = supabase.table(table_name).select("*").limit(limit).execute()
            df = pd.DataFrame(response.data)
        
        print(f"✅ Loaded {len(df)} rows from '{table_name}'")
        return df
    except Exception as e:
        print(f"❌ Error loading from '{table_name}': {e}")
        return pd.DataFrame()

def tidy_fbref_columns(df):
    """
    Rename columns from FBref summary tables to the canonical names you specified.

    ── How it works ──
    1. If the table came in as a MultiIndex (two header rows), join the
       levels with an underscore, e.g. ('Performance', 'GA') → 'Performance_GA'.
    2. Normalise the string: lower-case, strip spaces, 'cmp%' → 'cmp_pct', etc.
    3. Apply a dictionary of specific fixes where FBref's label is ambiguous.
    """
    # 1 ▸ flatten a possible MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            "_".join([str(x) for x in tup if x and x != "Unnamed: 0_level_0"]).strip()
            for tup in df.columns
        ]

    # 2 ▸ generic normalisation rules
    def _norm(col):
        col = col.lower()

        # percentage signs → _pct
        col = col.replace("%", "_pct")

        # spaces, slashes, dashes → underscore
        col = re.sub(r"[\\s/–-]+", "_", col)

        # tidy brackets
        col = col.replace(")", "").replace("(", "")

        return col

    df.columns = [_norm(c) for c in df.columns]

    # 3 ▸ targeted remaps to your exact list
    SPECIFIC = {
    # ── identifiers & meta ────────────────────────────────────────────────
    "player_id":            "player_id",
    "player":               "player",
    "pos":                  "position",
    "squad":                "club",
    "comp":                 "league",
    "scraped_at":           "loaded_utc",

    # ── global playing-time (goalkeepers & outfield) ──────────────────────
    "playing_time_mp":      "matches_played",
    "playing_time_starts":  "starts",
    "playing_time_min":     "minutes_played",              # generic matches col
    "playing_time_mn_mp":   "minutes_per_match",
    "playing_time_min_pct": "minutes_share%",
    "playing_time_90s":     "90s",

    # ── starts & subs break-down ───────────────────────────────────────────
    "starts_starts":        "started",
    "starts_mn_start":      "minutes_per_start",
    "starts_compl":         "complete_matches",
    "subs_subs":            "subbed_on",
    "subs_mn_sub":          "minutes_per_sub",
    "subs_unsub":           "unused_sub",

    # ── keeper performance (shot stopping) ────────────────────────────────
    "performance_ga":       "goals_allowed",
    "performance_ga90":     "goals_allowed/90",
    "performance_sota":     "shots_on_target_against",
    "performance_saves":    "saves_made",
    "performance_save_pct": "save%",
    "performance_w":        "wins",
    "performance_d":        "draws",
    "performance_l":        "losses",
    "performance_cs":       "clean_sheets",
    "performance_cs_pct":   "clean_sheet%",

    # ── keeper penalties ──────────────────────────────────────────────────
    "penalty_kicks_pkatt":      "pens_faced",
    "penalty_kicks_pka":        "pens_conceded",
    "penalty_kicks_pksv":       "pens_saved",
    "penalty_kicks_pkm":        "pens_missed_by_opponents",
    "penalty_kicks_save_pct":   "pen_save%",

    # ── keeper advanced xG ────────────────────────────────────────────────
    "expected_psxg":            "post_shot_xg",
    "expected_psxg_sot":        "psxg_per_shot_on_target",
    "expected_psxgplus__":      "psxg_minus_goals",
    "expected__90":             "psxg_minus_goals/90",

    # ── keeper distribution & sweeping ────────────────────────────────────
    "launched_cmp":         "launched_passes_completed",
    "launched_att":         "long_passes_attempted",
    "launched_cmp_pct":     "long_pass_completion%",
    "passes_att_gk":        "passes_attempted_gk",
    "passes_thr":           "throws_attempted",
    "passes_launch_pct":    "launch_pass%",
    "passes_avglen":        "avg_pass_length",
    "goal_kicks_att":       "goal_kicks",
    "goal_kicks_launch_pct":"goal_kick_launch%",
    "goal_kicks_avglen":    "avg_goal_kick_length",
    "crosses_opp":          "opponent_crosses",
    "crosses_stp":          "crosses_stopped",
    "crosses_stp_pct":      "cross_stop%",
    "sweeper_opa":          "sweeper_actions_outside_area",
    "sweeper_opa_90":       "sweeper_actions/90",
    "sweeper_avgdist":      "avg_sweeper_distance",

    # ── keeper goals conceded detail ──────────────────────────────────────
    "goals_ga":             "goals_allowed",
    "goals_pka":            "penalty_goals_allowed",
    "goals_fk":             "free_kick_goals_allowed",
    "goals_ck":             "corner_goals_allowed",
    "goals_og":             "own_goals_ne_vtu_storonu_voyuesh",

    # ── outfield shooting – counting & per-90 ─────────────────────────────
    "standard_gls":         "goals",
    "standard_sh":          "shots",
    "standard_sot":         "shots_on_target",
    "standard_sot_pct":     "shot_ot%",
    "standard_sh_90":       "shots/90",
    "standard_sot_90":      "shots_ot/90",
    "standard_g_sh":        "goals_per_shot",
    "standard_g_sot":       "goals_per_shot_ot",
    "standard_dist":        "avg_shot_distance",
    "standard_fk":          "free_kick_goals",
    "standard_pk":          "penalty_goals",
    "standard_pkatt":       "pens_taken",

    # ── outfield expected goals / assists ─────────────────────────────────
    "expected_xg":          "xG",
    "expected_npxg":        "NPxG",
    "expected_npxg_sh":     "NPxG_per_shot",
    "expected_g_xg":        "G_minus_xG",
    "expected_np_g_xg":     "NPG_minus_NPxG",
    "xag":                  "xA",
    "expected_xa":          "xA_chance",
    "expected_a_xag":       "A_minus_xA",

    # ── passing totals & length splits ────────────────────────────────────
    "total_cmp":            "passes_completed",
    "total_att":            "passes_attempted",
    "total_cmp_pct":        "pass_completion%",
    "total_totdist":        "total_pass_distance",
    "total_prgdist":        "progressive_pass_distance",
    "short_cmp":            "short_passes_completed",
    "short_att":            "short_passes_attempted",
    "short_cmp_pct":        "short_pass_completion%",
    "medium_cmp":           "medium_passes_completed",
    "medium_att":           "medium_passes_attempted",
    "medium_cmp_pct":       "medium_pass_completion%",
    "long_cmp":             "long_passes_completed",
    "long_att":             "long_passes_attempted",
    "long_cmp_pct":         "long_pass_completion%",

    # ── creative passing & progression ────────────────────────────────────
    "ast":                  "assists",
    "kp":                   "key_passes",
    "3":                    "passes_into_final_third",
    "ppa":                  "passes_into_pen_area",
    "crspa":                "crosses_into_pen_area",
    "prgp":                 "progressive_passes",

    # ── tackling & challenges ─────────────────────────────────────────────
    "tackles_tkl":          "tackles",
    "tackles_tklw":         "tackles_won",
    "tackles_def_3rd":      "tackles_def_3rd",
    "tackles_mid_3rd":      "tackles_mid_3rd",
    "tackles_att_3rd":      "tackles_att_3rd",
    "challenges_tkl":       "dribbles_tackled",
    "challenges_att":       "dribbles_challenged_against",
    "challenges_tkl_pct":   "dribbles_tackled%",
    "challenges_lost":      "tackles_lost",

    # ── blocking & interceptions ──────────────────────────────────────────
    "blocks_blocks":        "blocks",
    "blocks_sh":            "shot_blocks",
    "blocks_pass":          "pass_blocks",
    "int":                  "interceptions",
    "tklplusint":           "tackles+interceptions",
    "clr":                  "clearances",
    "err":                  "errors_leading_to_shot",

    # ── ball control & carrying ───────────────────────────────────────────
    "touches_touches":      "touches",
    "touches_def_pen":      "touches_def_pen_area",
    "touches_def_3rd":      "touches_def_3rd",
    "touches_mid_3rd":      "touches_mid_3rd",
    "touches_att_3rd":      "touches_att_3rd",
    "touches_att_pen":      "touches_att_pen_area",
    "touches_live":         "touches_live",
    "take_ons_att":         "take_ons_attempted",
    "take_ons_succ":        "take_ons_successful",
    "take_ons_succ_pct":    "take_ons_success%",
    "take_ons_tkld":        "times_tackled",
    "take_ons_tkld_pct":    "tackled%",
    "carries_carries":      "carries",
    "carries_totdist":      "carry_distance",
    "carries_prgdist":      "carry_progressive_distance",
    "carries_prgc":         "progressive_carries",
    "carries_1_3":          "carries_into_final_third",
    "carries_cpa":          "carries_into_pen_area",
    "carries_mis":          "miscontrols",
    "carries_dis":          "dispossessed",
    "receiving_rec":        "passes_received",
    "receiving_prgr":       "progressive_passes_received",

    # ── shot- & goal-creating actions ─────────────────────────────────────
    "sca_sca":              "sca_total",
    "sca_sca90":            "sca/90",
    "sca_types_passlive":   "sca_live_pass",
    "sca_types_passdead":   "sca_dead_pass",
    "sca_types_to":         "sca_take_on",
    "sca_types_sh":         "sca_shot",
    "sca_types_fld":        "sca_fouled",
    "sca_types_def":        "sca_defensive",
    "gca_gca":              "gca_total",
    "gca_gca90":            "gca/90",
    "gca_types_passlive":   "gca_live_pass",
    "gca_types_passdead":   "gca_dead_pass",
    "gca_types_to":         "gca_take_on",
    "gca_types_sh":         "gca_shot",
    "gca_types_fld":        "gca_fouled",
    "gca_types_def":        "gca_defensive",

    # ── pass types & corners ──────────────────────────────────────────────
    "att":                  "passes_attempted(could be)",
    "pass_types_live":      "live_passes",
    "pass_types_dead":      "dead_ball_passes",
    "pass_types_fk":        "free_kick_passes",
    "pass_types_tb":        "through_balls",
    "pass_types_sw":        "switches",
    "pass_types_crs":       "crosses",
    "pass_types_ti":        "throw_ins",
    "pass_types_ck":        "corner_kicks",
    "corner_kicks_in":      "corners_inswing",
    "corner_kicks_out":     "corners_outswing",
    "corner_kicks_str":     "corners_straight",
    "outcomes_cmp":         "passes_completed",
    "outcomes_off":         "passes_offside",
    "outcomes_blocks":      "passes_blocked",

    # ── disciplinary & misc ───────────────────────────────────────────────
    "performance_crdy":     "yellow_cards",
    "performance_crdr":     "red_cards",
    "performance_2crdy":    "second_yellow_cards",
    "performance_fls":      "fouls_committed",
    "performance_fld":      "fouls_drawn",
    "performance_off":      "offsides",
    "performance_crs":      "crosses",
    "performance_int":      "interceptions_misc",
    "performance_tklw":     "tackles_won_misc",
    "performance_pkwon":    "pens_won",
    "performance_pkcon":    "pens_conceded",
    "performance_og":       "own_goals",
    "performance_recov":    "ball_recoveries",

    # ── aerial duels ──────────────────────────────────────────────────────
    "aerial_duels_won":     "aerials_won",
    "aerial_duels_lost":    "aerials_lost",
    "aerial_duels_won_pct": "aerials_won%",

    # ── team on/off impact ────────────────────────────────────────────────
    "team_success_ppm":         "team_points_per_match",
    "team_success_ong":         "team_goals_scored_on_pitch",
    "team_success_onga":        "team_goals_allowed_on_pitch",
    "team_success_plus__":      "team_goals_plus_minus",
    "team_success_plus__90":    "team_plus_minus_per90",
    "team_success_on_off":      "team_on_off_goals_diff",
    "team_success_xg_onxg":     "team_xg_scored_on_pitch",
    "team_success_xg_onxga":    "team_xg_allowed_on_pitch",
    "team_success_xg_xgplus__": "team_xg_plus_minus",
    "team_success_xg_xgplus__90":"team_xg_plus_minus_per90",
    "team_success_xg_on_off":   "team_on_off_xg_diff",
}

    df = df.rename(columns=SPECIFIC, errors="ignore")

    # Finally, de-duplicate columns like 'matches', '90s' that appear several times
    deduped = (
        df.T.groupby(level=0)   # transpose → group duplicate row labels
          .first()              # keep the first non-NaN column
          .T
    )
    return deduped

def get_big5_players_data(table_name="big5_players_comprehensive"):
    """
    Main function to load and process Big 5 leagues player data from Supabase.
    
    Args:
        table_name (str): Name of the Supabase table to load data from
        
    Returns:
        pd.DataFrame: Processed dataframe with cleaned column names
    """
    # Load data from Supabase
    df = load_data(table_name)
    
    if df.empty:
        print("❌ No data loaded. Check your Supabase connection and table name.")
        return pd.DataFrame()
    
    print(f"\nTable shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Clean up unnamed columns
    rename_dict = {}
    for col in df.columns:
        if "unnamed" in col.lower():
            original_col = col
            new_col = col.strip('_')
            new_col = new_col.split('_')[-1]
            rename_dict[original_col] = new_col
    
    if rename_dict:
        df.rename(columns=rename_dict, inplace=True)
    
    # Apply FBRef column tidying
    df = tidy_fbref_columns(df)
    
    print(f"✅ Data processing complete. Final shape: {df.shape}")
    
    return df

# For backwards compatibility and testing
if __name__ == "__main__":
    # This will run if the file is executed directly
    df = get_big5_players_data()
    if not df.empty:
        print("\nFirst few rows:")
        print(df.head())