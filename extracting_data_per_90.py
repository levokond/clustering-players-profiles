import pandas as pd
import importlib
import extracting_data_per_90
importlib.reload(extracting_data_per_90)

def calculate_player_per90_stats(pl_2016):
    """
    Groups data by players, converts to per90 format and filters players with less than 500 minutes
    
    Args:
        pl_2016: DataFrame with event data
    
    Returns:
        DataFrame with per90 statistics for players with >= 500 minutes
    """
    
    # Create a copy of data to work with
    df = pl_2016.copy()
    
    # Get unique matches and players for playing time calculation
    matches = df['match_id'].unique()
    
    # Dictionary to store playing time for each player and their positions
    player_minutes = {}
    player_positions = {}
    
    # Go through each match
    for match_id in matches:
        match_data = df[df['match_id'] == match_id]
        
        # Get Starting XI data
        starting_xi_events = match_data[match_data['type'] == 'Starting XI']
        
        # Get substitution data
        substitution_events = match_data[match_data['type'] == 'Substitution']
        
        # For each team in the match
        for team_id in match_data['team_id'].unique():
            team_match_data = match_data[match_data['team_id'] == team_id]
            team_starting_xi = starting_xi_events[starting_xi_events['team_id'] == team_id]
            team_substitutions = substitution_events[substitution_events['team_id'] == team_id]
            
            # Get players from starting lineup
            if not team_starting_xi.empty:
                tactics_data = team_starting_xi.iloc[0]['tactics']
                if pd.notna(tactics_data) and isinstance(tactics_data, str):
                    import ast
                    try:
                        tactics_dict = ast.literal_eval(tactics_data)
                        if 'lineup' in tactics_dict:
                            for player_info in tactics_dict['lineup']:
                                player_id = player_info['player']['id']
                                player_name = player_info['player']['name']
                                player_position = player_info.get('position', {}).get('name', 'Unknown')
                                
                                # Initialize playing time (90 minutes by default)
                                if player_id not in player_minutes:
                                    player_minutes[player_id] = {'name': player_name, 'total_minutes': 0}
                                    player_positions[player_id] = []
                                
                                # Save player position
                                if player_position not in player_positions[player_id]:
                                    player_positions[player_id].append(player_position)
                                
                                # Check if player was substituted off
                                substituted_off = team_substitutions[
                                    team_substitutions['player_id'] == player_id
                                ]
                                
                                if not substituted_off.empty:
                                    # Player was substituted - take substitution minute
                                    sub_minute = substituted_off.iloc[0]['minute']
                                    player_minutes[player_id]['total_minutes'] += sub_minute
                                else:
                                    # Player played the full match
                                    player_minutes[player_id]['total_minutes'] += 90
                    except:
                        pass
            
            # Process players who came on as substitutes
            for _, sub_event in team_substitutions.iterrows():
                if pd.notna(sub_event.get('substitution_replacement_id')):
                    replacement_id = sub_event['substitution_replacement_id']
                    replacement_name = sub_event.get('substitution_replacement', 'Unknown')
                    sub_minute = sub_event['minute']
                    
                    if replacement_id not in player_minutes:
                        player_minutes[replacement_id] = {'name': replacement_name, 'total_minutes': 0}
                        player_positions[replacement_id] = []
                    
                    # Substitute player plays remaining time
                    player_minutes[replacement_id]['total_minutes'] += (90 - sub_minute)
    
    # Group data by players and calculate statistics
    player_stats = []
    
    print(f"Found players with playing time: {len(player_minutes)}")
    
    # Show distribution of playing time
    if player_minutes:
        minutes_list = [data['total_minutes'] for data in player_minutes.values()]
        print(f"Minimum time: {min(minutes_list)} minutes")
        print(f"Maximum time: {max(minutes_list)} minutes")
        print(f"Players with >= 500 minutes: {sum(1 for m in minutes_list if m >= 500)}")
        print(f"Players with >= 250 minutes: {sum(1 for m in minutes_list if m >= 250)}")
        print(f"Players with >= 90 minutes: {sum(1 for m in minutes_list if m >= 90)}")
    
    for player_id, time_data in player_minutes.items():
        if time_data['total_minutes'] >= 90:  # Temporarily lower threshold for testing
            player_events = df[df['player_id'] == player_id]
            
            if not player_events.empty:
                # Basic statistics
                stats = {
                    'player_id': player_id,
                    'player_name': time_data['name'],
                    'position': ', '.join(player_positions.get(player_id, ['Unknown'])),
                    'total_minutes': time_data['total_minutes'],
                    'matches_played': len(player_events['match_id'].unique()),
                }
                
                # Count different types of events
                event_counts = player_events['type'].value_counts()
                
                # Main statistics
                stats['passes'] = event_counts.get('Pass', 0)
                stats['shots'] = event_counts.get('Shot', 0)
                stats['dribbles'] = event_counts.get('Dribble', 0)
                stats['duels'] = event_counts.get('Duel', 0)
                stats['interceptions'] = event_counts.get('Interception', 0)
                stats['clearances'] = event_counts.get('Clearance', 0)
                stats['fouls_committed'] = event_counts.get('Foul Committed', 0)
                stats['fouls_won'] = event_counts.get('Foul Won', 0)
                stats['carries'] = event_counts.get('Carry', 0)
                
                # Successful passes
                successful_passes = player_events[
                    (player_events['type'] == 'Pass') & 
                    (player_events['pass_outcome'].isna())
                ].shape[0]
                stats['successful_passes'] = successful_passes
                
                # Successful dribbles
                successful_dribbles = player_events[
                    (player_events['type'] == 'Dribble') & 
                    (player_events['dribble_outcome'] == 'Complete')
                ].shape[0]
                stats['successful_dribbles'] = successful_dribbles
                
                # Goals
                goals = player_events[
                    (player_events['type'] == 'Shot') & 
                    (player_events['shot_outcome'] == 'Goal')
                ].shape[0]
                stats['goals'] = goals
                
                # NEW METRICS
                
                # xG (Expected Goals)
                xg_events = player_events[
                    (player_events['type'] == 'Shot') & 
                    (player_events['shot_statsbomb_xg'].notna())
                ]
                stats['xg'] = xg_events['shot_statsbomb_xg'].sum() if not xg_events.empty else 0
                
                # xA (Expected Assists) - from passes that led to shots
                xa_events = player_events[
                    (player_events['type'] == 'Pass') & 
                    (player_events['pass_shot_assist'].notna()) &
                    (player_events['pass_shot_assist'] == True)
                ]
                # Try to find xG for these shots
                stats['xa'] = 0
                for _, pass_event in xa_events.iterrows():
                    # Look for corresponding shot in the same match
                    match_shots = df[
                        (df['match_id'] == pass_event['match_id']) &
                        (df['type'] == 'Shot') &
                        (df['minute'] >= pass_event['minute']) &
                        (df['minute'] <= pass_event['minute'] + 1)  # Within a minute
                    ]
                    if not match_shots.empty:
                        # Take first shot after the pass
                        shot_xg = match_shots.iloc[0].get('shot_statsbomb_xg', 0)
                        if pd.notna(shot_xg):
                            stats['xa'] += shot_xg
                
                # Carries into final third
                carries_final_third = player_events[
                    (player_events['type'] == 'Carry') &
                    (player_events['carry_end_location_x'].notna()) &
                    (player_events['carry_end_location_x'] >= 80)  # Final third (80-120 on X axis)
                ].shape[0]
                stats['carries_final_third'] = carries_final_third
                
                # Zone-based touches
                all_touches = player_events[player_events['location_x'].notna()]
                
                # Touches in final third
                final_third_touches = all_touches[all_touches['location_x'] >= 80].shape[0]
                # Touches in middle third
                middle_third_touches = all_touches[
                    (all_touches['location_x'] >= 40) & 
                    (all_touches['location_x'] < 80)
                ].shape[0]
                # Touches in defensive third
                defensive_third_touches = all_touches[all_touches['location_x'] < 40].shape[0]
                
                total_touches = len(all_touches)
                
                stats['final_third_touches'] = final_third_touches
                stats['middle_third_touches'] = middle_third_touches
                stats['defensive_third_touches'] = defensive_third_touches
                stats['total_touches'] = total_touches
                
                # Percentage of touches in final third
                stats['final_third_touches_pct'] = (final_third_touches / total_touches * 100) if total_touches > 0 else 0
                
                # Pass length
                pass_events = player_events[
                    (player_events['type'] == 'Pass') &
                    (player_events['location_x'].notna()) &
                    (player_events['location_y'].notna()) &
                    (player_events['pass_end_location_x'].notna()) &
                    (player_events['pass_end_location_y'].notna())
                ]
                
                if not pass_events.empty:
                    # Calculate length of each pass
                    pass_lengths = []
                    for _, pass_event in pass_events.iterrows():
                        start_x, start_y = pass_event['location_x'], pass_event['location_y']
                        end_x, end_y = pass_event['pass_end_location_x'], pass_event['pass_end_location_y']
                        length = ((end_x - start_x)**2 + (end_y - start_y)**2)**0.5
                        pass_lengths.append(length)
                    
                    stats['avg_pass_length'] = sum(pass_lengths) / len(pass_lengths) if pass_lengths else 0
                    stats['total_pass_distance'] = sum(pass_lengths)
                else:
                    stats['avg_pass_length'] = 0
                    stats['total_pass_distance'] = 0
                
                # Progressive passes (passes that advance the ball towards opponent's goal)
                progressive_passes = pass_events[
                    (pass_events['location_x'] < pass_events['pass_end_location_x']) &  # Forward movement
                    ((pass_events['pass_end_location_x'] - pass_events['location_x']) >= 10)  # Minimum 10 meters forward
                ].shape[0]
                stats['progressive_passes'] = progressive_passes
                
                # Long passes (more than 25 meters)
                long_passes = 0
                for _, pass_event in pass_events.iterrows():
                    start_x, start_y = pass_event['location_x'], pass_event['location_y']
                    end_x, end_y = pass_event['pass_end_location_x'], pass_event['pass_end_location_y']
                    length = ((end_x - start_x)**2 + (end_y - start_y)**2)**0.5
                    if length >= 25:
                        long_passes += 1
                
                stats['long_passes'] = long_passes
                
                # Convert to per90
                minutes_factor = time_data['total_minutes'] / 90.0
                
                for key in stats.keys():
                    if key not in ['player_id', 'player_name', 'position', 'total_minutes', 'matches_played', 
                                   'final_third_touches_pct', 'avg_pass_length']:
                        stats[f'{key}_per90'] = stats[key] / minutes_factor if minutes_factor > 0 else 0
                
                # Percentage metrics
                if stats['passes'] > 0:
                    stats['pass_accuracy'] = (stats['successful_passes'] / stats['passes']) * 100
                else:
                    stats['pass_accuracy'] = 0
                
                if stats['dribbles'] > 0:
                    stats['dribble_success_rate'] = (stats['successful_dribbles'] / stats['dribbles']) * 100
                else:
                    stats['dribble_success_rate'] = 0
                
                # Progressive passes as percentage of all passes
                if stats['passes'] > 0:
                    stats['progressive_pass_pct'] = (stats['progressive_passes'] / stats['passes']) * 100
                else:
                    stats['progressive_pass_pct'] = 0
                
                player_stats.append(stats)
    
    # Create DataFrame
    result_df = pd.DataFrame(player_stats)
    
    # Check if there is data
    if result_df.empty:
        print("Warning: No players with >= 90 minutes of playing time")
        return result_df
    
    # Sort by total playing time
    if 'total_minutes' in result_df.columns:
        result_df = result_df.sort_values('total_minutes', ascending=False)
    
    return result_df

df = calculate_player_per90_stats(pl_2016)
print("Result size:", df.shape)
if not df.empty:
    df.head()