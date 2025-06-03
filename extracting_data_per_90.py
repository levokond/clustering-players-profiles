import pandas as pd

def calculate_player_per90_stats(pl_2016):
    """
    Группирует данные по игрокам, конвертирует в формат per90 и отфильтровывает игроков с менее чем 500 минутами
    
    Args:
        pl_2016: DataFrame с данными о событиях
    
    Returns:
        DataFrame с статистикой per90 для игроков с >= 500 минутами
    """
    
    
    # Создаем копию данных для работы
    df = pl_2016.copy()
    
    # Получаем уникальные матчи и игроков для расчета времени игры
    matches = df['match_id'].unique()
    
    # Словарь для хранения времени игры каждого игрока и их позиций
    player_minutes = {}
    player_positions = {}
    
    # Проходим по каждому матчу
    for match_id in matches:
        match_data = df[df['match_id'] == match_id]
        
        # Получаем данные о составах (Starting XI)
        starting_xi_events = match_data[match_data['type'] == 'Starting XI']
        
        # Получаем данные о заменах
        substitution_events = match_data[match_data['type'] == 'Substitution']
        
        # Для каждой команды в матче
        for team_id in match_data['team_id'].unique():
            team_match_data = match_data[match_data['team_id'] == team_id]
            team_starting_xi = starting_xi_events[starting_xi_events['team_id'] == team_id]
            team_substitutions = substitution_events[substitution_events['team_id'] == team_id]
            
            # Получаем игроков из стартового состава
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
                                
                                # Инициализируем время игры (90 минут по умолчанию)
                                if player_id not in player_minutes:
                                    player_minutes[player_id] = {'name': player_name, 'total_minutes': 0}
                                    player_positions[player_id] = []
                                
                                # Сохраняем позицию игрока
                                if player_position not in player_positions[player_id]:
                                    player_positions[player_id].append(player_position)
                                
                                # Проверяем, был ли игрок заменен
                                substituted_off = team_substitutions[
                                    team_substitutions['player_id'] == player_id
                                ]
                                
                                if not substituted_off.empty:
                                    # Игрок был заменен - берем время замены
                                    sub_minute = substituted_off.iloc[0]['minute']
                                    player_minutes[player_id]['total_minutes'] += sub_minute
                                else:
                                    # Игрок играл весь матч
                                    player_minutes[player_id]['total_minutes'] += 90
                    except:
                        pass
            
            # Обрабатываем игроков, которые вышли на замену
            for _, sub_event in team_substitutions.iterrows():
                if pd.notna(sub_event.get('substitution_replacement_id')):
                    replacement_id = sub_event['substitution_replacement_id']
                    replacement_name = sub_event.get('substitution_replacement', 'Unknown')
                    sub_minute = sub_event['minute']
                    
                    if replacement_id not in player_minutes:
                        player_minutes[replacement_id] = {'name': replacement_name, 'total_minutes': 0}
                        player_positions[replacement_id] = []
                    
                    # Игрок, вышедший на замену, играет оставшееся время
                    player_minutes[replacement_id]['total_minutes'] += (90 - sub_minute)
    
    # Группируем данные по игрокам и считаем статистику
    player_stats = []
    
    for player_id, time_data in player_minutes.items():
        if time_data['total_minutes'] >= 500:  # Фильтруем игроков с менее чем 500 минутами
            player_events = df[df['player_id'] == player_id]
            
            if not player_events.empty:
                # Базовая статистика
                stats = {
                    'player_id': player_id,
                    'player_name': time_data['name'],
                    'position': ', '.join(player_positions.get(player_id, ['Unknown'])),
                    'total_minutes': time_data['total_minutes'],
                    'matches_played': len(player_events['match_id'].unique()),
                }
                
                # Считаем различные типы событий
                event_counts = player_events['type'].value_counts()
                
                # Основные статистики
                stats['passes'] = event_counts.get('Pass', 0)
                stats['shots'] = event_counts.get('Shot', 0)
                stats['dribbles'] = event_counts.get('Dribble', 0)
                stats['duels'] = event_counts.get('Duel', 0)
                stats['interceptions'] = event_counts.get('Interception', 0)
                stats['clearances'] = event_counts.get('Clearance', 0)
                stats['fouls_committed'] = event_counts.get('Foul Committed', 0)
                stats['fouls_won'] = event_counts.get('Foul Won', 0)
                stats['carries'] = event_counts.get('Carry', 0)
                
                # Успешные передачи
                successful_passes = player_events[
                    (player_events['type'] == 'Pass') & 
                    (player_events['pass_outcome'].isna())
                ].shape[0]
                stats['successful_passes'] = successful_passes
                
                # Успешные дриблинги
                successful_dribbles = player_events[
                    (player_events['type'] == 'Dribble') & 
                    (player_events['dribble_outcome'] == 'Complete')
                ].shape[0]
                stats['successful_dribbles'] = successful_dribbles
                
                # Голы
                goals = player_events[
                    (player_events['type'] == 'Shot') & 
                    (player_events['shot_outcome'] == 'Goal')
                ].shape[0]
                stats['goals'] = goals
                
                # НОВЫЕ МЕТРИКИ
                
                # xG (Expected Goals)
                xg_events = player_events[
                    (player_events['type'] == 'Shot') & 
                    (player_events['shot_statsbomb_xg'].notna())
                ]
                stats['xg'] = xg_events['shot_statsbomb_xg'].sum() if not xg_events.empty else 0
                
                # xA (Expected Assists) - из передач, которые привели к ударам
                xa_events = player_events[
                    (player_events['type'] == 'Pass') & 
                    (player_events['pass_shot_assist'].notna()) &
                    (player_events['pass_shot_assist'] == True)
                ]
                # Пытаемся найти xG для этих ударов
                stats['xa'] = 0
                for _, pass_event in xa_events.iterrows():
                    # Ищем соответствующий удар в том же матче
                    match_shots = df[
                        (df['match_id'] == pass_event['match_id']) &
                        (df['type'] == 'Shot') &
                        (df['minute'] >= pass_event['minute']) &
                        (df['minute'] <= pass_event['minute'] + 1)  # В течение минуты
                    ]
                    if not match_shots.empty:
                        # Берем первый удар после передачи
                        shot_xg = match_shots.iloc[0].get('shot_statsbomb_xg', 0)
                        if pd.notna(shot_xg):
                            stats['xa'] += shot_xg
                
                # Выносы в финальную треть
                carries_final_third = player_events[
                    (player_events['type'] == 'Carry') &
                    (player_events['carry_end_location_x'].notna()) &
                    (player_events['carry_end_location_x'] >= 80)  # Финальная треть (80-120 по оси X)
                ].shape[0]
                stats['carries_final_third'] = carries_final_third
                
                # Зональные касания
                all_touches = player_events[player_events['location_x'].notna()]
                
                # Касания в финальной трети
                final_third_touches = all_touches[all_touches['location_x'] >= 80].shape[0]
                # Касания в средней трети
                middle_third_touches = all_touches[
                    (all_touches['location_x'] >= 40) & 
                    (all_touches['location_x'] < 80)
                ].shape[0]
                # Касания в защитной трети
                defensive_third_touches = all_touches[all_touches['location_x'] < 40].shape[0]
                
                total_touches = len(all_touches)
                
                stats['final_third_touches'] = final_third_touches
                stats['middle_third_touches'] = middle_third_touches
                stats['defensive_third_touches'] = defensive_third_touches
                stats['total_touches'] = total_touches
                
                # Процент касаний в финальной трети
                stats['final_third_touches_pct'] = (final_third_touches / total_touches * 100) if total_touches > 0 else 0
                
                # Длина передач
                pass_events = player_events[
                    (player_events['type'] == 'Pass') &
                    (player_events['location_x'].notna()) &
                    (player_events['location_y'].notna()) &
                    (player_events['pass_end_location_x'].notna()) &
                    (player_events['pass_end_location_y'].notna())
                ]
                
                if not pass_events.empty:
                    # Вычисляем длину каждой передачи
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
                
                # Прогрессивные передачи (передачи, которые продвигают мяч к воротам соперника)
                progressive_passes = pass_events[
                    (pass_events['location_x'] < pass_events['pass_end_location_x']) &  # Движение вперед
                    ((pass_events['pass_end_location_x'] - pass_events['location_x']) >= 10)  # Минимум 10 метров вперед
                ].shape[0]
                stats['progressive_passes'] = progressive_passes
                
                # Длинные передачи (более 25 метров)
                long_passes = 0
                for _, pass_event in pass_events.iterrows():
                    start_x, start_y = pass_event['location_x'], pass_event['location_y']
                    end_x, end_y = pass_event['pass_end_location_x'], pass_event['pass_end_location_y']
                    length = ((end_x - start_x)**2 + (end_y - start_y)**2)**0.5
                    if length >= 25:
                        long_passes += 1
                
                stats['long_passes'] = long_passes
                
                # Конвертируем в per90
                minutes_factor = time_data['total_minutes'] / 90.0
                
                for key in stats.keys():
                    if key not in ['player_id', 'player_name', 'position', 'total_minutes', 'matches_played', 
                                   'final_third_touches_pct', 'avg_pass_length']:
                        stats[f'{key}_per90'] = stats[key] / minutes_factor if minutes_factor > 0 else 0
                
                # Процентные показатели
                if stats['passes'] > 0:
                    stats['pass_accuracy'] = (stats['successful_passes'] / stats['passes']) * 100
                else:
                    stats['pass_accuracy'] = 0
                
                if stats['dribbles'] > 0:
                    stats['dribble_success_rate'] = (stats['successful_dribbles'] / stats['dribbles']) * 100
                else:
                    stats['dribble_success_rate'] = 0
                
                # Прогрессивные передачи как процент от всех передач
                if stats['passes'] > 0:
                    stats['progressive_pass_pct'] = (stats['progressive_passes'] / stats['passes']) * 100
                else:
                    stats['progressive_pass_pct'] = 0
                
                player_stats.append(stats)
    
    # Создаем DataFrame
    result_df = pd.DataFrame(player_stats)
    
    # Сортируем по общему времени игры
    result_df = result_df.sort_values('total_minutes', ascending=False)
    
    return result_df