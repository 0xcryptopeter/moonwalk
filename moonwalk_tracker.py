import pandas as pd
import sys
import requests
from tabulate import tabulate
from datetime import datetime
import json
import time
from bs4 import BeautifulSoup

def get_game_data(game_code, skip=0, take=20):
    """Through API to get game data"""
    url = f"https://api.moonwalk.fit/api/user-games/web/{game_code}?skip={skip}&take={take}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Origin': 'https://app.moonwalk.fit',
        'Referer': f'https://app.moonwalk.fit/game/{game_code}',
        'Connection': 'keep-alive'
    }
    
    try:
        print(f"Fetching data: skip={skip}, take={take}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        current_batch = len(data.get('val', []))
        print(f"Fetched: {current_batch} players")
        
        return data
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return None

def get_all_game_data(game_code):
    """Get all player data"""
    all_players = []
    skip = 0
    take = 20  # Fixed at 20, matching the data acquisition logic of the website
    retry_count = 0
    max_retries = 3
    
    while True:
        data = get_game_data(game_code, skip, take)
        if not data:
            if retry_count < max_retries:
                print(f"Failed to fetch data, retrying attempt {retry_count + 1}")
                retry_count += 1
                time.sleep(1)
                continue
            else:
                print(f"Failed to fetch data, reached maximum retry attempts")
                break
            
        retry_count = 0  # Reset retry count
        
        players = data.get('val', [])
        current_batch = len(players)
        
        if current_batch == 0:
            print(f"Warning: No data fetched for current batch (skip={skip})")
            break
        
        all_players.extend(players)
        print(f"Fetched data for {len(all_players)} players")
        
        # If the number of fetched players is less than 20, it means the end of the data has been reached
        if current_batch < take:
            print("Reached the end of the data")
            break
        
        skip += take  # Update skip with a fixed take value (20)
        time.sleep(0.2)  # Add a short delay
    
    print(f"\nFinal fetched {len(all_players)} players' data")
    return all_players

def process_player_data(players):
    """Process player data"""
    all_players_data = {}
    print(f"\nStarting to process {len(players)} players' data")
    
    for i, player in enumerate(players, 1):
        try:
            user_info = player.get('user', {})
            username = user_info.get('name', '')
            if not username:
                print(f"Skipping player {i}: No username")
                continue
                
            steps_data = player.get('steps', [])
            formatted_steps = []
            for step in steps_data:
                steps = step.get('steps', 0)
                formatted_steps.append(format_steps(steps))
                
            all_players_data[username] = {
                'steps': formatted_steps,
                'total_completed': sum(1 for s in steps_data if s.get('steps', 0) > 0),
                'highest_steps': max((s.get('steps', 0) for s in steps_data), default=0)
            }
            
            if i % 50 == 0:  # Display progress every 50 players
                print(f"Processed {i} players")
                
        except Exception as e:
            print(f"Error processing player {i} data: {e}")
    
    print(f"Successfully processed {len(all_players_data)} players' data")
    return all_players_data

def format_steps(steps):
    """Format step number display"""
    if isinstance(steps, str) and 'k+' in steps:
        return steps
    if pd.isna(steps) or steps == 0:
        return '-'
    if steps >= 30000:
        return '30k+'
    return f"{int(steps):,}"

def get_usernames_from_csv(csv_file='moonwalk_users.csv'):
    """Get a list of usernames and their IDs from CSV file"""
    try:
        df = pd.read_csv(csv_file)
        # Create a dictionary with username as key and ID as value
        user_info = {}
        for _, row in df.iterrows():
            if pd.notna(row['Name']) and isinstance(row['Name'], str):
                username = row['Name'].replace('@', '')
                user_info[username] = {
                    'id': row['ID'],
                    'status': row['账号状态']
                }
        print(f"\nUsernames read from CSV: {list(user_info.keys())}")
        return user_info
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return {}

def get_game_overview(game_code):
    """Get game overview information from API"""
    url = f"https://api.moonwalk.fit/api/games/overview/{game_code}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
        'Origin': 'https://app.moonwalk.fit',
        'Referer': f'https://app.moonwalk.fit/game/{game_code}'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get('sts') == 200 and data.get('isVld'):
            game_data = data.get('val', {})
            
            # Convert Unix timestamp to date string
            start_date = datetime.fromtimestamp(game_data.get('start', 0)).strftime('%Y/%m/%d')
            end_date = datetime.fromtimestamp(game_data.get('end', 0)).strftime('%Y/%m/%d')
            
            game_info = {
                'game_code': game_data.get('code', 'Unknown'),
                'game_name': game_data.get('name', 'Unknown'),
                'deposit_amount': game_data.get('deposit', 0),
                'token_symbol': game_data.get('currency', 'Unknown').upper(),
                'start_date': start_date,
                'end_date': end_date,
                'step_target': game_data.get('steps', 10000),
                'total_players': game_data.get('size', 0),
                'game_link': f"https://app.moonwalk.fit/game/{game_code}"
            }
            
            return game_info
            
    except Exception as e:
        print(f"Error getting game overview: {e}")
        return None

def check_task_completion(steps_data, step_target, current_date):
    """Check task completion status"""
    completed_days = 0
    required_days = 0
    
    for step in steps_data:
        step_date = step['day'].split('T')[0] if 'T' in step['day'] else step['day']
        if step_date <= current_date:
            required_days += 1
            if step.get('steps', 0) >= step_target:
                completed_days += 1
    
    if required_days == 0:
        return '-'
    elif completed_days == required_days:
        return 'Complete'
    else:
        return f'Failed({completed_days}/{required_days})'  # Use English text instead of symbols

def save_to_csv(data, headers, game_info, game_code):
    """Save data to CSV with game information in English"""
    # Create DataFrame for player data
    df = pd.DataFrame(data, columns=headers)
    
    # Create game information DataFrame
    game_info_data = [
        ['Game Information', ''],
        ['Game Code', game_info['game_code']],
        ['Game Name', game_info['game_name']],
        ['Game Link', game_info['game_link']],
        ['Deposit Amount', f"{game_info['deposit_amount']} {game_info['token_symbol']}"],
        ['Start Date', game_info['start_date']],
        ['End Date', game_info['end_date']],
        ['Step Target', f"{game_info['step_target']:,}"],
        ['Total Players', game_info['total_players']],
        ['', ''],  # Empty row for separation
        ['Player Data', '']  # Data title
    ]
    
    # Create game info DataFrame
    info_df = pd.DataFrame(game_info_data, columns=['Info', 'Value'])
    
    # Save to file with UTF-8-SIG encoding (with BOM)
    filename = f'our_players_{game_code}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    # Write game info first
    info_df.to_csv(filename, index=False, encoding='utf-8-sig')
    
    # Append player data
    df.to_csv(filename, mode='a', index=False, encoding='utf-8-sig')
    
    print(f"\nData saved to: {filename}")

def display_results(player_data, game_code, user_info, game_dates, step_target, game_info):
    """Display our players' data with completion status"""
    if not player_data:
        print("\nNo data found")
        return
    
    # Get current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Process only our users' data
    our_data = {}
    for username, info in user_info.items():
        if username in player_data:
            our_data[username] = {
                'id': info['id'],
                'steps': player_data[username]['steps']
            }
        else:
            print(f"Warning: User {username} not found")
    
    if not our_data:
        print("\nNo data found for our users")
        return
    
    # Sort by ID
    sorted_players = sorted(our_data.items(), key=lambda x: x[1]['id'])
    
    # Use actual dates as column headers
    headers = ['ID', 'Username'] + [date.split('T')[0] if 'T' in date else date for date in game_dates] + ['Task Status']
    data = []
    
    print(f"\nStep Target: {step_target:,} steps")
    for username, user_data in sorted_players:
        # 确保steps数据长度与日期数量匹配
        steps = user_data['steps']
        while len(steps) < len(game_dates):
            steps.append('-')
            
        row = [user_data['id'], username]
        row.extend(steps)
        completion = check_task_completion(
            [{'day': date, 'steps': int(steps[i].replace(',', '').replace('k+', '30000').replace('-', '0')) 
              if i < len(steps) and steps[i] != '-' else 0} 
             for i, date in enumerate(game_dates)],
            step_target,
            current_date
        )
        row.append(completion)
        data.append(row)
        
        # Debug print
        print(f"Debug - Row length: {len(row)}, Headers length: {len(headers)}")
        print(f"Debug - Row data: {row}")
    
    print(f"\nNumber of our players: {len(data)}")
    print("\nPlayer Step Data:")
    print(tabulate(data, headers=headers, tablefmt='grid'))
    
    # Verify data structure before saving
    print(f"\nDebug - Headers: {headers}")
    print(f"Debug - Number of columns in headers: {len(headers)}")
    for row in data:
        if len(row) != len(headers):
            print(f"Warning: Row length mismatch - Row: {len(row)}, Headers: {len(headers)}")
            print(f"Row data: {row}")
    
    # Save to CSV with game information
    save_to_csv(data, headers, game_info, game_code)

def get_game_info_from_web(game_code):
    """Get game information from web page"""
    url = f"https://app.moonwalk.fit/game/{game_code}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Find game data in script tag
        soup = BeautifulSoup(response.text, 'html.parser')
        for script in soup.find_all('script'):
            if script.string and 'window.__NUXT__=' in script.string:
                json_str = script.string.split('window.__NUXT__=')[1].strip()
                data = json.loads(json_str)
                game_data = data['state']['game']['game']
                
                game_info = {
                    'game_code': game_data.get('id', 'Unknown'),
                    'game_name': game_data.get('name', 'Unknown'),
                    'deposit_amount': game_data.get('deposit', 0),
                    'token_symbol': game_data.get('token', 'Unknown'),
                    'start_date': game_data.get('startDate', '').split('T')[0],
                    'end_date': game_data.get('endDate', '').split('T')[0],
                    'step_target': game_data.get('stepTarget', 10000),
                    'total_players': game_data.get('totalPlayers', 0),
                    'game_link': f"https://app.moonwalk.fit/game/{game_code}"
                }
                
                return game_info
                
    except Exception as e:
        print(f"Error getting game info from web: {e}")
        return None

def main():
    if len(sys.argv) != 2:
        print("Usage: python moonwalk_tracker.py <game_code>")
        sys.exit(1)
    
    game_code = sys.argv[1]
    user_info = get_usernames_from_csv()
    
    if not user_info:
        print("Failed to read user information from CSV file")
        return
    
    print("Starting to fetch game data...")
    
    # Get game overview information
    game_info = get_game_overview(game_code)
    if not game_info:
        print("Failed to get game overview")
        return
    
    print(f"\nGame Information:")
    print(f"Game Name: {game_info['game_name']}")
    print(f"Deposit Required: {game_info['deposit_amount']} {game_info['token_symbol']}")
    print(f"Game Period: {game_info['start_date']} to {game_info['end_date']}")
    print(f"Step Target: {game_info['step_target']:,}")
    print(f"Total Players: {game_info['total_players']}")
    
    # Get player data
    initial_data = get_game_data(game_code, 0, 20)
    if not initial_data:
        print("Failed to get player data")
        return
        
    # Get game dates from player data
    game_dates = []
    if initial_data.get('val'):
        first_player = initial_data['val'][0]
        game_dates = [step['day'] for step in first_player.get('steps', [])]
    
    if not game_dates:
        print("Failed to get game dates")
        return
    
    all_players = get_all_game_data(game_code)
    
    if all_players and len(all_players) > 0:
        print(f"Successfully fetched game data")
        player_data = process_player_data(all_players)
        if player_data:
            display_results(player_data, game_code, user_info, game_dates, game_info['step_target'], game_info)
        else:
            print("Error processing data")
    else:
        print("No player data found")

if __name__ == "__main__":
    main() 