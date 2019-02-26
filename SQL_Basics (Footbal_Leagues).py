#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
get_ipython().magic('matplotlib inline')


# In[2]:


pd.set_option('max_columns', 180)
pd.set_option('max_rows', 200000)
pd.set_option('max_colwidth', 5000)


# In[3]:


game_log = pd.read_csv('game_log.csv')
park_id = pd.read_csv('park_codes.csv')
player_id = pd.read_csv('person_codes.csv')
team_id = pd.read_csv('team_codes.csv')


# # Getting to Know the Data

# In[4]:


get_ipython().system('cat game_log_fields.txt')


# Files
# - game_log (161, 171907) - main file
# - park_codes (9, 252) - contain park codes, park names and more info about the parks.
# - person_codes (7, 20494) - players codes, names and more info.
# - team_codes (8, 150) - contain team codes, league, city, names and more info.
# 

# In[5]:


print(game_log.shape)
game_log.head()


# In[6]:


game_log['h_league'].isna().sum()


# game_log file
# 
# - In columns 106 - 159 are the defensive positions for visiting team's players and home team's players
# Each player gets 3 columns -
# h_player_9_id  --  Unique Player ID (TEXT)
# h_player_9_name  --  Player Name (TEXT)
# h_player_9_def_pos  --  Player Defence Position (FLOAT/NUMERIC)
# * The first letter h\v is for home\visitor
# 
# Player Defence Position - 
# each player has a defensive position, a number between 1-9)
# 
# 1. Pitcher (P)
# 2. Catcher (C)
# 3. 1st Base (1B)
# 4. 2nd Base (2B)
# 5. 3rd Base (3B)
# 6. Shortstop (SS)
# 7. Left Field (LF)
# 8. Center Field (CF)
# 9. Right Field (RF)
# 10. Unknown Position
# 
# There are 1086 rows with NaN h_league in game_log table.

# In[7]:


print(team_id.shape)
team_id.head()


# In[8]:


# Leagues
team_id.iloc[:,1].value_counts() 


# In[9]:


def leagues_years(league):
    league_games = game_log[game_log['h_league'] == league]
    first_game = league_games['date'].min()
    last_game = league_games['date'].max()
    print("{} - {} to {}".format(league,first_game,last_game))
    
for league in game_log['h_league'].unique():
    leagues_years(league)


# Leagues
# 
# According to Wikipedia - There are 2 leagues 
# AL (45) - American
# NL (25) - National
# 
# In our table there are 4 more leagues which are Old Leagues the last game 
# from all 4 was in 1915
# AA    (24) - American Association
# UA    (13) - Union Association
# FL     (9) - Federal League
# PL     (8) - Players League

# In[10]:


#french_ids
team_id.iloc[:,6].value_counts()


# In[11]:


# checking a french_id with more than 1 appearence in team table
team_id[team_id['franch_id'] == "MLA"]


# It seems that over the years, teams move between leagues and cities and their team_id changes.
# However the french_id remain the same. (The years in the table above do not overlap)

# In[12]:


db = "mlb.db"

def run_query(q):
    with sqlite3.connect(db) as conn:
            return pd.read_sql(q,conn)
            
def run_command(c):
    with sqlite3.connect(db) as conn:
        conn.execute('PRAGMA foreign_keys = ON;')        
        conn.isolation_level = None
        conn.execute(c)
            
def show_tables():
    q = '''
        SELECT 
              name,
              type
        FROM sqlite_master
        WHERE type IN ("table", "view");
        '''
    return run_query(q)


# In[13]:


tables = {
    "game_log" : game_log,
    "park_id" : park_id,
    "player_id" : player_id,
    "team_id" : team_id    
}

with sqlite3.connect(db) as conn:    
    for name, data in tables.items():
        conn.execute("DROP TABLE IF EXISTS {};".format(name))
        data.to_sql(name,conn,index=False)


# In[14]:


show_tables()


# In[15]:


# Adding game_id that can later become a primary key for game_log table

a = '''
ALTER TABLE game_log
ADD COLUMN game_id TEXT;
'''

try:
    run_command(a)
except:
    pass


# In[16]:


# creating the game_id from -home group - date of game - game number

a1 = '''
UPDATE game_log
SET game_id = date || h_name || number_of_game
WHERE game_id IS NULL; 
'''

run_command(a1)


# In[17]:


a2 = '''
SELECT
    game_id,
    date,
    h_name,
    number_of_game
FROM game_log
LIMIT 5;
'''

run_query(a2)


# # Looking for Normalization Opportunities

# In[18]:


player_id.head()


# In[19]:


park_id.head()


# ![Title](https://raw.githubusercontent.com/dataquestio/solutions/a57cf4f5da71e5972bf6988a7d5d6a19bb841d89/images/schema-screenshot.png)

# In[20]:


b = '''
CREATE TABLE IF NOT EXISTS person (
    person_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT
    );
'''

b1 = '''
INSERT OR IGNORE INTO person
SELECT 
    id,
    first,
    last
FROM player_id;
'''

b2 = '''
SELECT * FROM person 
LIMIT 5;
'''

run_command(b)
run_command(b1)
run_query(b2)


# In[21]:


c = '''
CREATE TABLE IF NOT EXISTS park (
    park_id TEXT PRIMARY KEY,
    name TEXT,
    nickname TEXT,
    city TEXT,
    state TEXT,
    notes TEXT
    );
'''

c1 = '''
INSERT OR IGNORE INTO park
SELECT 
    park_id,
    name,
    aka,
    city,
    state,
    notes
FROM park_id;
'''

c2 = '''
SELECT * FROM park 
LIMIT 5;
'''

run_command(c)
run_command(c1)
run_query(c2)


# 

# In[22]:


d = '''
CREATE TABLE IF NOT EXISTS league (
    league_id TEXT PRIMARY KEY,
    league_name TEXT
    );
'''

d1 = '''
INSERT OR IGNORE INTO league 
VALUES ("NL", "National League"),
        ("AL", "American League"),
        ("AA", "American Association"),
        ("FL", "Federal League"),
        ("PL", "Players League"),
        ("UA", "Union Association");
'''

d2 = '''
SELECT * FROM league 
LIMIT 5;
'''

run_command(d)
run_command(d1)
run_query(d2)


# In[23]:


e = '''
CREATE TABLE IF NOT EXISTS appearance_type (
    appearance_type_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT
    );
'''
run_command(e)

appearance_type = pd.read_csv('appearance_type.csv')

with sqlite3.connect('mlb.db') as conn:
    appearance_type.to_sql('appearance_type',
                           conn,
                           index=False,
                           if_exists='append')

e1 = """
SELECT * FROM appearance_type;
"""

run_query(e1)


# # Adding The Team and Game Tables

# In[ ]:


f = '''
CREATE TABLE IF NOT EXISTS team (
    team_id TEXT PRIMARY KEY,
    league_id TEXT,
    city TEXT,
    nickname TEXT,
    franch_id TEXT,
    FOREIGN KEY (league_id) REFERENCES league(league_id)
    );
'''

f1 = '''
INSERT OR IGNORE INTO team
SELECT 
    team_id,
    league,
    city,
    nickname,
    franch_id
FROM team_id;
'''

f2 = '''
SELECT * FROM team 
LIMIT 5;
'''

run_command(f)
run_command(f1)
run_query(f2)


# In[ ]:


g = '''
CREATE TABLE IF NOT EXISTS game (
    game_id TEXT PRIMARY KEY,
    date TEXT,
    number_of_game INTEGER,
    park_id TEXT,
    length_outs INTEGER,
    day INTEGER,
    completion TEXT,
    forefeit TEXT,
    protest TEXT,
    attendance INTEGER,
    length_minutes INTEGER,
    additional_info TEXT,
    acquisition TEXT,
    FOREIGN KEY (park_id) REFERENCES park(park_id)
    );
'''

g1 = '''
INSERT OR IGNORE INTO game
SELECT
    game_id,
    date,
    number_of_game,
    park_id,
    length_outs,
    CASE
        WHEN day_night = "D" THEN 1
        WHEN day_night = "N" THEN 0
        ELSE NULL
        END
        AS day,
    completion,
    forefeit,
    protest,
    attendance,
    length_minutes,
    additional_info,
    acquisition_info
FROM game_log;
'''

g2 = '''
SELECT * FROM game
LIMIT 5;
'''

run_command(g)
run_command(g1)
run_query(g2)


# # Adding the Team Appearance Table

# In[ ]:


h = '''
CREATE TABLE IF NOT EXISTS team_appearance (
    team_id TEXT,
    game_id TEXT,
    home BOOLEAN,
    league_id TEXT,
    score INTEGER,
    line_score TEXT,
    at_bats INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    homeruns INTEGER,
    rbi INTEGER,
    sacrifice_hits INTEGER,
    sacrifice_flies INTEGER,
    hit_by_pitch INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    grounded_into_double INTEGER,
    first_catcher_interference INTEGER,
    left_on_base INTEGER,
    pitchers_used INTEGER,
    individual_earned_runs INTEGER,
    team_earned_runs INTEGER,
    wild_pitches INTEGER,
    balks INTEGER,
    putouts INTEGER,
    assists INTEGER,
    errors INTEGER,
    passed_balls INTEGER,
    double_plays INTEGER,
    triple_plays INTEGER,
    PRIMARY KEY (team_id, game_id),
    FOREIGN KEY (team_id) REFERENCES team(team_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id),
    FOREIGN KEY (team_id) REFERENCES team(team_id)
);
'''
run_command(h)


h1 = '''
INSERT OR IGNORE INTO team_appearance
    SELECT
        h_name,
        game_id,
        1 AS home,
        h_league,
        h_score,
        h_line_score,
        h_at_bats,
        h_hits,
        h_doubles,
        h_triples,
        h_homeruns,
        h_rbi,
        h_sacrifice_hits,
        h_sacrifice_flies,
        h_hit_by_pitch,
        h_walks,
        h_intentional_walks,
        h_strikeouts,
        h_stolen_bases,
        h_caught_stealing,
        h_grounded_into_double,
        h_first_catcher_interference,
        h_left_on_base,
        h_pitchers_used,
        h_individual_earned_runs,
        h_team_earned_runs,
        h_wild_pitches,
        h_balks,
        h_putouts,
        h_assists,
        h_errors,
        h_passed_balls,
        h_double_plays,
        h_triple_plays
    FROM game_log

UNION

    SELECT    
        v_name,
        game_id,
        0 AS home,
        v_league,
        v_score,
        v_line_score,
        v_at_bats,
        v_hits,
        v_doubles,
        v_triples,
        v_homeruns,
        v_rbi,
        v_sacrifice_hits,
        v_sacrifice_flies,
        v_hit_by_pitch,
        v_walks,
        v_intentional_walks,
        v_strikeouts,
        v_stolen_bases,
        v_caught_stealing,
        v_grounded_into_double,
        v_first_catcher_interference,
        v_left_on_base,
        v_pitchers_used,
        v_individual_earned_runs,
        v_team_earned_runs,
        v_wild_pitches,
        v_balks,
        v_putouts,
        v_assists,
        v_errors,
        v_passed_balls,
        v_double_plays,
        v_triple_plays
    from game_log;
'''
run_command(h1)


# In[25]:


h2 = '''
SELECT * FROM team_appearance
ORDER BY game_id, home
LIMIT 10;

'''
run_query(h2)


# In[28]:


i = '''
CREATE TABLE IF NOT EXISTS person_appearance (
        appearance_id INTEGER,
        person_id TEXT,
        team_id TEXT,
        game_id TEXT,
        appearance_type_id TEXT,
        PRIMARY KEY (appearance_id),
        FOREIGN KEY (person_id) REFERENCES person(person_id),
        FOREIGN KEY (team_id) REFERENCES team(team_id),
        FOREIGN KEY (game_id) REFERENCES game(game_id),
        FOREIGN KEY (appearance_type_id) REFERENCES appearance_type(appearance_type_id)           
);
'''

i1 = '''
INSERT OR IGNORE INTO person_appearance (
    game_id,
    team_id,
    person_id,
    appearance_type_id
) 
    SELECT
        game_id,
        NULL,
        hp_umpire_id,
        "UHP"
    FROM game_log
    WHERE hp_umpire_id IS NOT NULL    

UNION

    SELECT
        game_id,
        NULL,
        [1b_umpire_id],
        "U1B"
    FROM game_log
    WHERE "1b_umpire_id" IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        [2b_umpire_id],
        "U2B"
    FROM game_log
    WHERE [2b_umpire_id] IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        [3b_umpire_id],
        "U3B"
    FROM game_log
    WHERE [3b_umpire_id] IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        lf_umpire_id,
        "ULF"
    FROM game_log
    WHERE lf_umpire_id IS NOT NULL

UNION

    SELECT
        game_id,
        NULL,
        rf_umpire_id,
        "URF"
    FROM game_log
    WHERE rf_umpire_id IS NOT NULL

UNION

    SELECT
        game_id,
        v_name,
        v_manager_id,
        "MM"
    FROM game_log
    WHERE v_manager_id IS NOT NULL

UNION

    SELECT
        game_id,
        h_name,
        h_manager_id,
        "MM"
    FROM game_log
    WHERE h_manager_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        winning_pitcher_id,
        "AWP"
    FROM game_log
    WHERE winning_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score < v_score THEN h_name
            ELSE v_name
            END,
        losing_pitcher_id,
        "ALP"
    FROM game_log
    WHERE losing_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        saving_pitcher_id,
        "ASP"
    FROM game_log
    WHERE saving_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        CASE
            WHEN h_score > v_score THEN h_name
            ELSE v_name
            END,
        winning_rbi_batter_id,
        "AWB"
    FROM game_log
    WHERE winning_rbi_batter_id IS NOT NULL

UNION

    SELECT
        game_id,
        v_name,
        v_starting_pitcher_id,
        "PSP"
    FROM game_log
    WHERE v_starting_pitcher_id IS NOT NULL

UNION

    SELECT
        game_id,
        h_name,
        h_starting_pitcher_id,
        "PSP"
    FROM game_log
    WHERE h_starting_pitcher_id IS NOT NULL;    
'''

run_command(i)
run_command(i1)


# In[29]:


i2 = '''
SELECT * FROM person_appearance
LIMIT 10;
'''

run_query(i2)


# In[31]:


show_tables()


# In[33]:


j = '''DROP TABLE IF EXISTS game_log;'''
k = '''DROP TABLE IF EXISTS park_id;'''
l = '''DROP TABLE IF EXISTS team_id;'''
m = '''DROP TABLE IF EXISTS player_id;'''

run_command(j)
run_command(k)
run_command(l)
run_command(m)


# In[34]:


show_tables()


# In[ ]:




