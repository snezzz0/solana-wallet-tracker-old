import json
import random
import os
import re
import sys
import time
import subprocess
import datetime
from collections import defaultdict

def process_wallets(input_file, output_txt, output_json):
    # Read wallet addresses from input file
    with open(input_file, 'r') as f:
        wallet_lines = [line.strip() for line in f if line.strip()]
    
    # Parse input lines - looking for both plain wallets and wallet:name pairs
    wallets = []
    existing_wallet_names = {}
    existing_names = set()  # Track all existing names
    
    for line in wallet_lines:
        # Check if the line contains a wallet:name pair
        match = re.search(r'([^:,\s]+):([^:,\s]+)', line)
        if match:
            wallet = match.group(1)
            name = match.group(2)
            wallets.append(wallet)
            existing_wallet_names[wallet] = name
            existing_names.add(name)  # Add to set of existing names
        else:
            # If no match, treat the whole line as a wallet address
            potential_wallet = line.split(',')[0].strip()  # In case there are commas
            if potential_wallet:
                wallets.append(potential_wallet)
    
    # Remove duplicates while preserving order
    unique_wallets = []
    seen = set()
    for wallet in wallets:
        if wallet not in seen:
            seen.add(wallet)
            unique_wallets.append(wallet)
    
    # List of comic book character names
    comic_names = [
        # Marvel Characters
        "peter_parker", "bruce_banner", "tony_stark", "steve_rogers", "natasha_romanoff",
        "thor_odinson", "wanda_maximoff", "scott_summers", "jean_grey", "ororo_munroe",
        "logan_howlett", "remy_lebeau", "emma_frost", "billy_batson", "victor_stone",
        "ben_grimm", "johnny_storm", "reed_richards", "sue_storm", "matt_murdock",
        "elektra_natchios", "frank_castle", "clint_barton", "jessica_jones", "luke_cage",
        "danny_rand", "jennifer_walters", "thanos", "loki_laufeyson", "miles_morales",
        "gwen_stacy", "jessica_drew", "carol_danvers", "sam_wilson", "bucky_barnes",
        "vision", "pietro_maximoff", "hank_pym", "janet_van_dyne", "tchalla",
        "shuri", "okoye", "namor", "doctor_strange", "wong", "clea", "ghost_rider",
        "blade", "moon_knight", "black_widow", "hawkeye", "war_machine", "falcon",
        "winter_soldier", "scarlet_witch", "quicksilver", "beast", "cyclops",
        "storm", "wolverine", "gambit", "rogue", "jubilee", "psylocke", "colossus",
        "nightcrawler", "iceman", "angel", "archangel", "cable", "deadpool",
        "domino", "negasonic", "firestar", "spider_gwen", "spider_man_2099",
        "spider_ham", "spider_woman", "black_cat", "silver_sable", "venom",
        "carnage", "morbius", "lizard", "doc_ock", "green_goblin", "sandman",
        "electro", "vulture", "kraven", "mysterio", "rhino", "scorpion",
        
        # DC Characters
        "bruce_wayne", "clark_kent", "diana_prince", "barry_allen", "hal_jordan",
        "dick_grayson", "barbara_gordon", "selina_kyle", "kate_kane", "dinah_lance",
        "oliver_queen", "john_stewart", "kyle_rayner", "arthur_curry", "mera_xebella",
        "j_jonah_jameson", "joker", "harley_quinn", "poison_ivy", "two_face",
        "penguin", "riddler", "scarecrow", "bane", "ra's_al_ghul", "deadshot",
        "deathstroke", "black_manta", "cheetah", "lex_luthor", "brainiac",
        "doomsday", "darkseid", "steppenwolf", "desaad", "granny_goodness",
        "killer_croc", "solomon_grundy", "black_adam", "shazam", "wonder_girl",
        "robin", "nightwing", "red_hood", "red_robin", "batgirl", "batwoman",
        "supergirl", "power_girl", "superboy", "cyborg", "raven", "starfire",
        "beast_boy", "kid_flash", "aqualad", "miss_martian", "artemis",
        "zatanna", "constantine", "swamp_thing", "etrigan", "deadman",
        "spectre", "dr_fate", "hawkman", "hawkgirl", "atom", "firestorm",
        "green_arrow", "black_canary", "martian_manhunter", "plastic_man",
        "booster_gold", "blue_beetle", "shazam", "captain_cold", "mirror_master",
        "weather_wizard", "trickster", "zoom", "reverse_flash", "grodd",
        
        # Image Comics Characters
        "spawn", "invincible", "savage_dragon", "witchblade", "darkness",
        "cyberforce", "shadowhawk", "bratpack", "youngblood", "prophet",
        "glory", "supreme", "bloodstrike", "brigade", "wetworks",
        "stormwatch", "wildcats", "gen13", "backlash", "voodoo",
        "grifter", "zealot", "maul", "apollo", "midnighter",
        "spider_king", "savage_wolf", "firebreather", "elephantmen",
        "walking_dead", "spawn", "cable", "deadpool", "wolverine",
        
        # Vertigo Characters
        "sandman", "death", "destiny", "desire", "despair",
        "delirium", "destruction", "dream", "lucifer", "mazikeen",
        "constantine", "swamp_thing", "preacher", "jesse_custer",
        "tulip", "cassidy", "hellblazer", "sandman", "death",
        
        # Additional Marvel Characters
        "nova", "star_lord", "gamora", "drax", "rocket_raccoon",
        "groot", "mantis", "nebula", "yondu", "adam_warlock",
        "phoenix", "apocalypse", "mister_sinister", "sabretooth",
        "mystique", "azazel", "havok", "polaris", "banshee",
        "forge", "bishop", "cable", "hope_summers", "x_23",
        "daken", "omega_red", "lady_deathstrike", "sauron",
        "brood", "sentinel", "mastermold", "bastion", "cameron_hodge",
        
        # Additional DC Characters
        "batman_beyond", "terry_mcginnis", "amanda_waller", "rick_flag",
        "captain_boomerang", "killer_croc", "enchantress", "el_diablo",
        "slipknot", "katana", "deadshot", "harley_quinn", "joker",
        "poison_ivy", "two_face", "penguin", "riddler", "scarecrow",
        "bane", "ra's_al_ghul", "deadshot", "deathstroke", "black_manta",
        "cheetah", "lex_luthor", "brainiac", "doomsday", "darkseid",
        "steppenwolf", "desaad", "granny_goodness", "killer_croc",
        "solomon_grundy", "black_adam", "shazam", "wonder_girl",
        "robin", "nightwing", "red_hood", "red_robin", "batgirl",
        "batwoman", "supergirl", "power_girl", "superboy", "cyborg",
        "raven", "starfire", "beast_boy", "kid_flash", "aqualad",
        "miss_martian", "artemis", "zatanna", "constantine", "swamp_thing",
        "etrigan", "deadman", "spectre", "dr_fate", "hawkman",
        "hawkgirl", "atom", "firestorm", "green_arrow", "black_canary",
        "martian_manhunter", "plastic_man", "booster_gold", "blue_beetle",
        "shazam", "captain_cold", "mirror_master", "weather_wizard",
        "trickster", "zoom", "reverse_flash", "grodd"
    ]
    
    # Filter out names that already exist
    available_names = [name for name in comic_names if name not in existing_names]
    
    # If we have more wallets than available names, we'll add numbers to names
    if len(unique_wallets) > len(available_names):
        extended_names = []
        iterations = (len(unique_wallets) // len(available_names)) + 1
        for i in range(iterations):
            suffix = f"_{i+1}" if i > 0 else ""
            extended_names.extend([f"{name}{suffix}" for name in available_names])
        available_names = extended_names
    
    # Shuffle the names to make assignments random
    random.shuffle(available_names)
    
    # Assign names to wallets (keeping existing names)
    wallet_to_name = {}
    name_index = 0
    
    for wallet in unique_wallets:
        if wallet in existing_wallet_names:
            # Use existing name if available
            wallet_to_name[wallet] = existing_wallet_names[wallet]
        else:
            # Assign a new name from available names
            wallet_to_name[wallet] = available_names[name_index]
            name_index += 1
            # If we run out of names (unlikely but possible), loop back
            if name_index >= len(available_names):
                name_index = 0
    
    # Create sections of 25 wallets
    sections = defaultdict(list)
    for i, wallet in enumerate(unique_wallets):
        section_num = i // 25 + 1
        sections[f"Section {section_num}"].append(f"{wallet}:{wallet_to_name[wallet]}")
    
    # Ensure config directory exists
    os.makedirs(os.path.dirname(output_txt), exist_ok=True)
    
    # Write to TXT file
    with open(output_txt, 'w') as f:
        for section, wallet_entries in sections.items():
            f.write(f"{section}\n")
            for entry in wallet_entries:
                f.write(f"{entry}\n")
            f.write("\n")  # Add an extra newline between sections
    
    # Write to JSON file
    with open(output_json, 'w') as f:
        json.dump(wallet_to_name, f, indent=4)
    
    return len(unique_wallets), len(wallets) - len(unique_wallets), len(sections), len(existing_wallet_names)

def start_bots():
    """Start the bots using start_bots.py"""
    print("Starting bots...")
    
    # Get the directory where this script is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Get the parent directory
    parent_dir = os.path.dirname(current_dir)
    # Path to start_bots.py
    start_bots_path = os.path.join(parent_dir, "cloudversion", "start_bots.py")
    
    if not os.path.exists(start_bots_path):
        print(f"Error: start_bots.py not found at {start_bots_path}")
        return False
    
    try:
        # Start the bots using subprocess
        subprocess.Popen([sys.executable, start_bots_path])
        print("Bots started successfully!")
        return True
    except Exception as e:
        print(f"Error starting bots: {e}")
        return False

def main():
    """Main function that processes wallets and starts bots"""
    # Define file paths using absolute paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    input_file = os.path.join(parent_dir, "scripts", "wallet_addresses.txt")  # Changed to look in scripts directory
    config_dir = os.path.join(parent_dir, "config")
    output_txt = os.path.join(config_dir, "wallet_sections.txt")
    output_json = os.path.join(config_dir, "wallet_names.json")
    
    # Process wallets
    unique_count, duplicate_count, section_count, preserved_count = process_wallets(input_file, output_txt, output_json)
    
    print(f"\nProcessing complete!")
    print(f"- Found {unique_count} unique wallet addresses")
    print(f"- Removed {duplicate_count} duplicate addresses")
    print(f"- Preserved {preserved_count} existing wallet-name associations")
    print(f"- Created {section_count} sections of wallets")
    print(f"- Output saved to {output_txt} and {output_json}")
    
    # Start the bots after processing
    start_bots()

if __name__ == "__main__":
    print("Wallet Name Processor started")
    main()