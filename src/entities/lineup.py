from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Union
import pandas as pd

class Position(Enum):
    """
    Enumeration representing hockey player positions.
    
    Attributes:
        C: Center position
        L: Left Wing position
        R: Right Wing position
        D: Defense position
        G: Goalie position
    
    The category property groups positions into three main categories:
        - F (Forward): C, L, R positions
        - D (Defense): D position
        - G (Goalie): G position
    """
    C = 'C'
    L = 'L'
    R = 'R'
    D = 'D'
    G = 'G'
    
    @property
    def category(self) -> str:
        """
        Returns the category of the position (Forward, Defense, or Goalie).
        
        Returns:
            str: 'F' for forwards (C, L, R), 'D' for defense, 'G' for goalie
        """
        if self in {Position.C, Position.L, Position.R}:
            return 'F'
        elif self == Position.D:
            return 'D'
        elif self == Position.G:
            return 'G'
    
    def __str__(self) -> str:
        """
        Returns the string value of the position.
        
        Returns:
            str: The position's value (e.g., 'C', 'L', 'R', 'D', 'G')
        """
        return self.value

@dataclass
class Player:
    """
    A dataclass representing a hockey player.
    
    Attributes:
        name (str): The player's full name
        team (str): The team code/abbreviation (e.g., 'TOR', 'MTL')
        position (Position): The player's position as a Position enum
        player_id (Optional[int]): The player's unique identifier, if available
    """
    name: str
    team: str
    position: Position
    player_id: Optional[int] = None

    def __str__(self) -> str:
        """
        Returns a string representation of the player.
        
        Returns:
            str: A formatted string containing the player's name, position, and team
                 Example: "John Doe (C) - TOR"
        """
        return f"{self.name} ({self.position}) - {self.team}"

    def to_dict(self) -> Dict[str, Optional[str]]:
        """
        Converts the Player instance into a dictionary.
        
        Returns:
            Dict[str, Optional[str]]: A dictionary containing the player's attributes:
                - player_id: The player's ID (if available)
                - name: The player's name
                - team: The team code
                - position: The position value as a string
        """
        return {
            'player_id': self.player_id,
            'name': self.name,
            'team': self.team,
            'position': self.position.value
        }

@dataclass
class Lineup:
    """
    A dataclass representing a hockey team lineup.
    
    The lineup maintains separate lists for forwards, defense, and goalies,
    with constraints on the number of players in each category and the total roster size.
    
    Attributes:
        name (str): The name/identifier of the lineup
        forwards (List[Optional[Player]]): List of forward positions (default: 12 slots)
        defense (List[Optional[Player]]): List of defense positions (default: 6 slots)
        goalies (List[Optional[Player]]): List of goalie positions (default: 2 slots)
        back_to_back (Optional[bool]): Indicates if this lineup is being used in a back-to-back game situation
    
    Class Constants:
        ALLOWED_FORWARD_CATEGORIES (Set[str]): Valid categories for forward positions
        ALLOWED_DEFENSE_CATEGORY (str): Valid category for defense positions
        ALLOWED_GOALIE_CATEGORY (str): Valid category for goalie positions
    
    Constraints:
        - Maximum of 20 players total across all positions
        - Forward slots can vary between 11-13 (default: 12)
        - Defense slots can vary between 5-7 (default: 6)
        - Goalie slots fixed at 2
    """
    name: str
    forwards: List[Optional[Player]] = field(default_factory=lambda: [None] * 12)
    defense: List[Optional[Player]] = field(default_factory=lambda: [None] * 6)
    goalies: List[Optional[Player]] = field(default_factory=lambda: [None] * 2)
    back_to_back: Optional[bool] = None
    
    ALLOWED_FORWARD_CATEGORIES = {'F'}
    ALLOWED_DEFENSE_CATEGORY = 'D'
    ALLOWED_GOALIE_CATEGORY = 'G'
    
    def __post_init__(self):
        """
        Validates the lineup size after initialization.
        
        Raises:
            ValueError: If the total number of players exceeds 20
        """
        self.validate_lineup_size()
    
    def validate_lineup_size(self):
        """
        Validates that the total number of players does not exceed 20.
        
        Raises:
            ValueError: If the total number of players exceeds 20
        """
        total_players = sum(player is not None for player in self.forwards + self.defense + self.goalies)
        if total_players > 20:
            raise ValueError(f"Total number of players ({total_players}) exceeds the hard limit of 20.")
    
    def add_player(
        self,
        category: str,
        player: Player,
        slot: int,
        allowed_categories: Union[str, Set[str]],
        max_slots: int
    ):
        """
        Adds a player to the specified category and slot after validating their position category.
        Ensures that the total number of players does not exceed 20.
        
        Args:
            category (str): The category to add the player to ('forwards', 'defense', 'goalies')
            player (Player): The player to add
            slot (int): The slot number (0-based index)
            allowed_categories (Union[str, Set[str]]): Valid position categories for this slot
            max_slots (int): Maximum number of slots in this category
        
        Raises:
            TypeError: If allowed_categories is neither a string nor a set
            ValueError: If the player's position category is not allowed or if adding would exceed 20 players
            IndexError: If the slot number is out of range
        """
        if isinstance(allowed_categories, str):
            allowed_categories = {allowed_categories}
        elif isinstance(allowed_categories, set):
            allowed_categories = allowed_categories
        else:
            raise TypeError("allowed_categories must be a string or a set of strings.")
        
        if player.position.category not in allowed_categories:
            raise ValueError(
                f"Cannot add player '{player.name}' with position '{player.position.value}' "
                f"to {category}. Allowed categories: {', '.join(allowed_categories)}."
            )
        
        if not 0 <= slot < max_slots:
            raise IndexError(f"{category.capitalize()} slot must be between 0 and {max_slots - 1}.")
        
        current_category = getattr(self, category)
        if current_category[slot]:
            existing_player = current_category[slot].name
            print(f"Warning: Slot {slot + 1} in {category} is already occupied by '{existing_player}'. Overwriting.")
        
        # Check total players before adding
        total_players = sum(player is not None for player in self.forwards + self.defense + self.goalies)
        if current_category[slot] is None and total_players >= 20:
            raise ValueError("Cannot add more players. The lineup has reached the hard limit of 20 players.")
        
        current_category[slot] = player
        setattr(self, category, current_category)
        print(f"Added player '{player.name}' to {category.capitalize()} slot {slot + 1}.")
    
    def add_forward(self, player: Player, slot: int):
        """
        Adds a forward player to the specified slot.
        
        Args:
            player (Player): The player to add (must have a forward position)
            slot (int): The slot number (0-based index)
        
        Raises:
            ValueError: If the player's position is not a forward position
            IndexError: If the slot number is out of range
        """
        self.add_player(
            category='forwards',
            player=player,
            slot=slot,
            allowed_categories=self.ALLOWED_FORWARD_CATEGORIES,
            max_slots=len(self.forwards)
        )
    
    def add_defense(self, player: Player, slot: int):
        """
        Adds a defense player to the specified slot.
        
        Args:
            player (Player): The player to add (must have a defense position)
            slot (int): The slot number (0-based index)
        
        Raises:
            ValueError: If the player's position is not a defense position
            IndexError: If the slot number is out of range
        """
        self.add_player(
            category='defense',
            player=player,
            slot=slot,
            allowed_categories={self.ALLOWED_DEFENSE_CATEGORY},
            max_slots=len(self.defense)
        )
    
    def set_goalie(self, player: Player, slot: int):
        """
        Sets a goalie in the specified slot.
        
        Args:
            player (Player): The player to add (must have a goalie position)
            slot (int): The slot number (0-based index)
        
        Raises:
            ValueError: If the player's position is not a goalie position
            IndexError: If the slot number is out of range
        """
        self.add_player(
            category='goalies',
            player=player,
            slot=slot,
            allowed_categories={self.ALLOWED_GOALIE_CATEGORY},
            max_slots=len(self.goalies)
        )
    
    def adjust_slots(self, category: str, delta: int):
        """
        Adjusts the number of slots in the specified category by delta.
        Allows ±1 adjustment only.
        
        Args:
            category (str): The category to adjust ('forwards' or 'defense')
            delta (int): The change in number of slots (+1 or -1)
        
        Raises:
            ValueError: If:
                - category is not 'forwards' or 'defense'
                - delta is not +1 or -1
                - resulting slot count would be outside allowed range
                - total players would exceed 20
        """
        if category not in {'forwards', 'defense'}:
            raise ValueError("Can only adjust 'forwards' or 'defense' categories.")
        if delta not in {-1, 1}:
            raise ValueError("Delta must be either +1 or -1.")
        
        current_slots = getattr(self, category)
        new_slot_count = len(current_slots) + delta
        
        if category == 'forwards':
            if not (11 <= new_slot_count <= 13):
                raise ValueError("Number of forwards can only vary by ±1 from the default of 12.")
        elif category == 'defense':
            if not (5 <= new_slot_count <= 7):
                raise ValueError("Number of defensemen can only vary by ±1 from the default of 6.")
        
        if delta == 1:
            current_slots.append(None)
        elif delta == -1:
            removed_player = current_slots.pop()
            if removed_player:
                print(f"Removed player '{removed_player.name}' from {category}.")
        
        setattr(self, category, current_slots)
        print(f"Adjusted {category} slots to {len(getattr(self, category))}.")
        self.validate_lineup_size()
    
    def display_lineup(self):
        """
        Prints the current lineup in a structured format.
        
        The output includes:
            - Lineup name
            - Forwards section with slot numbers and player info
            - Defense section with slot numbers and player info
            - Goalies section with slot numbers and player info
        
        Empty slots are marked as 'Empty'.
        """
        print(f"Lineup: {self.name}\n")
        
        for category, title in [('forwards', 'Forwards'), ('defense', 'Defense'), ('goalies', 'Goalies')]:
            print(f"{title}:")
            for idx, player in enumerate(getattr(self, category), start=1):
                player_info = str(player) if player else 'Empty'
                print(f"  Slot {idx}: {player_info}")
            print()
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Converts the lineup into a pandas DataFrame.
        
        Returns:
            pd.DataFrame: A DataFrame with columns:
                - Position: Position code with slot number (e.g., 'f1', 'd1', 'g1')
                - Player: Player name or 'Empty'
                - Player ID: (optional) Player's ID if available
                
        Note:
            Columns containing only NaN values are automatically dropped.
        """
        data = []
        for category, pos in [('forwards', 'f'), ('defense', 'd'), ('goalies', 'g')]:
            for idx, player in enumerate(getattr(self, category), start=1):
                player_dict = {
                    'Position': f"{pos}{idx}",
                    'Player': player.name if player else 'Empty'
                }
                # Conditionally add 'player_id' if it exists
                if player and player.player_id is not None:
                    player_dict['Player ID'] = player.player_id
                data.append(player_dict)
        
        df = pd.DataFrame(data)
        
        # Optionally, remove columns where all values are NaN
        df.dropna(axis=1, how='all', inplace=True)
        
        return df
    
    def to_transposed_dataframe(self) -> pd.DataFrame:
        """
        Creates a transposed version of the lineup DataFrame.
        
        Returns:
            pd.DataFrame: A single-row DataFrame where:
                - Each column represents a position-slot combination
                - Column names are position codes with slot numbers (e.g., 'f1', 'd1', 'g1')
                - Values are player names
                - Additional columns with '_ID' suffix contain player IDs if available
        """
        df = self.to_dataframe()
        
        # Initialize dictionaries to hold player names and optional IDs
        player_data = {}
        id_data = {}
        
        for _, row in df.iterrows():
            pos = row['Position']
            player_name = row['Player']
            player_data[pos] = player_name
            
            # Handle 'Player ID' if it exists
            if 'Player ID' in row and pd.notna(row['Player ID']):
                id_data[f"{pos}_ID"] = row['Player ID']
        
        # Combine player names and IDs into a single dictionary
        transposed_data = {**player_data, **id_data}
        
        # Create the transposed DataFrame with a single row
        transposed_df = pd.DataFrame([transposed_data])
        
        return transposed_df