import pandas as pd
import json
from datetime import datetime
from slugify import slugify
from typing import Dict, List, Optional, Any, Tuple

class CategoryConverter:
    def __init__(self):
        self.pk_counter = 4000
        self.data_pk_counter = 7000
        self.movement_pk_counter = 3000  # Initialize movement PK counter
        self.timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        self.pk_map: Dict[str, int] = {}  # Maps full path to pk

    def create_category(
        self,
        name: str,
        parent_pk: Optional[int] = None,
        sort: int = 1,
        has_data: int = 1,
        category_type: Optional[str] = None
    ) -> dict:
        """Create a category dictionary with the given parameters."""
        category = {
            "model": "kalari.Categories",
            "pk": self.pk_counter,
            "fields": {
                "name": str(name),
                "slug": slugify(str(name)),
                "type": "SEQUENCE",
                "parent": parent_pk,
                "has_data": has_data,
                "is_active": 1,
                "created_at": self.timestamp,
                "updated_at": self.timestamp,
                "sort": sort
            }
        }
        
        # Add category_type only for top-level categories
        if category_type and parent_pk is None:
            category["fields"]["category_type"] = category_type
            
        self.pk_counter += 1
        return category

    def create_category_data(
        self,
        title: str,
        category_id: int,
        direction: str
    ) -> dict:
        """Create a category data dictionary."""
        data = {
            "model": "kalari.CategoriesData",
            "pk": self.data_pk_counter,
            "fields": {
                "title": title,
                "category_id": category_id,
                "direction": direction,
                "created_at": self.timestamp,
                "updated_at": self.timestamp
            }
        }
        self.data_pk_counter += 1
        return data

    def create_category_data_url(
        self,
        category_data_id: int,
        filename: str
    ) -> dict:
        """Create a category data URL dictionary."""
        return {
            "model": "kalari.CategoriesDataUrls",
            "pk": category_data_id,
            "fields": {
                "path": f"odissi/sequence/{filename}",
                "type": "video",
                "category_data_id": category_data_id
            }
        }
        
    def create_category_movement(self, category_data_id: int, related_slug: str, movement_type: str, is_related_only: bool = False) -> dict:
        """Create a categories_movement dictionary."""
        movement = {
            "model": "kalari.CategoriesMovements",
            "pk": self.movement_pk_counter,
            "fields": {
                "name": "Related " + movement_type.capitalize(),  # Descriptive name
                "category_data_movements_id": category_data_id,
                "related_" + movement_type.lower() + "_slug": related_slug,
                "type": movement_type.upper(),  # SEQUENCE, UNIT, EXPLANATION, CONTEXT
                "is_related_only": is_related_only  # True if the CategoryData is only related to that 
            }
        }
        self.movement_pk_counter += 1
        return movement

    def get_level_columns(self, df: pd.DataFrame) -> List[str]:
        """Get all level columns from the DataFrame."""
        # Clean column names by stripping whitespace
        df.columns = df.columns.str.strip()
        
        # Get all columns that contain the word 'level' (case-insensitive)
        level_cols = [col for col in df.columns if 'level' in col.lower()]
        
        if not level_cols:
            print("Available columns in the Excel file:")
            for col in df.columns:
                print(f"  - '{col}'")  # Added quotes to show whitespace
            raise ValueError("No 'level' columns found in the Excel file. Column names should contain the word 'level'.")
        
        # Sort them to ensure they're in the correct order
        level_cols.sort()
        return level_cols

    def process_row(
        self,
        row: pd.Series,
        level_columns: List[str],
        categories: List[dict],
        categories_data: List[dict],
        categories_data_urls: List[dict],
        categories_movements: List[dict],  # ADDED: categories_movements
        current_path: List[str],
        sort_counters: Dict[str, int]
    ) -> None:
        """Process a single row and update all category-related lists."""
        last_category_id = None
        
        for i, col in enumerate(level_columns):
            if pd.notna(row[col]):
                # Convert value to string and update current path
                current_path[i:] = [str(row[col])]
                if len(current_path) > i + 1:
                    current_path = current_path[:i + 1]
                
                # Get the full path string for this category
                full_path = '|'.join(filter(None, current_path[:i + 1]))
                
                # Skip if we've already processed this category
                if full_path in self.pk_map:
                    last_category_id = self.pk_map[full_path]
                    continue
                
                # Get parent pk if not top level
                parent_pk = None
                if i > 0 and len(current_path) > 1:
                    parent_path = '|'.join(filter(None, current_path[:i]))
                    parent_pk = self.pk_map.get(parent_path)
                
                # Get sort counter for this level and path
                parent_path = '|'.join(filter(None, current_path[:i])) if i > 0 else ''
                sort_key = f"{parent_path}|{col}"
                if sort_key not in sort_counters:
                    sort_counters[sort_key] = 1
                
                # Create category
                category = self.create_category(
                    name=str(row[col]),
                    parent_pk=parent_pk,
                    sort=sort_counters[sort_key],
                    has_data=0 if i == 0 else 1,
                    category_type="ODISSI" if i == 0 else None
                )
                
                # Store pk for future reference
                self.pk_map[full_path] = category["pk"]
                last_category_id = category["pk"]
                
                # Add category to list
                categories.append(category)
                
                # Increment sort counter
                sort_counters[sort_key] += 1

        # Process front and side data if this is a leaf category (last level)
        if last_category_id:
            title = None
            for col in reversed(level_columns):
                if pd.notna(row[col]):
                    title = str(row[col])
                    break
            if title is None:
                title = "Untitled"  # Fallback title
            
            # Create front view data
            front_file = row.get('Front') or row.get('front') or row.get('movie file name') or row.get('Movie File Name') or row.get('movie_file_name') or row.get('Movie_File_Name') or row.get('FileName') or row.get('filename') or row.get('FILENAME')
            side_file = row.get('Side') or row.get('side')

            if front_file and pd.notna(front_file):    
                front_data = self.create_category_data(
                    title=title,
                    category_id=last_category_id,
                    direction="front"
                )
                categories_data.append(front_data)
                
                # Create front view URL using the filename from Excel
                front_url = self.create_category_data_url(
                    category_data_id=front_data["pk"],
                    filename=str(front_file).strip()
                )
                categories_data_urls.append(front_url)

                 # Create CategoriesMovements entry for front data
                explanation_slug = row.get('explanation_slug') or row.get('Explanation')
                sequence_slug = row.get('sequence_slug') or row.get('Sequence')
                if explanation_slug and pd.notna(explanation_slug):
                     movement = self.create_category_movement(front_data["pk"], str(explanation_slug).strip(), "explanation")
                     categories_movements.append(movement) 
                elif sequence_slug and pd.notna(sequence_slug):
                     movement = self.create_category_movement(front_data["pk"], str(sequence_slug).strip(), "sequence")
                     categories_movements.append(movement)
            
            if side_file and pd.notna(side_file):
            
                # Create side view data
                side_data = self.create_category_data(
                    title=title,
                    category_id=last_category_id,
                    direction="side"
                )
                categories_data.append(side_data)
                
                # Create side view URL using the filename from Excel
                side_url = self.create_category_data_url(
                    category_data_id=side_data["pk"],
                    filename=str(side_file).strip()
                )
                categories_data_urls.append(side_url)
            
                # Create CategoriesMovements entry for side data
                explanation_slug = row.get('explanation_slug') or row.get('Explanation')
                sequence_slug = row.get('sequence_slug') or row.get('Sequence')

                if explanation_slug and pd.notna(explanation_slug):
                    movement = self.create_category_movement(side_data["pk"], str(explanation_slug).strip(), "explanation")
                    categories_movements.append(movement)
                elif sequence_slug and pd.notna(sequence_slug):
                    movement = self.create_category_movement(side_data["pk"], str(sequence_slug).strip(), "sequence")
                    categories_movements.append(movement)

    def generate_categories_json(self, excel_files: List[str]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
        """Generate all category-related JSON data from a list of Excel files."""
        # Initialize variables
        categories: List[dict] = []
        categories_data: List[dict] = []
        categories_data_urls: List[dict] = []
        categories_movements: List[dict] = []  # ADDED: categories_movements
        current_path: List[str] = []
        sort_counters: Dict[str, int] = {}
        
        for excel_file in excel_files:
            print(f"Processing file: {excel_file}")
            # Read Excel file
            df = pd.read_excel(excel_file)
            
            # Clean column names by stripping whitespace
            df.columns = df.columns.str.strip()
            
            # Get level columns
            level_columns = self.get_level_columns(df)
            if not level_columns:
                print(f"Skipping file '{excel_file}' due to missing or invalid level columns.")
                continue  # Skip to the next file

            current_path = [''] * len(level_columns)
            
            print("Found level columns:", level_columns)
            print("All columns in Excel:", list(df.columns))
            
            # Process each row
            for _, row in df.iterrows():
                self.process_row(
                    row,
                    level_columns,
                    categories,
                    categories_data,
                    categories_data_urls,
                    categories_movements, # ADDED: Passing categories_movements
                    current_path,
                    sort_counters
                )
        
        return categories, categories_data, categories_data_urls, categories_movements

def save_json(data: List[dict], output_file: str) -> None:
    """Save data to JSON file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    try:
        # Prompt user for Excel file names
        excel_files = input("Enter the names of the Excel files (comma-separated): ").split(',')
        excel_files = [f.strip() for f in excel_files]  # Remove leading/trailing spaces
        
        # Check if files exist
        for excel_file in excel_files:
            try:
                with open(excel_file, 'rb'):
                    pass
            except FileNotFoundError:
                print(f"Error: The file '{excel_file}' was not found.")
                print("Please make sure your Excel files are in the same directory as this script.")
                exit(1)
        
        # Create converter and generate all category data
        converter = CategoryConverter()
        categories, categories_data, categories_data_urls, categories_movements = converter.generate_categories_json(excel_files)
        
        # Save all JSON files
        save_json(categories, 'categories.json')
        save_json(categories_data, 'categories_data.json')
        save_json(categories_data_urls, 'categories_data_urls.json')
        save_json(categories_movements, 'categories_movements.json')  # ADDED: Save categories_movements.json
        
        print(f"Generated {len(categories)} categories successfully!")
        print(f"Generated {len(categories_data)} category data entries!")
        print(f"Generated {len(categories_data_urls)} category data URLs!")
        print(f"Generated {len(categories_movements)} category movements successfully!") # Added : Printing success messages

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()