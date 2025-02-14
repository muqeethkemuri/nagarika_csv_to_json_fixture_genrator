import pandas as pd
import json
from datetime import datetime
from slugify import slugify
from typing import Dict, List, Optional, Any, Tuple

class CategoryConverter:
    def __init__(self):
        self.pk_counter = 4000
        self.data_pk_counter = 7000
        self.movement_pk_counter = 3000
        self.timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        self.pk_map: Dict[str, int] = {}
        self.explanation_slug_map: Dict[str, Dict[str, str]] = {}  # Store explanation slugs

    def load_explanation_slugs(self, explanation_files: Dict[str, str]):
        """Loads slugs from explanation CSVs for later lookup."""
        for file_type, csv_file in explanation_files.items():
            try:
                df_explanation = pd.read_csv(csv_file)
            except (FileNotFoundError, pd.errors.EmptyDataError, Exception) as e:
                print(f"Error reading or processing explanation file '{csv_file}': {e}")
                continue  # Skip to the next file on error

            level_cols = self.get_level_columns(df_explanation)
            slug_dict = {}
            for _, row in df_explanation.iterrows():
                level_key = "|".join(str(row[col]) if pd.notna(row[col]) else "" for col in level_cols)
                slug_column = 'movie file name' if 'explanation_unit' in csv_file else level_cols[-1]
                slug = row.get(slug_column, '')
                if pd.notna(slug):
                    slug_dict[level_key] = str(slug).replace(".mp4", "").replace(".MP4", "")  # Clean up slug
            self.explanation_slug_map[file_type] = slug_dict
        #print("Explanation Slug Map:", self.explanation_slug_map)

    def create_category(self, name: str, parent_pk: Optional[int] = None, sort: int = 1, has_data: int = 1, category_type: Optional[str] = None, type: str = "SEQUENCE") -> dict:
        """Create a category dictionary."""
        category = {
            "model": "kalari.Categories",
            "pk": self.pk_counter,
            "fields": {
                "name": str(name),
                "slug": slugify(str(name)),
                "type": type,
                "parent": parent_pk,
                "has_data": has_data,
                "is_active": 1,
                "created_at": self.timestamp,
                "updated_at": self.timestamp,
                "sort": sort
            }
        }
        if category_type and parent_pk is None:
            category["fields"]["category_type"] = category_type
        self.pk_counter += 1
        return category


    def create_category_data(self, title: str, category_id: int, direction: str) -> dict:
        """Create a category data dictionary."""
        data = {
            "model": "kalari.CategoriesData",
            "pk": self.data_pk_counter,
            "fields": {
                "title": str(title),
                "category_id": category_id,
                "direction": direction,
                "created_at": self.timestamp,
                "updated_at": self.timestamp
            }
        }
        self.data_pk_counter += 1
        return data

    def create_category_data_url(self, category_data_id: int, filename: str) -> dict:
        """Create a category data URL dictionary."""
        data_url = {
            "model": "kalari.CategoriesDataUrls",
            "pk": self.data_pk_counter,
            "fields": {
                "path": f"odissi/sequence/{filename}",  # Use filename directly
                "type": "video",
                "category_data_id": category_data_id
            }
        }
        self.data_pk_counter+=1 # Incrementing pk here for url entries
        return data_url

    def create_category_movement(self, category_data_id: int, title: str, file_type:str, related_explanation_slug: Optional[str] = None) -> dict:
        """Create a categories_movement dictionary."""

        # Determine the movement type based on file type

        if file_type in ('sequence_menu','unit_menu'):
           movement_type = file_type.split('_')[0].upper()
        elif file_type in ('explanation_sequence','explanation_unit'):
            movement_type = 'EXPLANATION'
        else:
          movement_type = "CONTEXT"



        movement = {
            "model": "kalari.CategoriesMovements",
            "pk": self.movement_pk_counter,
            "fields": {
                "name": title,
                "category_data_movements_id": category_data_id,
                "type": movement_type,
                "is_related_only": False  # Default to False
            }
        }
        if file_type in ('sequence_menu','unit_menu'):
           movement["fields"]["is_related_only"]= True # set to true
        if related_explanation_slug:
            movement["fields"]["related_explanation_slug"] = related_explanation_slug
        self.movement_pk_counter += 1
        return movement

    def get_level_columns(self, df: pd.DataFrame) -> List[str]:
        """Get all level columns from the DataFrame."""
        df.columns = df.columns.str.strip()
        level_cols = [col for col in df.columns if 'level' in col.lower()]
        level_cols.sort()
        return level_cols

    def process_row(self, row: pd.Series, level_columns: List[str], categories: List[dict],
                    categories_data: List[dict], categories_data_urls: List[dict],
                    categories_movements: List[dict], current_path: List[str],
                    sort_counters: Dict[str, int], file_type: str) -> None:

        last_category_id = None
        
        # Iterate through levels to build category hierarchy
        for i, col in enumerate(level_columns):
            if pd.notna(row[col]):
                current_path[i:] = [str(row[col])] # Keep the type consistent as string
                if len(current_path) > i + 1: # correct path length if necessary
                    current_path = current_path[:i+1]
                # Create a unique key for the current category level
                full_path = '|'.join(filter(None, current_path[:i+1]))

                if full_path in self.pk_map:
                    last_category_id = self.pk_map[full_path]
                    continue #skip if category exists


                # Determine the parent category's PK
                parent_pk = None
                if i > 0:
                    parent_path = '|'.join(filter(None, current_path[:i]))
                    parent_pk = self.pk_map.get(parent_path)  # Use .get() to handle potential missing keys
                parent_path = '|'.join(filter(None,current_path[:i])) if i> 0 else ''

                sort_key = f"{parent_path}|{col}"
                if sort_key not in sort_counters:
                   sort_counters[sort_key] = 1


                category = self.create_category(
                    name=row[col],
                    parent_pk=parent_pk,
                    sort=sort_counters[sort_key],
                    has_data=0 if i < len(level_columns)-1 else 1,  # Only the last level has data
                    category_type="ODISSI" if i == 0 and file_type != "context_menu" else None, # Apply only to the first level
                    type = "CONTEXT" if file_type == "context_menu" and i < len(level_columns) -1 else "SEQUENCE" if file_type == "sequence_menu" else "UNIT" # Set correct category type, UNIT for unit_menu
                )

                self.pk_map[full_path] = category["pk"]  # Store the PK for later reference
                categories.append(category)
                last_category_id = category["pk"]
                sort_counters[sort_key] += 1 # Increment the counter for this level


        # Create CategoryData and CategoryDataUrls entries (only for the deepest level)
        if last_category_id: # proceed only if category id is present
             title = str(row[level_columns[-1]]) # Get the title from the last level column
             front_file = row.get('Front') or row.get('front') or row.get('movie file name') or row.get('Movie File Name') or row.get('movie_file_name') or row.get('Movie_File_Name') or row.get('FileName') or row.get('filename') or row.get('FILENAME') # added various possible column names for front
             side_file = row.get('Side') or row.get('side')

             # Handle Front video
             if front_file and pd.notna(front_file):
                data_entry = self.create_category_data(title, last_category_id, "front")
                categories_data.append(data_entry)
                categories_data_urls.append(self.create_category_data_url(data_entry["pk"], str(front_file).strip()))
                explanation_key = "|".join(filter(None, current_path))
                explanation_slug = self.explanation_slug_map.get(file_type, {}).get(explanation_key, None)
                movement = self.create_category_movement(data_entry["pk"], title, file_type, explanation_slug)
                categories_movements.append(movement)
                


             # Handle Side video
             if side_file and pd.notna(side_file):
                data_entry = self.create_category_data(title, last_category_id, "side")
                categories_data.append(data_entry)
                categories_data_urls.append(self.create_category_data_url(data_entry["pk"], str(side_file).strip()))
                explanation_key = "|".join(filter(None, current_path))
                explanation_slug = self.explanation_slug_map.get(file_type, {}).get(explanation_key, None)
                movement = self.create_category_movement(data_entry["pk"], title, file_type, explanation_slug)
                categories_movements.append(movement)



    def generate_categories_json(self, csv_files: List[str], explanation_files: Dict[str, str]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
        self.load_explanation_slugs(explanation_files)

        categories = []
        categories_data = []
        categories_data_urls = []
        categories_movements = []
        for csv_file in csv_files:
            print(f"Processing file: {csv_file}")
            try:
                df = pd.read_csv(csv_file)
            except Exception as e:
                print(f"Error reading file '{csv_file}': {e}")
                continue

            df.columns = df.columns.str.strip()
            level_columns = self.get_level_columns(df)
            if not level_columns:
                print(f"Skipping file '{csv_file}' due to missing level columns.")
                continue

            current_path = [''] * len(level_columns)
            sort_counters: Dict[str, int] = {}  # Counter for unique sorting

            # Determine file type for setting appropriate relationship
            if 'sequence_menu' in csv_file:
                file_type = 'sequence_menu'
            elif 'unit_menu' in csv_file:
                file_type = 'unit_menu'
            elif 'context_menu' in csv_file:
                file_type = "context_menu"
            elif "explanation_unit" in csv_file: # for explanation unit
                file_type = "explanation_unit"
            elif "explanation_sequence" in csv_file: # for explanation sequence
                file_type = "explanation_sequence"
            else:
                file_type = 'unknown'  # Fallback

            for _, row in df.iterrows():
                self.process_row(row, level_columns, categories, categories_data,
                                  categories_data_urls, categories_movements,
                                  current_path, sort_counters, file_type)

        return categories, categories_data, categories_data_urls, categories_movements


def save_json(data: List[dict], output_file: str) -> None:
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
def main():
     try:
        csv_files = [
            "context_menu.csv",
            "sequence_menu.csv",
            "unit_menu.csv"
        ]

        explanation_files = { # Define explanation files and types
            "explanation_sequence": "explanation_sequence.csv",
            "explanation_unit": "explanation_unit.csv"
        }
         # Check if files exist (including explanation files)
        all_files = csv_files + list(explanation_files.values())
        for file_path in all_files:
             try:
                 with open(file_path, 'r'):
                     pass
             except FileNotFoundError:
                print(f"Error: The file '{file_path}' was not found.")
                print("Please make sure your CSV files are in the same directory as this script.")
                exit(1)

        converter = CategoryConverter()
        categories, categories_data, categories_data_urls, categories_movements = converter.generate_categories_json(csv_files, explanation_files) # Pass explanation_files


        save_json(categories, 'categories.json')
        save_json(categories_data, 'categories_data.json')
        save_json(categories_data_urls, 'categories_data_urls.json')
        save_json(categories_movements, 'categories_movements.json')

        print(f"Generated {len(categories)} categories successfully!")
        print(f"Generated {len(categories_data)} category data entries!")
        print(f"Generated {len(categories_data_urls)} category data URLs!")
        print(f"Generated {len(categories_movements)} category movements successfully!")

     except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()