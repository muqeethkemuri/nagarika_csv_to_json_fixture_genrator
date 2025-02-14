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
        self.explanation_slug_map: Dict[str, Dict[str, str]] = {} # Store explanation slugs

    def load_explanation_slugs(self, explanation_files: Dict[str, str]): # Load explanation CSVs
        for file_type, csv_file in explanation_files.items():
            df_explanation = pd.read_csv(csv_file)
            level_cols = self.get_level_columns(df_explanation)
            slug_dict = {}
            for _, row in df_explanation.iterrows():
                level_key = "|".join(str(row[col]) if pd.notna(row[col]) else "" for col in level_cols)
                print("Explanation Key in load:", level_key) # DEBUG PRINT
                slug_column = 'movie file name' if 'explanation_unit' in csv_file else level_cols[-1] # handle both explanation files
                slug = row.get('movie file name', '') if 'explanation_unit' in csv_file else row.get(slug_column, '') # Get movie file name as slug for explanation
                if pd.notna(slug): # Check if slug is not NaN
                    slug_str = str(slug) # Convert to string to handle potential float issues
                    slug_dict[level_key] = slug_str.replace(".mp4", "").replace(".MP4","") # clean up slug
            self.explanation_slug_map[file_type] = slug_dict
        print("Explanation Slug Map:", self.explanation_slug_map) # DEBUG PRINT


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
        self.data_pk_counter += 1
        return {
            "model": "kalari.CategoriesDataUrls",
            "pk": self.data_pk_counter,
            "fields": {
                "path": f"odissi/sequence/{filename}",
                "type": "video",
                "category_data_id": category_data_id
            }
        }

    def create_category_movement(self, category_data_id: int, related_slug: str, movement_type: str, is_related_only: bool = False) -> dict:
        """Create a categories_movement dictionary."""
        self.movement_pk_counter += 1
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
        categories_movements: List[dict],
        current_path: List[str],
        sort_counters: Dict[str, int],
        file_type: str  # ADDED: file_type parameter
    ) ->None:
        print("Level Columns for current CSV:", level_columns) # DEBUG PRINT
        last_category_id = None

        for i, col in enumerate(level_columns):
            if pd.notna(row[col]):
                current_path[i:] = [str(row[col])]
                if len(current_path) > i + 1:
                    current_path = current_path[:i + 1]
                full_path = '|'.join(filter(None, current_path[:i + 1]))

                if full_path in self.pk_map:
                    last_category_id = self.pk_map[full_path]
                    continue

                parent_pk = None
                if i > 0 and len(current_path) > 1:
                    parent_path = '|'.join(filter(None, current_path[:i]))
                    parent_pk = self.pk_map.get(parent_path)

                parent_path = '|'.join(filter(None, current_path[:i])) if i > 0 else ''
                sort_key = f"{parent_path}|{col}"
                if sort_key not in sort_counters:
                    sort_counters[sort_key] = 1

                category = self.create_category(
                    name=str(row[col]),
                    parent_pk=parent_pk,
                    sort=sort_counters[sort_key],
                    has_data=0 if i == 0 else 1,
                    category_type="ODISSI" if i == 0 else None
                )
                self.pk_map[full_path] = category["pk"]
                last_category_id = category["pk"]
                categories.append(category)
                sort_counters[sort_key] += 1

        if last_category_id:
            title = None
            for col in reversed(level_columns):
                if pd.notna(row[col]):
                    title = str(row[col])
                    break
            if title is None:
                title = "Untitled"

            front_file = row.get('Front') or row.get('front') or row.get('movie file name') or row.get('Movie File Name') or row.get('movie_file_name') or row.get('Movie_File_Name') or row.get('FileName') or row.get('filename') or row.get('FILENAME')
            side_file = row.get('Side') or row.get('side')

            if front_file and pd.notna(front_file):
                front_data = self.create_category_data(
                    title=title,
                    category_id=last_category_id,
                    direction="front"
                )
                categories_data.append(front_data)

                front_url = self.create_category_data_url(
                    category_data_id=front_data["pk"],
                    filename=str(front_file).strip()
                )
                categories_data_urls.append(front_url)

                # Construct key for explanation lookup
                explanation_key = "|".join(filter(None, current_path))
                explanation_slug = self.explanation_slug_map.get(file_type, {}).get(explanation_key)

                print("Explanation Key:", explanation_key, "Explanation Slug:", explanation_slug, "File Type:", file_type) # DEBUG PRINT

                if explanation_slug:  # Lookup explanation slug and create movement
                    print("Explanation Slug FOUND:", explanation_slug) # DEBUG PRINT
                    movement_type = "explanation" if file_type in ["explanation_sequence", "explanation_unit"] else "context" # Determine movement type based on file_type
                    movement = self.create_category_movement(front_data["pk"], explanation_slug, movement_type, is_related_only=True if file_type == "sequence_menu" else False) # is_related_only for sequences only
                    categories_movements.append(movement)


            if side_file and pd.notna(side_file):
                side_data = self.create_category_data(
                    title=title,
                    category_id=last_category_id,
                    direction="side"
                )
                categories_data.append(side_data)

                side_url = self.create_category_data_url(
                    category_data_id=side_data["pk"],
                    filename=str(side_file).strip()
                )
                categories_data_urls.append(side_url)

                explanation_key = "|".join(filter(None, current_path))
                explanation_slug = self.explanation_slug_map.get(file_type, {}).get(explanation_key)

                if explanation_slug: # Lookup explanation slug and create movement
                    movement_type = "explanation" if file_type in ["explanation_sequence", "explanation_unit"] else "context" # Determine movement type based on file_type
                    movement = self.create_category_movement(side_data["pk"], explanation_slug, movement_type, is_related_only=True if file_type == "sequence_menu" else False) # is_related_only for sequences only
                    categories_movements.append(movement)


    def generate_categories_json(self, csv_files: List[str], explanation_files: Dict[str, str]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
        """Generate all category-related JSON data from CSV files, including explanation lookup."""
        # Load explanation slugs first
        self.load_explanation_slugs(explanation_files)

        categories: List[dict] = []
        categories_data: List[dict] = []
        categories_data_urls: List[dict] = []
        categories_movements: List[dict] = []
        current_path: List[str] = []
        sort_counters: Dict[str, int] = {}

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
            file_type = ""
            if "sequence_menu" in csv_file:
                file_type = "sequence_menu"
            elif "unit_menu" in csv_file:
                file_type = "unit_menu"
            elif "context_menu" in csv_file:
                file_type = "context_menu"
            elif "explanation_unit" in csv_file: # for explanation unit
                file_type = "explanation_unit"
            elif "explanation_sequence" in csv_file: # for explanation sequence
                file_type = "explanation_sequence"


            for _, row in df.iterrows():
                self.process_row(
                    row,
                    level_columns,
                    categories,
                    categories_data,
                    categories_data_urls,
                    categories_movements,
                    current_path,
                    sort_counters,
                    file_type # ADDED: Pass file_type
                )

        return categories, categories_data, categories_data_urls, categories_movements


def save_json(data: List[dict], output_file: str) -> None:
    """Save data to JSON file."""
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

        save_json(categories_movements, 'categories_movements.json') # save categories_movements json here
        save_json(categories, 'categories.json')
        save_json(categories_data, 'categories_data.json')
        save_json(categories_data_urls, 'categories_data_urls.json')


        print(f"Generated {len(categories)} categories successfully!")
        print(f"Generated {len(categories_data)} category data entries!")
        print(f"Generated {len(categories_data_urls)} category data URLs!")
        print(f"Generated {len(categories_movements)} category movements successfully!")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()