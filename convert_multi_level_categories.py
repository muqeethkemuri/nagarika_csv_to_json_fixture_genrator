import pandas as pd
import json
from datetime import datetime
from slugify import slugify
from typing import Dict, List, Optional, Any, Tuple

class CategoryConverter:
    def __init__(self):
        self.pk_counter_categories = 4000
        self.pk_counter_data = 7000
        self.pk_counter_movements = 3000
        self.timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        self.pk_map: Dict[str, int] = {}
        self.explanation_slug_map: Dict[str, Dict[str, str]] = {}

    def reset_pk_counters(self):
        """Resets PK counters for each file processing."""
        self.pk_counter_categories = 4000
        self.pk_counter_data = 7000
        self.pk_counter_movements = 3000

    def load_explanation_slugs(self, explanation_files: Dict[str, str]):
        """Loads slugs from explanation CSVs for later lookup."""
        for file_type, csv_file in explanation_files.items():
            try:
                df_explanation = pd.read_csv(csv_file)
            except (FileNotFoundError, pd.errors.EmptyDataError, Exception) as e:
                print(f"Error reading or processing explanation file '{csv_file}': {e}")
                continue

            level_cols = self.get_level_columns(df_explanation)
            slug_dict = {}
            for _, row in df_explanation.iterrows():
                level_key = "|".join(str(row[col]) if pd.notna(row[col]) else "" for col in level_cols)  # Corrected here
                # Use the last level column as the slug source.
                slug_column = level_cols[-1]
                slug_value = row.get(slug_column)  # Get the value from the last level column
                if pd.notna(slug_value):
                   slug_dict[level_key] = slugify(str(slug_value))  # Use slug_value for slugification
            self.explanation_slug_map[file_type] = slug_dict


    def create_category(self, name: str, parent_pk: Optional[int] = None, sort: int = 1, has_data: int = 1, category_type: Optional[str] = None, type: str = "SEQUENCE") -> dict:
        """Create a category dictionary."""
        category = {
            "model": "kalari.Categories",
            "pk": self.pk_counter_categories,
            "fields": {
                "name": str(name),
                "slug": slugify(str(name)),
                "type": type,
                "parent": parent_pk,
                "has_data": has_data,
                "is_active": 1,
                "created_at": self.timestamp,
                "updated_at": self.timestamp,
                "sort": sort,
                "category_type": category_type or "ODISSI"
            }
        }
        if parent_pk is None:
            category["fields"]["parent"] = None

        self.pk_counter_categories += 1
        return category


    def create_category_data(self, title: str, category_id: int, direction: str) -> dict:
        data = {
            "model": "kalari.CategoriesData",
            "pk": self.pk_counter_data,
            "fields": {
                "title": str(title),
                "category_id": category_id,
                "direction": direction,
                "created_at": self.timestamp,
                "updated_at": self.timestamp
            }
        }
        self.pk_counter_data += 1
        return data

    def create_category_data_url(self, category_data_id: int, filename: str, file_type: str) -> dict:
        """Create a category data URL dictionary, including file type."""
        file_type_no_suffix = file_type.split('_')[0] #Removes context_menu suffixes if it exists
        if file_type_no_suffix == "context":
            path = f"odissi/context/{filename}"
        elif file_type_no_suffix == "sequence":
             path = f"odissi/sequence/{filename}"
        elif file_type_no_suffix == "unit":
             path = f"odissi/unit/{filename}"
        elif file_type_no_suffix == "explanation":
             path = f"odissi/explanation/{filename}"
        else:
            path = f"odissi/{file_type}/{filename}"

        data_url = {
            "model": "kalari.CategoriesDataUrls",
            "pk": self.pk_counter_data,
            "fields": {
                "path": path,
                "type": "video",
                "category_data_id": category_data_id
            }
        }
        self.pk_counter_data+=1
        return data_url

    def create_category_movement(self, category_data_id: int, title: str, file_type:str, related_slug_name: Optional[str] = None, related_slug_type: Optional[str] = None, start_time: Optional[int] = None, end_time: Optional[int] = None) -> dict:
        """Create a categories_movement dictionary."""
        file_type_no_suffix = file_type.split('_')[0]
        movement_type = file_type_no_suffix.upper()

        movement = {
            "model": "kalari.CategoriesMovements",
            "pk": self.pk_counter_movements,
            "fields": {
                "name": title,
                "category_data_movements_id": category_data_id,
                "type": movement_type,
                "is_related_only": False,
            }
        }
        if related_slug_name and related_slug_type:
             movement["fields"][f"related_{related_slug_type.lower()}_slug"] = related_slug_name
             movement["fields"]["is_related_only"] = True
        if start_time is not None:
            movement["fields"]["start_time"] = start_time
        if end_time is not None:
            movement["fields"]["end_time"] = end_time

        self.pk_counter_movements += 1
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
                    sort_counters: Dict[str, int], file_type: str, top_level: bool) -> None:

        last_category_id = None

        for i, col in enumerate(level_columns):
            if pd.notna(row[col]): # Corrected here
                current_path[i:] = [str(row[col])]
                full_path = '|'.join(filter(None, current_path[:i+1]))

                if full_path in self.pk_map:
                    last_category_id = self.pk_map[full_path]
                    continue

                parent_path = '|'.join(filter(None, current_path[:i]))
                parent_pk = self.pk_map.get(parent_path)

                sort_key = f"{parent_path}|{col}"
                if sort_key not in sort_counters:
                   sort_counters[sort_key] = 1

                category_type_val = "ODISSI" if top_level and i == 0 else None

                category = self.create_category(
                    name=row[col],
                    parent_pk=parent_pk,
                    sort=sort_counters[sort_key],
                    has_data=0 if i < len(level_columns)-1 else 1,
                    category_type=category_type_val,
                    type = "CONTEXT" if file_type == "context_menu" else "EXPLANATION" if "explanation" in file_type else "UNIT" if "unit_menu" in file_type else "SEQUENCE"
                )

                self.pk_map[full_path] = category["pk"]
                categories.append(category)
                last_category_id = category["pk"]
                sort_counters[sort_key] += 1

        if last_category_id:
             title = str(row[level_columns[-1]])
             front_file = row.get('Front') or row.get('front') or row.get('movie file name') or row.get('Movie File Name') or row.get('movie_file_name') or row.get('Movie_File_Name') or row.get('FileName') or row.get('filename') or row.get('FILENAME')
             side_file = row.get('Side') or row.get('side')

             direction = row.get('direction','') or ""
             data_entry = self.create_category_data(title, last_category_id, direction) # direction is empty now
             categories_data.append(data_entry)

             if front_file and pd.notna(front_file):
                categories_data_urls.append(self.create_category_data_url(data_entry["pk"], str(front_file).strip(),file_type))
             if side_file and pd.notna(side_file):
                categories_data_urls.append(self.create_category_data_url(data_entry["pk"], str(side_file).strip(),file_type))

             explanation_key = "|".join(filter(None, current_path))
             explanation_slug = self.explanation_slug_map.get(file_type.split('_')[0], {}).get(explanation_key, None)

            # Movement name should be the last level title
             movement_name = str(row[level_columns[-1]])

            # Set related_slug_type correctly
             if "explanation" in file_type:
                related_slug_type = "explanation"
             elif "sequence" in file_type or "unit" in file_type:
                related_slug_type = "sequence" if "sequence" in file_type else "unit"
             else:
                related_slug_type = None
             
             start_time = row.get('start_time')
             end_time = row.get('end_time')
             movement = self.create_category_movement(data_entry["pk"], movement_name, file_type, related_slug_name=explanation_slug, related_slug_type=related_slug_type, start_time=start_time, end_time=end_time)
             categories_movements.append(movement)

    def generate_categories_json(self, files_and_types: List[Tuple[str, str]], explanation_files: Dict[str, str]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
        self.load_explanation_slugs(explanation_files)

        all_categories = []
        all_categories_data = []
        all_categories_data_urls = []
        all_categories_movements = []

        # Reset PK counters only once at the beginning
        self.reset_pk_counters()
        self.pk_map = {}

        for csv_file, file_type in files_and_types:
            print(f"Processing file: {csv_file} with type: {file_type}")
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
            sort_counters: Dict[str, int] = {}

            categories = []
            categories_data = []
            categories_data_urls = []
            categories_movements = []

            # Pass file_type directly and set top_level
            top_level = True if file_type in ['context_menu', 'sequence_menu', 'unit_menu'] else False

            for _, row in df.iterrows():
                self.process_row(row, level_columns, categories, categories_data,
                                  categories_data_urls, categories_movements,
                                  current_path, sort_counters, file_type, top_level)

            all_categories.extend(categories)
            all_categories_data.extend(categories_data)
            all_categories_data_urls.extend(categories_data_urls)
            all_categories_movements.extend(categories_movements)

        return all_categories, all_categories_data, all_categories_data_urls, all_categories_movements

def save_json(data: List[dict], output_file: str) -> None:
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    try:
        # Explicitly define file types
        files_and_types = [
            ("context_menu.csv", "context_menu"),
            ("sequence_menu.csv", "sequence_menu"),
            ("unit_menu.csv", "unit_menu"),
             ("explanation_sequence.csv", "explanation_sequence"),
            ("explanation_unit.csv", "explanation_unit")
        ]

        explanation_files = {
            "explanation_sequence": "explanation_sequence.csv",
            "explanation_unit": "explanation_unit.csv"
        }

        # Check if files exist
        all_files = [file_path for file_path, _ in files_and_types] + list(explanation_files.values())
        for file_path in all_files:
            try:
                with open(file_path, 'r'):
                    pass
            except FileNotFoundError:
                print(f"Error: The file '{file_path}' was not found.")
                print("Please make sure your CSV files are in the same directory as this script.")
                exit(1)


        converter = CategoryConverter()
        categories, categories_data, categories_data_urls, categories_movements = converter.generate_categories_json(files_and_types, explanation_files)

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