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
                level_key = "|".join(str(row[col]) if pd.notna(row[col]) else "" for col in level_cols)
                slug_column = level_cols[-1]
                slug_value = row.get(slug_column)

                if pd.notna(slug_value):
                   slug_dict[level_key] = slugify(str(slug_value))

            self.explanation_slug_map[file_type] = slug_dict


    def create_category(self, name: str, parent_pk: Optional[int] = None, sort: int = 1, has_data: int = 1, type: str = "SEQUENCE") -> dict:
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
                "category_type" : "ODISSI"
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
        file_type_no_suffix = file_type.split('_')[0] 
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
      file_type_no_suffix = file_type.split('_')[0]
      movement_type = file_type_no_suffix.upper()
        # If explanation file, create a related explanation movement
      movement = {
            "model": "kalari.CategoriesMovements",
            "pk": self.pk_counter_movements,
            "fields": {
                "name": title,
                "category_data_movements_id": category_data_id,
                "type": movement_type,
                "is_related_only": False
            }
        }

      if related_slug_name and related_slug_type:
            movement["fields"]["is_related_only"] = True
            movement["fields"][f"related_{related_slug_type.lower()}_slug"] = related_slug_name
            if start_time:
              movement["fields"]["start_time"] = int(start_time)
            if end_time:
             movement["fields"]["end_time"] = int(end_time)
      self.pk_counter_movements += 1
      return movement

    def get_level_columns(self, df: pd.DataFrame) -> List[str]:
        """Get all level columns from the DataFrame."""

        level_cols = [col.lower() for col in df.columns if 'level' in col.lower()] #convert to lower case to handle variations
        level_cols.sort()
        return level_cols

    def process_row(self, row: pd.Series, level_columns: List[str], categories: List[dict],
                    categories_data: List[dict], categories_data_urls: List[dict],
                    categories_movements: List[dict], current_path: List[str],
                    sort_counters: Dict[str, int], file_type: str, top_level:bool) -> None:

        # Iterate through each level in the row
        last_category_id = None
        for i, col in enumerate(level_columns):
            if pd.notna(row[col]):
                current_path[i:] = [str(row[col])]
                full_path = "|".join(filter(None, current_path[:i + 1]))

                if full_path not in self.pk_map:
                    parent_path = "|".join(filter(None, current_path[:i]))
                    parent_pk = self.pk_map.get(parent_path) # This handles the top level
                    sort_key = "|".join(filter(None, current_path[:i + 1]))  # Include current level
                    if sort_key not in sort_counters:
                        sort_counters[sort_key] = 1


                    has_data = 0 if any(col in row and pd.notna(row[col]) for col in ['front', 'side', 'movie file name','FileName']) else 0 if any(pd.notna(row[level_columns[j]]) for j in range(i + 1, len(level_columns))) else 1
                   

                    category_type = "CONTEXT" if file_type == "context_menu" else "EXPLANATION" if "explanation" in file_type else "UNIT" if "unit_menu" in file_type else "SEQUENCE"
                    category = self.create_category(
                        name=row[col],
                        parent_pk=parent_pk,
                        sort=sort_counters[sort_key],
                        has_data=has_data,  # Correctly set has_data
                        type=category_type,
                    )
                    self.pk_map[full_path] = category["pk"]
                    categories.append(category)
                    sort_counters[sort_key] += 1

                last_category_id = self.pk_map[full_path]



        if last_category_id and ('front' in row or 'side' in row or 'movie file name' in row or 'FileName' in row):
            title = current_path[-1]  # Title is the last non-empty level
            front_file = row.get('front') or row.get('movie file name') or row.get('FileName')
            side_file = row.get('side')
            direction = row.get('direction','')


            data_entry = self.create_category_data(title, last_category_id, direction)
            categories_data.append(data_entry)

            if front_file and pd.notna(front_file):
                categories_data_urls.append(self.create_category_data_url(data_entry["pk"], str(front_file).strip(), file_type))

            if side_file and pd.notna(side_file):
                categories_data_urls.append(self.create_category_data_url(data_entry["pk"], str(side_file).strip(), file_type))


            explanation_key = "|".join(filter(None, current_path))
            explanation_slug = self.explanation_slug_map.get(file_type.split('_')[0], {}).get(explanation_key)

            if "explanation" in file_type:
                related_slug_type = "explanation"
                related_slug_name = explanation_slug
            elif "sequence" in file_type or "unit" in file_type:
                related_slug_type = "sequence" if "sequence" in file_type else "unit"
                related_slug_name = slugify(str(row[level_columns[-1]])) if pd.notna(row[level_columns[-1]]) else None
            else:
                related_slug_type = None
                related_slug_name = None  # No related unit for context or general data

            start_time = row.get('start_time')
            end_time = row.get('end_time')
            movement = self.create_category_movement(data_entry["pk"], title, file_type, explanation_slug, related_slug_type, start_time, end_time)

            categories_movements.append(movement)





    def generate_categories_json(self, files_and_types: List[Tuple[str, str]], explanation_files: Dict[str, str]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
        """Generates the categories, category data, category data URLs, and
           category movements for JSON output.

        """
        self.load_explanation_slugs(explanation_files)  # Load explanation slugs first

        all_categories = []
        all_categories_data = []
        all_categories_data_urls = []
        all_categories_movements = []

        self.reset_pk_counters()  # Reset counters at the start
        self.pk_map = {}  # Reset the pk_map

        for csv_file, file_type in files_and_types:
            print(f"Processing file: {csv_file} with type: {file_type}")
            try:
                df = pd.read_csv(csv_file)
                if df.empty:
                   print(f"Skipping file '{csv_file}' because it is empty.")
                   continue
            except FileNotFoundError:
                print(f"Error: File '{csv_file}' not found.")
                continue
            except pd.errors.EmptyDataError:
                print(f"Error: File '{csv_file}' is empty.")
                continue
            except Exception as e:
                print(f"Error reading file '{csv_file}': {e}")
                continue


            df.columns = df.columns.str.strip()
            level_columns = self.get_level_columns(df)  # Get level columns dynamically
            if not level_columns:
                print(f"Skipping file '{csv_file}' due to missing level columns.")
                continue

            current_path = [''] * len(level_columns)  # Initialize current_path
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
                                  current_path, sort_counters, file_type, top_level) # Pass file_type

            all_categories.extend(categories)
            all_categories_data.extend(categories_data)
            all_categories_data_urls.extend(categories_data_urls)
            all_categories_movements.extend(categories_movements)


        return all_categories, all_categories_data, all_categories_data_urls,all_categories_movements

def save_json(data: List[dict], output_file: str) -> None:
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    try:
        # Define the input CSV files and their types
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
        # Check if files exist.
        all_files = [file_path for file_path, _ in files_and_types] + list(explanation_files.values())
        for file_path in all_files:
           try:
             with open(file_path, 'r'):
                 pass
           except FileNotFoundError:
                print(f"Error: The file '{file_path}' was not found.")
                print("Please make sure your CSV files are in the same directory as this script.")
                exit(1)

        # Initialize converter
        converter = CategoryConverter()
        # Pass the explanation_files to the converter
        categories, categories_data, categories_data_urls,categories_movements = converter.generate_categories_json(files_and_types, explanation_files)



        # Output the generated JSON data to files
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