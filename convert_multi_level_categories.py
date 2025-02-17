import pandas as pd
import json
from datetime import datetime
from slugify import slugify
from typing import Dict, List, Optional, Tuple

# Mapping of file_type to category type for fixture "type" field.
TYPE_MAP = {
    "context_menu": "CONTEXT",
    "sequence_menu": "SEQUENCE",
    "unit_menu": "UNIT",
    "explanation_sequence": "EXPLANATION",
    "explanation_unit": "EXPLANATION"
}

class CategoryConverter:
    def __init__(self):
        self.pk_counter_categories = 4000
        self.pk_counter_data = 7000
        self.pk_counter_data_urls = 7000
        self.pk_counter_movements = 3000
        # Use a fixed timestamp so fixture files are consistent.
        self.timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        # This will hold the mapping from full hierarchical key to the category pk.
        self.pk_map: Dict[str, int] = {}
        # Explanation slug maps are loaded from explanation CSV files.
        self.explanation_slug_map: Dict[str, Dict[str, str]] = {}
    
    def reset_pk_counters(self):
        self.pk_counter_categories = 4000
        self.pk_counter_data = 7000
        self.pk_counter_data_urls = 7000
        self.pk_counter_movements = 3000

    def load_explanation_slugs(self, explanation_files: Dict[str, str]) -> None:
        # For each explanation file (keys: explanation_sequence, explanation_unit)
        for ftype, csv_file in explanation_files.items():
            try:
                df = pd.read_csv(csv_file)
            except Exception as e:
                print(f"Error reading explanation file '{csv_file}': {e}")
                continue
            # Assume explanation CSV has level columns (e.g. level1, level2, …)
            level_cols = self.get_level_columns(df)
            slug_dict = {}
            for _, row in df.iterrows():
                key = "|".join(str(row[col]).strip() if pd.notna(row[col]) else "" for col in level_cols)
                # Use the last level column as the slug basis
                slug_value = row.get(level_cols[-1])
                if pd.notna(slug_value):
                    slug_dict[key] = slugify(str(slug_value))
            self.explanation_slug_map[ftype] = slug_dict

    def create_category(self, name: str, parent_pk: Optional[int], has_data: int, file_type: str, sort: int) -> dict:
        fields = {
            "name": str(name),
            "slug": slugify(str(name)),
            "type": TYPE_MAP[file_type],
            "parent": parent_pk,
            "has_data": has_data,
            "is_active": 1,
            "created_at": self.timestamp,
            "updated_at": self.timestamp,
            "sort": sort
        }
        # Only top-level categories get category_type field.
        if parent_pk is None:
            fields["category_type"] = "ODISSI"
        category = {
            "model": "kalari.Categories",
            "pk": self.pk_counter_categories,
            "fields": fields
        }
        self.pk_counter_categories += 1
        return category

    def create_category_data(self, title: str, category_id: int, direction: str) -> dict:
        data = {
            "model": "kalari.CategoriesData",
            "pk": self.pk_counter_data,
            "fields": {
                "title": title,
                "category_id": category_id,
                "direction": direction,
                "created_at": self.timestamp,
                "updated_at": self.timestamp
            }
        }
        self.pk_counter_data += 1
        return data

    def create_category_data_url(self, category_data_id: int, filename: str, file_type: str) -> dict:
        # Build path prefix based on file type
        if file_type.startswith("context"):
            prefix = "odissi/context/"
        elif file_type.startswith("sequence"):
            prefix = "odissi/sequence/"
        elif file_type.startswith("unit"):
            prefix = "odissi/unit/"
        elif file_type.startswith("explanation"):
            prefix = "odissi/explanation/"
        else:
            prefix = f"odissi/{file_type}/"
        path = f"{prefix}{filename}"
        data_url = {
            "model": "kalari.CategoriesDataUrls",
            "pk": self.pk_counter_data_urls,
            "fields": {
                "path": path,
                "type": "video",
                "category_data_id": category_data_id
            }
        }
        self.pk_counter_data_urls += 1
        return data_url

    def create_category_movement(self, category_data_id: int, title: str,
                                 movement_type: str,
                                 related_slug_name: Optional[str] = None,
                                 start_time: Optional[int] = None,
                                 end_time: Optional[int] = None,
                                 is_related_only: bool = False) -> dict:
        fields = {
            "name": title,
            "category_data_movements_id": category_data_id,
            "type": movement_type,
            "is_related_only": is_related_only
        }
        if start_time is not None:
            fields["start_time"] = int(start_time)
        if end_time is not None:
            fields["end_time"] = int(end_time)
        movement = {
            "model": "kalari.CategoriesMovements",
            "pk": self.pk_counter_movements,
            "fields": fields
        }
        self.pk_counter_movements += 1
        return movement

    def get_level_columns(self, df: pd.DataFrame) -> List[str]:
        # Return all column names that include "level" (case-insensitive), sorted alphabetically.
        return sorted([col for col in df.columns if "level" in col.lower()])

    def process_row(self, row: pd.Series, level_columns: List[str], file_type: str,
                    sort_counters: Dict[str, int],
                    categories: List[dict], categories_data: List[dict],
                    categories_data_urls: List[dict], categories_movements: List[dict]) -> None:
        # Determine indices of non-null level columns.
        non_null = [i for i, col in enumerate(level_columns) if pd.notna(row[col])]
        if not non_null:
            return
        leaf_index = max(non_null)
        current_path = [""] * len(level_columns)
        last_cat_pk = None
        for i, col in enumerate(level_columns):
            if pd.notna(row[col]):
                current_path[i] = str(row[col]).strip()
                full_key = "|".join(current_path[:i+1])
                # Use a sort counter per hierarchical key to preserve order.
                sort_key = full_key
                if sort_key not in sort_counters:
                    sort_counters[sort_key] = 1
                # Only create a new category if not already created.
                if full_key not in self.pk_map:
                    has_data = 1 if i == leaf_index else 0
                    cat = self.create_category(row[col], 
                                               parent_pk=self.pk_map.get("|".join(current_path[:i])) if i > 0 else None,
                                               has_data=has_data,
                                               file_type=file_type,
                                               sort=sort_counters[sort_key])
                    self.pk_map[full_key] = cat["pk"]
                    categories.append(cat)
                    sort_counters[sort_key] += 1
                last_cat_pk = self.pk_map[full_key]
        # Create a CategoriesData record only for the leaf category.
        leaf_title = current_path[leaf_index]
        # For menus that have file info (sequence_menu, unit_menu, explanation_*), try to get direction and file names.
        direction = ""
        if file_type in ["sequence_menu", "unit_menu", "explanation_sequence", "explanation_unit"]:
            direction = str(row.get("direction", "")).strip()
        data_rec = self.create_category_data(leaf_title, last_cat_pk, direction)
        categories_data.append(data_rec)
        # Determine file fields (try “front”, if not then “movie file name” or “FileName”)
        front_file = None
        side_file = None
        if "front" in row and pd.notna(row["front"]):
            front_file = str(row["front"]).strip()
        elif "movie file name" in row and pd.notna(row["movie file name"]):
            front_file = str(row["movie file name"]).strip()
        if "side" in row and pd.notna(row["side"]):
            side_file = str(row["side"]).strip()
        # Create URL entries if files exist.
        if front_file:
            url_rec = self.create_category_data_url(data_rec["pk"], front_file, file_type)
            categories_data_urls.append(url_rec)
        if side_file:
            url_rec = self.create_category_data_url(data_rec["pk"], side_file, file_type)
            categories_data_urls.append(url_rec)
        # Determine movement type and (if applicable) related explanation slug.
        # For explanation files or for sequence/unit menus, we look up explanation slug from the corresponding explanation CSV.
        movement_type = TYPE_MAP[file_type]
        related_slug = None
        is_related_only = False
        if file_type == "sequence_menu":
            movement_type = "EXPLANATION"
            # Look up explanation slug from explanation_sequence.csv using the joined levels.
            key = "|".join(current_path[:leaf_index+1])
            related_slug = self.explanation_slug_map.get("explanation_sequence", {}).get(key)
            if related_slug:
                is_related_only = True
        elif file_type == "unit_menu":
            movement_type = "EXPLANATION"
            key = "|".join(current_path[:leaf_index+1])
            related_slug = self.explanation_slug_map.get("explanation_unit", {}).get(key)
            if related_slug:
                is_related_only = True
        elif file_type in ["explanation_sequence", "explanation_unit"]:
            movement_type = "EXPLANATION"
            key = "|".join(current_path[:leaf_index+1])
            related_slug = self.explanation_slug_map.get(file_type, {}).get(key)
            if related_slug:
                is_related_only = True
        # For context_menu, keep movement type as CONTEXT.
        mov = self.create_category_movement(data_rec["pk"], leaf_title,
                                              movement_type,
                                              related_slug_name=related_slug,
                                              start_time=row.get("start_time"),
                                              end_time=row.get("end_time"),
                                              is_related_only=is_related_only)
        categories_movements.append(mov)
    
    def generate_categories_json(self, files_and_types: List[Tuple[str, str]],
                                   explanation_files: Dict[str, str]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
        # First, load explanation slugs from explanation CSVs.
        self.load_explanation_slugs(explanation_files)
        all_categories: List[dict] = []
        all_categories_data: List[dict] = []
        all_categories_data_urls: List[dict] = []
        all_categories_movements: List[dict] = []
        self.reset_pk_counters()
        self.pk_map = {}
        sort_counters: Dict[str, int] = {}
        # Process each CSV file.
        for csv_file, file_type in files_and_types:
            try:
                df = pd.read_csv(csv_file)
                if df.empty:
                    print(f"Skipping empty file '{csv_file}'.")
                    continue
            except Exception as e:
                print(f"Error reading '{csv_file}': {e}")
                continue
            df.columns = df.columns.str.strip()
            level_columns = self.get_level_columns(df)
            if not level_columns:
                print(f"File '{csv_file}' does not contain level columns. Skipping.")
                continue
            # Process each row.
            for _, row in df.iterrows():
                self.process_row(row, level_columns, file_type, sort_counters,
                                 all_categories, all_categories_data,
                                 all_categories_data_urls, all_categories_movements)
        return all_categories, all_categories_data, all_categories_data_urls, all_categories_movements

def save_json(data: List[dict], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    # List of CSV files with their associated file type.
    files_and_types = [
        ("context_menu.csv", "context_menu"),
        ("sequence_menu.csv", "sequence_menu"),
        ("unit_menu.csv", "unit_menu"),
        ("explanation_sequence.csv", "explanation_sequence"),
        ("explanation_unit.csv", "explanation_unit")
    ]
    # Explanation CSV files mapping.
    explanation_files = {
        "explanation_sequence": "explanation_sequence.csv",
        "explanation_unit": "explanation_unit.csv"
    }
    # Check that all input files exist.
    for f, _ in files_and_types:
        try:
            with open(f, "r"):
                pass
        except FileNotFoundError:
            print(f"Error: Input file '{f}' not found.")
            exit(1)
    for f in explanation_files.values():
        try:
            with open(f, "r"):
                pass
        except FileNotFoundError:
            print(f"Error: Explanation file '{f}' not found.")
            exit(1)
    converter = CategoryConverter()
    cats, cats_data, cats_data_urls, cats_movs = converter.generate_categories_json(files_and_types, explanation_files)
    save_json(cats, "odissi_categories.json")
    save_json(cats_data, "odissi_categories_data.json")
    save_json(cats_data_urls, "odissi_categories_data_urls.json")
    save_json(cats_movs, "odissi_categories_movements.json")
    print(f"Generated {len(cats)} categories, {len(cats_data)} data entries, {len(cats_data_urls)} data URLs, and {len(cats_movs)} movements.")

if __name__ == "__main__":
    main()
