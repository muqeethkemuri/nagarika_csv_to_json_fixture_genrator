import pandas as pd
import json
from datetime import datetime
from slugify import slugify
from typing import Dict, List, Optional, Tuple

TYPE_MAP = {
    "context_menu": "CONTEXT",
    "sequence_menu": "SEQUENCE",
    "unit_menu": "UNIT",
    "explanation_sequence": "EXPLANATION",
    "explanation_unit": "EXPLANATION"
}

class CategoryConverter:
    def __init__(self):
        self.reset_pk_counters()
        self.timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        self.pk_map: Dict[str, int] = {}
        self.explanation_slug_map: Dict[str, Dict[str, str]] = {}

    def reset_pk_counters(self):
        self.pk_counter_categories = 4000
        self.pk_counter_data = 7000
        self.pk_counter_data_urls = 7000
        self.pk_counter_movements = 3000

    def load_explanation_slugs(self, explanation_files: Dict[str, str]) -> None:
        for ftype, csv_file in explanation_files.items():
            try:
                df = pd.read_csv(csv_file)
            except Exception as e:
                print(f"Error reading explanation file '{csv_file}': {e}")
                continue
            level_cols = self.get_level_columns(df)
            slug_dict = {}
            for _, row in df.iterrows():
                key = "|".join(str(row[col]).strip() if pd.notna(row[col]) else "" for col in level_cols)
                slug_value = row.get(level_cols[-1])
                if pd.notna(slug_value):
                    slug_dict[key] = slugify(str(slug_value))
            self.explanation_slug_map[ftype] = slug_dict

    def create_category(self, name: str, parent_pk: Optional[int], has_data: int, file_type: str, sort: int) -> dict:
        fields = {
            "name": str(name),
            "slug": slugify(str(name)),
            "type": TYPE_MAP.get(file_type, "CONTEXT"),
            "parent": parent_pk,
            "has_data": has_data,
            "is_active": 1,
            "created_at": self.timestamp,
            "updated_at": self.timestamp,
            "sort": sort
        }
        if parent_pk is None:
            fields["category_type"] = "ODISSI"
        category = {
            "model": "kalari.Categories",
            "pk": self.pk_counter_categories,
            "fields": fields
        }
        self.pk_counter_categories += 1
        return category

    def create_category_data(self, title: str, category_id: int, direction: str = '') -> dict:
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
        # Fix typo: replace 'ct.604' with 'ct_604'
        path = f"{prefix}{filename.replace('ct.604', 'ct_604')}"
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

    def create_category_movement(self, category_data_id: int, name: str, movement_type: str,
                                 related_explanation_slug: Optional[str] = None,
                                 start_time: Optional[int] = None,
                                 end_time: Optional[int] = None,
                                 is_related_only: bool = False) -> dict:
        fields = {
            "name": name,
            "category_data_movements_id": category_data_id,
            "type": movement_type,
            "is_related_only": is_related_only
        }
        if related_explanation_slug is not None:
            fields["related_explanation_slug"] = related_explanation_slug
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
        return sorted([col for col in df.columns if "level" in col.lower()])

    def process_row(self, row: pd.Series, level_columns: List[str], file_type: str,
                    sort_counters: Dict[str, int],
                    categories: List[dict], categories_data: List[dict],
                    categories_data_urls: List[dict], categories_movements: List[dict]) -> None:
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
                sort_key = full_key
                if sort_key not in sort_counters:
                    sort_counters[sort_key] = 1
                if full_key not in self.pk_map:
                    has_data = 1 if i == leaf_index else 0
                    parent_pk = None
                    if i > 0:
                        parent_key = "|".join(current_path[:i])
                        parent_pk = self.pk_map.get(parent_key)
                    cat = self.create_category(row[col], parent_pk, has_data, file_type, sort_counters[sort_key])
                    self.pk_map[full_key] = cat["pk"]
                    categories.append(cat)
                    sort_counters[sort_key] += 1
                last_cat_pk = self.pk_map[full_key]
        leaf_title = current_path[leaf_index]

        # Instead of one data record, create separate records for front and side (if available)
        data_records = []
        front_file = None
        if "front" in row and pd.notna(row["front"]):
            front_file = str(row["front"]).strip()
        elif "movie file name" in row and pd.notna(row["movie file name"]):
            front_file = str(row["movie file name"]).strip()
        if front_file:
            data_front = self.create_category_data(leaf_title, last_cat_pk, "front")
            categories_data.append(data_front)
            url_rec = self.create_category_data_url(data_front["pk"], front_file, file_type)
            categories_data_urls.append(url_rec)
            data_records.append(data_front)
        side_file = None
        if "side" in row and pd.notna(row["side"]):
            side_file = str(row["side"]).strip()
        if side_file:
            data_side = self.create_category_data(leaf_title, last_cat_pk, "side")
            categories_data.append(data_side)
            url_rec = self.create_category_data_url(data_side["pk"], side_file, file_type)
            categories_data_urls.append(url_rec)
            data_records.append(data_side)
        if not data_records:
            default_data = self.create_category_data(leaf_title, last_cat_pk, "")
            categories_data.append(default_data)
            data_records.append(default_data)

        st = row.get("start_time")
        et = row.get("end_time")
        has_timing = pd.notna(st) and pd.notna(et)

        leaf_key = "|".join(current_path[:leaf_index+1])
        lookup_type = None
        leaf_slug = None
        if file_type in ["sequence_menu", "unit_menu"]:
            lookup_type = "explanation_sequence" if file_type == "sequence_menu" else "explanation_unit"
            leaf_slug = self.explanation_slug_map.get(lookup_type, {}).get(leaf_key)
        elif file_type in ["explanation_sequence", "explanation_unit"]:
            leaf_slug = self.explanation_slug_map.get(file_type, {}).get(leaf_key)

        # Create movement records for each data record (front and/or side)
        for data_rec in data_records:
            if has_timing:
                parent_name = current_path[leaf_index - 1] if leaf_index > 0 else leaf_title
                overview_name = f"{parent_name} Overview"
                parent_key = "|".join(current_path[:leaf_index]) if leaf_index > 0 else ""
                parent_slug = self.explanation_slug_map.get(lookup_type, {}).get(parent_key) if lookup_type else None
                mov_overview = self.create_category_movement(data_rec["pk"], overview_name, "EXPLANATION",
                                                             related_explanation_slug=parent_slug,
                                                             is_related_only=True)
                categories_movements.append(mov_overview)
                mov_detail = self.create_category_movement(data_rec["pk"], leaf_title, "EXPLANATION",
                                                           related_explanation_slug=leaf_slug,
                                                           start_time=st, end_time=et,
                                                           is_related_only=False)
                categories_movements.append(mov_detail)
            else:
                mov = self.create_category_movement(data_rec["pk"], leaf_title, "EXPLANATION", related_explanation_slug=leaf_slug)
                categories_movements.append(mov)

    def generate_categories_json(self, files_and_types: List[Tuple[str, str]],
                                 explanation_files: Dict[str, str]) -> Tuple[List[dict], List[dict], List[dict], List[dict]]:
        self.load_explanation_slugs(explanation_files)
        all_categories: List[dict] = []
        all_categories_data: List[dict] = []
        all_categories_data_urls: List[dict] = []
        all_categories_movements: List[dict] = []
        self.reset_pk_counters()
        self.pk_map = {}
        sort_counters: Dict[str, int] = {}
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
            for _, row in df.iterrows():
                self.process_row(row, level_columns, file_type, sort_counters,
                                 all_categories, all_categories_data,
                                 all_categories_data_urls, all_categories_movements)
        return all_categories, all_categories_data, all_categories_data_urls, all_categories_movements

def save_json(data: List[dict], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
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
    # Check if all files exist
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