import csv
import json
import datetime
import re

def slugify(text):
    """Convert a string into a slug: lowercase, hyphens instead of spaces, etc."""
    return (
        text.lower()
            .strip()
            .replace(' ', '-')
            .replace('(', '')
            .replace(')', '')
            .replace('/', '-')
    )

def generate_categories(csv_file_path, category_type, slug_suffix="", start_pk=4000):
    """
    Reads a CSV file with columns [level1, level2, level3, level4], creates a
    hierarchical structure of categories, and returns a tuple:
    (list_of_categories, next_pk_value).
    
    :param csv_file_path: Path to the CSV file
    :param category_type: The value for the "type" field in each category
    :param slug_suffix: Suffix to append to each slug (e.g. "-SN", "-UN")
    :param start_pk: The starting primary key (default=4000)
    :return: (categories, next_pk)
    """
    
    timestamp = "2024-12-10T15:43:19.760"
    
    categories = []
    next_pk = start_pk
    
    # Keep track of parent->children
    parent_map = {}
    
    # Track our current PK references for levels
    current_parent_pk = None       # For level1
    current_child_pk = None        # For level2
    current_grandchild_pk = None   # For level3
    
    # Sort order for top-level categories
    sort_order_level1 = 1

    # -----------------------------
    # NEW: Track used slugs at each level
    used_level2_slugs = set()
    used_level3_slugs = set()
    used_level4_slugs = set()
    
    # Also keep a map of pk -> slug (so we can find the parent's slug easily)
    pk_to_slug = {}
    # -----------------------------
    
    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # Read in the levels, stripping extra whitespace
        print("DEBUG ROW:", row)
        for row in reader:
            level1 = row.get('level1', '').strip()
            level2 = row.get('level2', '').strip()
            level3 = row.get('level3', '').strip()
            level4 = row.get('level4', '').strip()
            
           # ------------- LEVEL 1 -------------
        # If this row has a new level1, create a new top-level category
            if level1:
                parent_slug = slugify(level1) + slug_suffix
                parent_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level1,
                        "slug": parent_slug,
                        "type": category_type,
                        "parent": None,
                        "has_data": 1,  # updated later if it has children
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "category_type": "ODISSI",
                        "sort": sort_order_level1
                    }
                }
                categories.append(parent_category)
                pk_to_slug[next_pk] = parent_slug  # Store slug
                parent_map[next_pk] = []
                
                current_parent_pk = next_pk
                current_child_pk = None
                current_grandchild_pk = None
                
                next_pk += 1
                sort_order_level1 += 1
            
            # ------------------ LEVEL 2 ------------------
            if level2 and current_parent_pk is not None:
                raw_child_slug = slugify(level2) + slug_suffix
                # Check if this slug is used at level2
                if raw_child_slug in used_level2_slugs:
                    # Prepend parent's first chunk to make it unique
                    parent_slug_full = pk_to_slug[current_parent_pk]
                    parent_first_part = re.split(r'[-_]', parent_slug_full)[0]
                    child_slug = parent_first_part + "-" + raw_child_slug
                else:
                    child_slug = raw_child_slug

                used_level2_slugs.add(child_slug)
                
                child_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level2,
                        "slug": child_slug,
                        "type": category_type,
                        "parent": current_parent_pk,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_parent_pk]) + 1
                    }
                }
                categories.append(child_category)
                pk_to_slug[next_pk] = child_slug  # Store slug for level2
                
                parent_map[current_parent_pk].append(next_pk)
                parent_map[next_pk] = []
                
                current_child_pk = next_pk
                current_grandchild_pk = None
                next_pk += 1
            
            # ------------------ LEVEL 3 ------------------
            if level3 and current_child_pk is not None:
                raw_grandchild_slug = slugify(level3) + slug_suffix
                # Check if this slug is used at level3
                if raw_grandchild_slug in used_level3_slugs:
                    parent_slug_full = pk_to_slug[current_child_pk]
                    parent_first_part = re.split(r'[-_]', parent_slug_full)[0]
                    grandchild_slug = parent_first_part + "-" + raw_grandchild_slug
                else:
                    grandchild_slug = raw_grandchild_slug

                used_level3_slugs.add(grandchild_slug)
                
                grandchild_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level3,
                        "slug": grandchild_slug,
                        "type": category_type,
                        "parent": current_child_pk,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_child_pk]) + 1
                    }
                }
                categories.append(grandchild_category)
                pk_to_slug[next_pk] = grandchild_slug  # Store slug for level3
                
                parent_map[current_child_pk].append(next_pk)
                parent_map[next_pk] = []
                
                current_grandchild_pk = next_pk
                next_pk += 1
            
            # ------------------ LEVEL 4 ------------------
            if level4 and current_grandchild_pk is not None:
                raw_fourth_slug = slugify(level4) + slug_suffix
                # Check if this slug is used at level4
                if raw_fourth_slug in used_level4_slugs:
                    parent_slug_full = pk_to_slug[current_grandchild_pk]
                    parent_first_part = re.split(r'[-_]', parent_slug_full)[0]
                    fourth_slug = parent_first_part + "-" + raw_fourth_slug
                else:
                    fourth_slug = raw_fourth_slug
                
                used_level4_slugs.add(fourth_slug)
                
                fourth_level_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level4,
                        "slug": fourth_slug,
                        "type": category_type,
                        "parent": current_grandchild_pk,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_grandchild_pk]) + 1
                    }
                }
                categories.append(fourth_level_category)
                pk_to_slug[next_pk] = fourth_slug  # Store slug for level4
                
                parent_map[current_grandchild_pk].append(next_pk)
                parent_map[next_pk] = []
                
                next_pk += 1
    
    # Update has_data = 0 for categories having children
    for cat in categories:
        cat_pk = cat["pk"]
        if cat_pk in parent_map and parent_map[cat_pk]:
            cat["fields"]["has_data"] = 0
    
    return categories, next_pk


if __name__ == "__main__":
    
    # Now each tuple includes (csv_file_path, type_string, slug_suffix)
    files_and_types = [
        ("input_csv/sequence_menu.csv", "SEQUENCE", ""),
        ("input_csv/unit_menu.csv", "UNIT", "-un"),
        ("input_csv/explanation_menu_sequence.csv", "EXPLANATION", "-un-exp"),
        ("input_csv/explanation_menu_unit.csv", "EXPLANATION", "-un-exp"),
        ("input_csv/context_menu.csv", "CONTEXT", "-un-dev")
    ]
    
    all_categories = []
    current_pk = 4000
    
    # Process each CSV in the specified order
    for csv_file, cat_type, slug_suffix in files_and_types:
        new_cats, current_pk = generate_categories(
            csv_file,
            cat_type,
            slug_suffix=slug_suffix,
            start_pk=current_pk
        )
        all_categories.extend(new_cats)

    # Write the output JSON
    output_file = "output.json"
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(all_categories, outfile, indent=2)
    
    print(f"JSON output has been saved to {output_file}")
