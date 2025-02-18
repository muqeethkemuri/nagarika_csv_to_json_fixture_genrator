import csv
import json
import datetime

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
    
    # Fixed timestamp for all entries
    timestamp = "2024-12-10T15:43:19.760"
    
    categories = []
    next_pk = start_pk
    
    # A map of pk -> list of child_pks
    # This helps us identify which categories have children.
    parent_map = {}
    
    # We'll keep track of the current "level1" PK, "level2" PK, "level3" PK
    current_parent_pk = None   # for level1
    current_child_pk = None    # for level2
    current_grandchild_pk = None  # for level3
    
    # Sort order for top-level categories
    # (If you want a single global sort, you can adapt as needed.)
    sort_order_level1 = 1
    
    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Read in the levels, stripping extra whitespace
            print("DEBUG ROW:", row)
            level1 = row.get('level1', '').strip()
            level2 = row.get('level2', '').strip()
            level3 = row.get('level3', '').strip()
            level4 = row.get('level4', '').strip()  # NEW for level4
            
            # ------------- LEVEL 1 -------------
            # If this row has a new level1, create a new top-level category
            if level1:
                parent_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level1,
                        "slug": slugify(level1) + slug_suffix,
                        "type": category_type,           # <--- uses the passed-in category_type
                        "parent": None,
                        "has_data": 1,  # will be updated to 0 if it has children
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "category_type": "ODISSI",       # keeping the original usage
                        "sort": sort_order_level1
                    }
                }
                categories.append(parent_category)
                
                # Initialize the parent in parent_map
                parent_map[next_pk] = []
                
                # Update references
                current_parent_pk = next_pk
                current_child_pk = None
                current_grandchild_pk = None
                next_pk += 1
                sort_order_level1 += 1
            
            # ------------- LEVEL 2 -------------
            # If we have level2, create or update the child category
            if level2 and current_parent_pk is not None:
                child_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level2,
                        "slug": slugify(level2) + slug_suffix,
                        "type": category_type,
                        "parent": current_parent_pk,
                        "has_data": 1,  # will be updated to 0 if it has children
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        # "category_type": "ODISSI",  # if you also want it on children
                        # The "sort" of child categories is the count of parent's children + 1
                        "sort": len(parent_map[current_parent_pk]) + 1
                    }
                }
                categories.append(child_category)
                
                # Update parent_map
                parent_map[current_parent_pk].append(next_pk)
                parent_map[next_pk] = []  # this child may have its own children (level3, level4)
                
                # Update references
                current_child_pk = next_pk
                current_grandchild_pk = None
                next_pk += 1
            
            # ------------- LEVEL 3 -------------
            # If we have level3, create a grandchild category under current_child_pk
            if level3 and current_child_pk is not None:
                grandchild_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level3,
                        "slug": slugify(level3) + slug_suffix,
                        "type": category_type,
                        "parent": current_child_pk,
                        "has_data": 1,  # will be updated to 0 if it has children
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        # "category_type": "ODISSI",  # if needed
                        # sort = number of children the child has + 1
                        "sort": len(parent_map[current_child_pk]) + 1
                    }
                }
                categories.append(grandchild_category)
                
                parent_map[current_child_pk].append(next_pk)
                parent_map[next_pk] = []
                
                current_grandchild_pk = next_pk
                next_pk += 1
            
            # ------------- LEVEL 4 -------------
            # If we have level4, create a child under the level3 category
            if level4 and current_grandchild_pk is not None:
                fourth_level_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level4,
                        "slug": slugify(level4) + slug_suffix,
                        "type": category_type,
                        "parent": current_grandchild_pk,
                        "has_data": 1,  # presumably no further levels
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_grandchild_pk]) + 1
                    }
                }
                categories.append(fourth_level_category)
                
                parent_map[current_grandchild_pk].append(next_pk)
                parent_map[next_pk] = []
                
                next_pk += 1
            
            # (If you want logic for single-level rows with no level1,
            # you can add it here—most likely not needed if your CSV always has a level1.)
    
    # --- Update has_data for any category that has children ---
    for cat in categories:
        cat_pk = cat["pk"]
        # If this pk has children in parent_map, set has_data=0
        if cat_pk in parent_map:
            if parent_map[cat_pk]:  # if non-empty
                cat["fields"]["has_data"] = 0
    
    return categories, next_pk


if __name__ == "__main__":
    # Now each tuple includes (csv_file_path, type_string, slug_suffix)
    files_and_types = [
        ("input_csv/sequence_menu.csv","SEQUENCE", ""),
        ("input_csv/unit_menu.csv", "UNIT", "-un"),
        ("input_csv/explanation_menu_sequence.csv", "EXPLANATION", "-un-exp"),
        ("input_csv/explanation_menu_unit.csv", "EXPLANATION", "-un-exp"),
        ("input_csv/context_menu.csv", "CONTEXT", "-un-dev")
        # Add more tuples if needed
    ]
    
    all_categories = []
    current_pk = 4000  # start pk
    
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
