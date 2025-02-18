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

def generate_categories(csv_file_path, category_type, start_pk=4000):
    """
    Reads a CSV file with columns [level1, level2, level3], creates a
    hierarchical structure of categories, and returns a tuple:
    (list_of_categories, next_pk_value).
    
    :param csv_file_path: Path to the CSV file
    :param category_type: The value for the "type" field in each category
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
    
    # We'll keep track of the current "level1" parent PK and "level2" child PK
    current_parent_pk = None
    current_child_pk = None
    
    # Sort order for top-level categories
    # (You can also do a single global sort order for everything if desired.)
    sort_order_level1 = 1
    
    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Read in the levels, stripping extra whitespace
            level1 = row.get('level1', '').strip()
            level2 = row.get('level2', '').strip()
            level3 = row.get('level3', '').strip()
            
            # If this row has a new level1, create a new top-level category
            if level1:
                parent_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level1,
                        "slug": slugify(level1),
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
                
                # Update current references
                current_parent_pk = next_pk
                current_child_pk = None  # reset child pk
                next_pk += 1
                sort_order_level1 += 1
            
            # If we have level2, create or update the child category
            if level2:
                child_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level2,
                        "slug": slugify(level2),
                        "type": category_type,
                        "parent": current_parent_pk,
                        "has_data": 1,  # will be updated to 0 if it has children
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        # 'category_type' was only in the parent in your original code,
                        # but if you also want it in children, just uncomment below:
                        # "category_type": "ODISSI",
                        # The "sort" of child categories is based on how many children
                        # the parent already has + 1
                        "sort": len(parent_map[current_parent_pk]) + 1
                    }
                }
                categories.append(child_category)
                
                # Update parent_map
                parent_map[current_parent_pk].append(next_pk)
                parent_map[next_pk] = []  # this child may have its own children (level3)
                
                # Update references
                current_child_pk = next_pk
                next_pk += 1
                
            # If we have level3, create a grandchild category under current_child_pk
            if level3 and current_child_pk is not None:
                grandchild_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level3,
                        "slug": slugify(level3),
                        "type": category_type,
                        "parent": current_child_pk,
                        "has_data": 1,  # no further levels, so presumably 1 unless you have level4
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        # "category_type": "ODISSI",  # if needed
                        # sort is the number of children the child has + 1
                        "sort": len(parent_map[current_child_pk]) + 1
                    }
                }
                categories.append(grandchild_category)
                
                parent_map[current_child_pk].append(next_pk)
                parent_map[next_pk] = []
                
                next_pk += 1
                
            # If level1 was empty but we have an existing current_parent_pk,
            # the original script tried to handle single-level categories.
            # But with the new logic, you typically won't need this else-block,
            # because each row that has no level1 won't create a parent at all.
            # If needed, adapt from your original logic.
    
    # Now update has_data for any parent that has children
    for cat in categories:
        cat_pk = cat["pk"]
        # If this pk has children in parent_map, set has_data to 0
        if cat_pk in parent_map:
            if parent_map[cat_pk]:  # has non-empty list of children
                cat["fields"]["has_data"] = 0
    
    return categories, next_pk


if __name__ == "__main__":
    # List of (csv_file_path, type_string) in the order you want to process them
    files_and_types = [
        ("input_csv/sequence_menu.csv","SEQUENCE"),
        ("input_csv/unit_menu.csv", "UNIT"),
        ("input_csv/explanation_unit.csv", "EXPLANATION"),
        ("input_csv/context_menu.csv", "CONTEXT")
        # Add more (csv_file_path, type_string) tuples as needed
    ]
    
    all_categories = []
    current_pk = 4000  # start pk
    
    # Process each CSV in order
    for csv_file, cat_type in files_and_types:
        new_cats, current_pk = generate_categories(csv_file, cat_type, start_pk=current_pk)
        all_categories.extend(new_cats)
    
    # Write the output JSON
    output_file = "output.json"
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(all_categories, outfile, indent=2)
    
    print(f"JSON output has been saved to {output_file}")
