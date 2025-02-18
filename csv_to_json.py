import csv
import json
import datetime

def slugify(text):
    """Convert a string into a slug: lowercase, hyphens instead of spaces, etc."""
    return text.lower().strip().replace(' ', '-').replace('(', '').replace(')', '').replace('/', '-')

def generate_categories(csv_file_path):
    # Fixed timestamp for all entries
    timestamp = "2024-12-10T15:43:19.760"
    
    # Start with sequence categories (pk starts at 4000)
    categories = []
    next_pk = 4000
    
    # Track parent-child relationships
    parent_map = {}
    
    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        current_level1 = None
        sort_order = 1
        
        for row in reader:
            print("DEBUG -> row read:", row)
            level1 = row['level1'].strip()
            level2 = row['level2'].strip()
            
            # Update current_level1 if level1 is not empty
            if level1:
                current_level1 = level1
                # Create parent category
                parent_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": current_level1,
                        "slug": slugify(current_level1),
                        "type": "SEQUENCE",
                        "parent": None,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "category_type": "ODISSI",
                        "sort": sort_order
                    }
                }
                categories.append(parent_category)
                parent_map[next_pk] = []
                current_parent_pk = next_pk
                next_pk += 1
                sort_order += 1
            
            # Create child category if level2 exists
            if level2:
                child_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level2,
                        "slug": slugify(level2),
                        "type": "SEQUENCE",
                        "parent": current_parent_pk,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_parent_pk]) + 1
                    }
                }
                categories.append(child_category)
                parent_map[current_parent_pk].append(next_pk)
                next_pk += 1
            elif not level1:  # If level1 is empty but we have a current_level1
                # This is a single-level category
                single_category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": current_level1,
                        "slug": slugify(current_level1),
                        "type": "SEQUENCE",
                        "parent": None,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "category_type": "ODISSI",
                        "sort": sort_order
                    }
                }
                categories.append(single_category)
                next_pk += 1
                sort_order += 1

    # Update has_data for parent categories
    for category in categories:
        if category["fields"]["parent"] is None and category["pk"] in parent_map:
            if parent_map[category["pk"]]:  # If the parent has children
                category["fields"]["has_data"] = 0
            else:
                category["fields"]["has_data"] = 1

    return categories

if __name__ == "__main__":
    csv_file = "input_csv/sequence_menu.csv"  # your CSV file
    data = generate_categories(csv_file)
    
    # Write the output JSON to a file
    output_file = "output.json"
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(data, outfile, indent=2)
    
    print(f"JSON output has been saved to {output_file}")