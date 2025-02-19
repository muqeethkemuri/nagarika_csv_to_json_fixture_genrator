import csv
import json
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

def ensure_unique_slug(raw_slug, parent_pk, pk_to_slug, used_slugs):
    """
    Given a raw slug, checks if it is in used_slugs.
    If it is, prepend the first chunk of the parent's slug, then check again,
    repeating until we find a slug that isn't in used_slugs.
    
    :param raw_slug: The candidate slug (string)
    :param parent_pk: The parent's pk (or None if top-level)
    :param pk_to_slug: dict of { pk: slug } so we can look up parent's slug
    :param used_slugs: set of already-used slugs
    :return: A guaranteed-unique slug (string)
    """
    final_slug = raw_slug
    
    # If it's already used, and we do have a parent to help disambiguate:
    while final_slug in used_slugs and parent_pk is not None:
        parent_slug_full = pk_to_slug.get(parent_pk, "")
        parent_first_part = re.split(r'[-_]', parent_slug_full)[0] if parent_slug_full else "parent"
        final_slug = parent_first_part + "-" + final_slug
    
    return final_slug

def generate_categories(
    csv_file_path, 
    category_type, 
    slug_suffix="", 
    start_pk=4000,
    used_slugs=None,
    pk_to_slug=None
):
    """
    Reads a CSV with columns [level1, level2, level3, level4], builds hierarchical
    categories, and returns:
      (list_of_categories, next_pk, used_slugs, pk_to_slug).
    """
    
    timestamp = "2024-12-10T15:43:19.760"
    
    # If these are None, initialize them
    if used_slugs is None:
        used_slugs = set()
    if pk_to_slug is None:
        pk_to_slug = {}
    
    categories = []
    next_pk = start_pk
    
    # Keep track of children for has_data
    parent_map = {}
    
    # Track the current parent/grandparent pks
    current_parent_pk = None      # level1 PK
    current_child_pk = None       # level2 PK
    current_grandchild_pk = None  # level3 PK
    
    # Sort index for top-level categories
    sort_order_level1 = 1
    
    # Read the CSV
    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
      
        for row in reader:
            # Read in the levels, stripping extra whitespace
            print("DEBUG ROW:", row)
            level1 = row.get('level1', '').strip()
            level2 = row.get('level2', '').strip()
            level3 = row.get('level3', '').strip()
            level4 = row.get('level4', '').strip()
            
            # ------------------ LEVEL 1 ------------------
            if level1:
                raw_slug = slugify(level1) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, parent_pk=None, pk_to_slug=pk_to_slug, used_slugs=used_slugs)
                
                category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level1,
                        "slug": unique_slug,
                        "type": category_type,
                        "parent": None,
                        "has_data": 1,  # updated if it has children
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "category_type": "ODISSI",
                        "sort": sort_order_level1
                    }
                }
                categories.append(category)
                
                # Mark this slug as used, remember pk->slug
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug
                
                parent_map[next_pk] = []
                
                # Update references
                current_parent_pk = next_pk
                current_child_pk = None
                current_grandchild_pk = None
                
                next_pk += 1
                sort_order_level1 += 1
            
            # ------------------ LEVEL 2 ------------------
            if level2 and current_parent_pk is not None:
                raw_slug = slugify(level2) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, parent_pk=current_parent_pk, pk_to_slug=pk_to_slug, used_slugs=used_slugs)
                
                category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level2,
                        "slug": unique_slug,
                        "type": category_type,
                        "parent": current_parent_pk,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_parent_pk]) + 1
                    }
                }
                categories.append(category)
                
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug
                
                parent_map[current_parent_pk].append(next_pk)
                parent_map[next_pk] = []
                
                current_child_pk = next_pk
                current_grandchild_pk = None
                next_pk += 1
            
            # ------------------ LEVEL 3 ------------------
            if level3 and current_child_pk is not None:
                raw_slug = slugify(level3) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, parent_pk=current_child_pk, pk_to_slug=pk_to_slug, used_slugs=used_slugs)
                
                category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level3,
                        "slug": unique_slug,
                        "type": category_type,
                        "parent": current_child_pk,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_child_pk]) + 1
                    }
                }
                categories.append(category)
                
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug
                
                parent_map[current_child_pk].append(next_pk)
                parent_map[next_pk] = []
                
                current_grandchild_pk = next_pk
                next_pk += 1
            
            # ------------------ LEVEL 4 ------------------
            if level4 and current_grandchild_pk is not None:
                raw_slug = slugify(level4) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, parent_pk=current_grandchild_pk, pk_to_slug=pk_to_slug, used_slugs=used_slugs)
                
                category = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level4,
                        "slug": unique_slug,
                        "type": category_type,
                        "parent": current_grandchild_pk,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "sort": len(parent_map[current_grandchild_pk]) + 1
                    }
                }
                categories.append(category)
                
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug
                
                parent_map[current_grandchild_pk].append(next_pk)
                parent_map[next_pk] = []
                
                next_pk += 1
    
    # Final pass: If a category has children, has_data=0
    for cat in categories:
        cat_pk = cat["pk"]
        if cat_pk in parent_map and len(parent_map[cat_pk]) > 0:
            cat["fields"]["has_data"] = 0
    
    return categories, next_pk, used_slugs, pk_to_slug


if __name__ == "__main__":
    # Adjust as needed
    files_and_types = [
        ("input_csv/sequence_menu.csv", "SEQUENCE", ""),
        ("input_csv/unit_menu.csv", "UNIT", "-un"),
        #("input_csv/explanation_menu_sequence.csv", "EXPLANATION", "-un-exp"),
        #("input_csv/explanation_menu_unit.csv", "EXPLANATION", "-un-exp"),
        ("input_csv/explanation_menu_sequence+explanation_menu_unit.csv", "EXPLANATION", "-un-exp"),
        ("input_csv/context_menu.csv", "CONTEXT", "-un-dev")
    ]
    
    all_categories = []
    current_pk = 4000
    
    # SINGLE used_slugs set for all CSVs
    used_slugs = set()
    # pk -> slug mapping
    pk_to_slug = {}
    
    for csv_file, cat_type, slug_suffix in files_and_types:
        new_cats, current_pk, used_slugs, pk_to_slug = generate_categories(
            csv_file,
            cat_type,
            slug_suffix=slug_suffix,
            start_pk=current_pk,
            used_slugs=used_slugs,
            pk_to_slug=pk_to_slug
        )
        all_categories.extend(new_cats)
    
    output_file = "output.json"
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(all_categories, outfile, indent=2)
    
    print(f"JSON output has been saved to {output_file}")
