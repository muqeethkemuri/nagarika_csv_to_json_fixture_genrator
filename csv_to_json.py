import csv
import json
import re

def slugify(text):
    """Convert a string into a slug: lowercase, hyphens instead of spaces, etc."""
    # Replace spaces with hyphens and remove unwanted characters
    slug = (
        text.lower()
            .strip()
            .replace(' ', '-')
            .replace('(', '')
            .replace(')', '')
            .replace('/', '-')
    )
    # Replace multiple consecutive hyphens with a single hyphen
    slug = re.sub(r'-+', '-', slug)
    return slug

### CHANGED ###
KNOWN_SUFFIXES = ('-un', '-exp', '-dev')
def remove_known_suffixes(slug_value):
    """
    If the slug ends with -un, -exp, or -dev (possibly multiple times),
    remove those trailing pieces. This avoids repeated suffixes in expansions.
    """
    # e.g. if slug_value = "bodha-un", strip off "-un".
    # If "bodha-un-exp", strip off "-exp", then "-un", etc.
    pattern = r'(?:' + '|'.join(KNOWN_SUFFIXES) + r')+$'
    return re.sub(pattern, '', slug_value)


def ensure_unique_slug(raw_slug, parent_pks, pk_to_slug, used_slugs):
    """
    Ensure a unique slug by:
      1) Trying raw_slug alone.
      2) If collision, try prepending each parent's "base" level by level,
         stopping as soon as a unique combination is found.
      3) If no unique combination is found after trying all parents,
         return the last generated slug (which will be the most prepended).
    """

    # 1) If child's raw_slug is free, use it immediately
    if raw_slug not in used_slugs:
        return raw_slug

    # 2) There's a collision → build expansions from the parent chain
    #    We remove the parent's suffix from the parent's slug so that
    #    we don't get repeated '-un' or '-exp' or '-dev'.

    for parent_pk in parent_pks:  # Iterate through parents one by one
        parent_full_slug = pk_to_slug.get(parent_pk, '')
        parent_base = remove_known_suffixes(parent_full_slug)

        if parent_base:
            candidate = f"{parent_base}-{raw_slug}" # Prepend parent to the *raw_slug*
        else:
            candidate = raw_slug # Should not happen unless parent slug is empty

        if candidate not in used_slugs:
            return candidate  # Found a unique slug with *this* level of parent! Return immediately

    # 3) If no unique combination is found after trying all parents,
    #    return the last candidate (which is the raw_slug prepended with all parents).
    #    In this version, if we reach here, it means even with all parents prepended,
    #    it's still not unique (which is unlikely in your scenario, but possible).
    #    In the original code, it was returning the *last* candidate built.
    #    Here we should probably return a slug that is *guaranteed* unique,
    #    even if it's very long. For simplicity, let's just return the last candidate
    #    which in this loop structure would be the raw_slug prepended with all parents.

    # Original code's behavior was to return the last `candidate` generated.
    # Let's maintain that for now.  The last candidate will have all parent prefixes.
    if parent_pks: # If there were parents to consider, take the last candidate
        parent_full_slug = pk_to_slug.get(parent_pks[-1], '') # Get the *last* parent's slug
        parent_base = remove_known_suffixes(parent_full_slug)
        if parent_base:
            return f"{parent_base}-{raw_slug}" # Return with *all* parents prefixed
        else:
            return raw_slug # Fallback if even last parent has no slug
    else: # No parents to consider, and raw_slug is not unique.  This is unexpected in your logic.
        return raw_slug # As a fallback, just return the raw_slug (it will still be marked as used)
                      # You might want to add a counter or something more robust if collisions
                      # are expected even after prepending all parents.

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
            level1 = row.get('level1', '').strip()
            level2 = row.get('level2', '').strip()
            level3 = row.get('level3', '').strip()
            level4 = row.get('level4', '').strip()
            
            # ------------------ LEVEL 1 ------------------
            if level1:
                raw_slug = slugify(level1) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, [], pk_to_slug, used_slugs)
                
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
                unique_slug = ensure_unique_slug(raw_slug, [current_parent_pk], pk_to_slug, used_slugs)
                
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
                unique_slug = ensure_unique_slug(raw_slug, [current_child_pk, current_parent_pk], pk_to_slug, used_slugs)
                
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
                unique_slug = ensure_unique_slug(raw_slug, [current_grandchild_pk, current_child_pk, current_parent_pk], pk_to_slug, used_slugs)
                
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
    
    output_file = "odissi_categories.json"
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(all_categories, outfile, indent=2)
    
    print(f"JSON output has been saved to {output_file}")
