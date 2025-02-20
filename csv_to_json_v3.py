import csv
import json
import re

def slugify(text):
    """Convert a string into a slug: lowercase, hyphens instead of spaces, etc."""
    slug = (
        text.lower()
            .strip()
            .replace(' ', '-')
            .replace('(', '')
            .replace(')', '')
            .replace('/', '-')
    )
    slug = re.sub(r'-+', '-', slug)
    return slug

KNOWN_SUFFIXES = ('-un', '-exp', '-dev')
def remove_known_suffixes(slug_value):
    pattern = r'(?:' + '|'.join(KNOWN_SUFFIXES) + r')+$'
    return re.sub(pattern, '', slug_value)

def ensure_unique_slug(raw_slug, parent_pks, pk_to_slug, used_slugs):
    if raw_slug not in used_slugs:
        return raw_slug
    parent_chain = []
    for parent_pk in parent_pks:
        parent_full_slug = pk_to_slug.get(parent_pk, '')
        parent_base = remove_known_suffixes(parent_full_slug)
        if parent_base:
            parent_chain.insert(0, parent_base)
            candidate = "-".join(parent_chain + [raw_slug])
            if candidate not in used_slugs:
                return candidate
    raise ValueError(
        f"Slug collision: Could not generate a unique slug for '{raw_slug}' with "
        f"parent chain {[pk_to_slug.get(pk, '') for pk in parent_pks]}."
    )

def generate_categories(
    csv_file_path, 
    category_type, 
    slug_suffix="",
    video_prefix="",
    start_pk=4000,
    used_slugs=None,
    pk_to_slug=None,
    data_pk_start=7000,
    current_data_pk=None,
    all_categories_data=None,
    all_categories_data_urls=None,
    all_categories_movements=None,  # New arg for movements
    movements_pk_start=3000,        # New arg for movements PK
    current_movements_pk=None       # New arg to track movements PK
):
    timestamp = "2024-12-10T15:43:19.760"
    
    if used_slugs is None:
        used_slugs = set()
    if pk_to_slug is None:
        pk_to_slug = {}
    if current_data_pk is None:
        current_data_pk = data_pk_start
    if all_categories_data is None:
        all_categories_data = []
    if all_categories_data_urls is None:
        all_categories_data_urls = []
    if all_categories_movements is None:
        all_categories_movements = []
    if current_movements_pk is None:
        current_movements_pk = movements_pk_start

    categories = []
    next_pk = start_pk
    
    parent_map = {}
    current_parent_pk = None
    current_child_pk = None
    current_grandchild_pk = None
    
    sort_order_level1 = 1

    # Dictionary to store overview slugs for each level1 category
    overview_slugs = {}

    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            level1 = row.get('level1', '').strip()
            level2 = row.get('level2', '').strip()
            level3 = row.get('level3', '').strip()
            level4 = row.get('level4', '').strip()
            front_val = row.get('front', '').strip()
            side_val = row.get('side', '').strip()
            movie_val = row.get('movie', '').strip()

            # LEVEL 1
            if level1:
                raw_slug = slugify(level1) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, [], pk_to_slug, used_slugs)
                
                cat_obj = {
                    "model": "kalari.Categories",
                    "pk": next_pk,
                    "fields": {
                        "name": level1,
                        "slug": unique_slug,
                        "type": category_type,
                        "parent": None,
                        "has_data": 1,
                        "is_active": 1,
                        "created_at": timestamp,
                        "updated_at": timestamp,
                        "category_type": "ODISSI",
                        "sort": sort_order_level1
                    }
                }
                categories.append(cat_obj)
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug

                parent_map[next_pk] = []
                overview_slugs[level1] = f"{slugify(level1)}-overview{slug_suffix}"

                current_parent_pk = next_pk
                current_child_pk = None
                current_grandchild_pk = None

                next_pk += 1
                sort_order_level1 += 1
            
            # LEVEL 2
            if level2 and current_parent_pk is not None:
                raw_slug = slugify(level2) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, [current_parent_pk], pk_to_slug, used_slugs)
                
                cat_obj = {
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
                categories.append(cat_obj)
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug

                parent_map[current_parent_pk].append(next_pk)
                parent_map[next_pk] = []
                
                current_child_pk = next_pk
                current_grandchild_pk = None
                next_pk += 1
            
            # LEVEL 3
            if level3 and current_child_pk is not None:
                raw_slug = slugify(level3) + slug_suffix
                unique_slug = ensure_unique_slug(raw_slug, [current_child_pk, current_parent_pk], pk_to_slug, used_slugs)
                
                cat_obj = {
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
                categories.append(cat_obj)
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug

                parent_map[current_child_pk].append(next_pk)
                parent_map[next_pk] = []

                current_grandchild_pk = next_pk
                next_pk += 1
            
            # LEVEL 4
            if level4 and current_grandchild_pk is not None:
                raw_slug = slugify(level4) + slug_suffix
                unique_slug = ensure_unique_slug(
                    raw_slug, 
                    [current_grandchild_pk, current_child_pk, current_parent_pk], 
                    pk_to_slug, used_slugs
                )
                
                cat_obj = {
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
                categories.append(cat_obj)
                used_slugs.add(unique_slug)
                pk_to_slug[next_pk] = unique_slug

                parent_map[current_grandchild_pk].append(next_pk)
                parent_map[next_pk] = []

                final_category_pk_for_this_row = next_pk
                next_pk += 1
            else:
                if current_grandchild_pk:
                    final_category_pk_for_this_row = current_grandchild_pk
                elif current_child_pk:
                    final_category_pk_for_this_row = current_child_pk
                else:
                    final_category_pk_for_this_row = current_parent_pk

            # CREATE CATEGORIES_DATA & DATA_URLS
            def create_data_and_urls(title_str, direction_val, video_path, parent_cat_pk):
                nonlocal current_data_pk, current_movements_pk
                data_item = {
                    "model": "kalari.CategoriesData",
                    "pk": current_data_pk,
                    "fields": {
                        "title": title_str,
                        "category_id": parent_cat_pk,
                        "direction": direction_val,
                        "created_at": timestamp,
                        "updated_at": timestamp
                    }
                }
                all_categories_data.append(data_item)

                urls_item = {
                    "model": "kalari.CategoriesDataUrls",
                    "pk": current_data_pk,
                    "fields": {
                        "path": video_path,
                        "type": "video",
                        "category_data_id": current_data_pk
                    }
                }
                all_categories_data_urls.append(urls_item)

                # CREATE CATEGORIES_MOVEMENTS
                row_title = level4 if level4 else (level3 if level3 else (level2 if level2 else level1))
                slug_base = slugify(row_title)
                related_slug = f"{slug_base}{slug_suffix}"
                
                # Add overview entry if applicable
                overview_added = False
                if level1 and level1 in overview_slugs:
                    overview_movement = {
                        "model": "kalari.CategoriesMovements",
                        "pk": current_movements_pk,
                        "fields": {
                            "name": f"{level1} Overview",
                            "category_data_movements_id": current_data_pk,
                            "related_explanation_slug": overview_slugs[level1],
                            "type": "EXPLANATION",
                            "is_related_only": True
                        }
                    }
                    all_categories_movements.append(overview_movement)
                    current_movements_pk += 1
                    overview_added = True

                # Add movement entry
                movement = {
                    "model": "kalari.CategoriesMovements",
                    "pk": current_movements_pk,
                    "fields": {
                        "name": row_title,
                        "category_data_movements_id": current_data_pk,
                        "start_time": 0,
                        "end_time": 2000,
                        "related_explanation_slug": related_slug,
                        "type": category_type
                    }
                }
                all_categories_movements.append(movement)
                current_movements_pk += 1

                current_data_pk += 1

            row_title = level4 if level4 else (level3 if level3 else (level2 if level2 else level1))

            if front_val:
                prefixed_path = f"{video_prefix}/{front_val}" if video_prefix else front_val
                create_data_and_urls(row_title, "front", prefixed_path, final_category_pk_for_this_row)

            if side_val:
                prefixed_path = f"{video_prefix}/{side_val}" if video_prefix else side_val
                create_data_and_urls(row_title, "side", prefixed_path, final_category_pk_for_this_row)

            if movie_val:
                prefixed_path = f"{video_prefix}/{movie_val}" if video_prefix else movie_val
                create_data_and_urls(row_title, "", prefixed_path, final_category_pk_for_this_row)

    # Final pass for has_data
    for cat in categories:
        cat_pk = cat["pk"]
        if cat_pk in parent_map and len(parent_map[cat_pk]) > 0:
            cat["fields"]["has_data"] = 0

    return (
        categories, next_pk, used_slugs, pk_to_slug, current_data_pk,
        all_categories_movements, current_movements_pk
    )

if __name__ == "__main__":
    files_and_types = [
        ("input_csv_v2/sequence_menu.csv", "SEQUENCE", "", "odissi/sequence"),
        ("input_csv_v2/unit_menu.csv", "UNIT", "-un", "odissi/unit"),
        ("input_csv_v2/explanation_menu_sequence+explanation_menu_unit.csv", "EXPLANATION", "-un-exp", "odissi/explanation"),
        ("input_csv_v2/context_menu.csv", "CONTEXT", "-un-dev", "odissi/context")
    ]

    all_categories = []
    current_pk = 4000
    used_slugs = set()
    pk_to_slug = {}
    all_categories_data = []
    all_categories_data_urls = []
    all_categories_movements = []
    current_data_pk = 7000
    current_movements_pk = 3000

    for csv_file, cat_type, slug_suffix, video_prefix in files_and_types:
        (
            new_cats, current_pk, used_slugs, pk_to_slug, current_data_pk,
            new_movements, current_movements_pk
        ) = generate_categories(
            csv_file,
            cat_type,
            slug_suffix=slug_suffix,
            video_prefix=video_prefix,
            start_pk=current_pk,
            used_slugs=used_slugs,
            pk_to_slug=pk_to_slug,
            data_pk_start=7000,
            current_data_pk=current_data_pk,
            all_categories_data=all_categories_data,
            all_categories_data_urls=all_categories_data_urls,
            all_categories_movements=all_categories_movements,
            movements_pk_start=3000,
            current_movements_pk=current_movements_pk
        )
        all_categories.extend(new_cats)
        all_categories_movements.extend(new_movements)

    # Write Categories fixture
    with open("odissi_categories.json", "w", encoding="utf-8") as outfile:
        json.dump(all_categories, outfile, indent=2)
    print("JSON output has been saved to odissi_categories.json")

    # Write CategoriesData fixture
    with open("odissi_categories_data.json", "w", encoding="utf-8") as outfile:
        json.dump(all_categories_data, outfile, indent=2)
    print("JSON output has been saved to odissi_categories_data.json")

    # Write CategoriesDataUrls fixture
    with open("odissi_categories_data_urls.json", "w", encoding="utf-8") as outfile:
        json.dump(all_categories_data_urls, outfile, indent=2)
    print("JSON output has been saved to odissi_categories_data_urls.json")

    # Write CategoriesMovements fixture
    with open("odissi_categories_movements.json", "w", encoding="utf-8") as outfile:
        json.dump(all_categories_movements, outfile, indent=2)
    print("JSON output has been saved to odissi_categories_movements.json")