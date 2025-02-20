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
    video_prefix="",          # <-- NEW ARG, default empty
    start_pk=4000,
    used_slugs=None,
    pk_to_slug=None,
    # --- NEW ARGS FOR DATA FIXTURES ---
    data_pk_start=7000,
    current_data_pk=None,
    all_categories_data=None,
    all_categories_data_urls=None
):
    """
    Generates Category fixtures from CSV, plus also creates
    corresponding CategoriesData & CategoriesDataUrls fixtures.

    :param video_prefix: String to prepend before any 'front'/'side'/'movie' filename.
    """

    timestamp = "2024-12-10T15:43:19.760"
    
    if used_slugs is None:
        used_slugs = set()
    if pk_to_slug is None:
        pk_to_slug = {}

    # For the data fixtures
    if current_data_pk is None:
        current_data_pk = data_pk_start
    if all_categories_data is None:
        all_categories_data = []
    if all_categories_data_urls is None:
        all_categories_data_urls = []

    categories = []
    next_pk = start_pk
    
    # Keep track of children for has_data
    parent_map = {}
    
    # Track the current parent/grandparent pks
    current_parent_pk = None      # level1 PK
    current_child_pk = None       # level2 PK
    current_grandchild_pk = None  # level3 PK
    
    sort_order_level1 = 1

    with open(csv_file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            level1 = row.get('level1', '').strip()
            level2 = row.get('level2', '').strip()
            level3 = row.get('level3', '').strip()
            level4 = row.get('level4', '').strip()
            front_val = row.get('front', '').strip()
            side_val = row.get('side', '').strip()
            movie_val = row.get('movie', '').strip()  # only if your CSV has "movie"

            # ------------------ LEVEL 1 ------------------
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
                        "has_data": 1,  # will flip to 0 if it has children
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

                current_parent_pk = next_pk
                current_child_pk = None
                current_grandchild_pk = None

                next_pk += 1
                sort_order_level1 += 1
            
            # ------------------ LEVEL 2 ------------------
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
            
            # ------------------ LEVEL 3 ------------------
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
            
            # ------------------ LEVEL 4 ------------------
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
                # If level4 doesn't exist, but we have level3, then that is final
                # If no level3, maybe level2 is final, etc.
                if current_grandchild_pk:
                    final_category_pk_for_this_row = current_grandchild_pk
                elif current_child_pk:
                    final_category_pk_for_this_row = current_child_pk
                else:
                    final_category_pk_for_this_row = current_parent_pk

            # ------------------ CREATE CATEGORIES_DATA & DATA_URLS  ------------------

            def create_data_and_urls(title_str, direction_val, video_path, parent_cat_pk):
                nonlocal current_data_pk
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
                current_data_pk += 1

            # Decide a default "row_title" from whichever level you want
            row_title = level4 if level4 else (level3 if level3 else (level2 if level2 else level1))

            # If 'front' cell is not empty
            if front_val:
                # Prepend prefix if given
                prefixed_path = f"{video_prefix}/{front_val}" if video_prefix else front_val
                create_data_and_urls(
                    title_str=row_title,
                    direction_val="front",
                    video_path=prefixed_path,
                    parent_cat_pk=final_category_pk_for_this_row
                )

            # If 'side' cell is not empty
            if side_val:
                prefixed_path = f"{video_prefix}/{side_val}" if video_prefix else side_val
                create_data_and_urls(
                    title_str=row_title,
                    direction_val="side",
                    video_path=prefixed_path,
                    parent_cat_pk=final_category_pk_for_this_row
                )

            # If 'movie' cell is not empty
            if movie_val:
                prefixed_path = f"{video_prefix}/{movie_val}" if video_prefix else movie_val
                create_data_and_urls(
                    title_str=row_title,
                    direction_val="",  # or "movie" if you prefer
                    video_path=prefixed_path,
                    parent_cat_pk=final_category_pk_for_this_row
                )
            
            # Done processing this CSV row

    # Final pass: if a category has children, set "has_data"=0
    for cat in categories:
        cat_pk = cat["pk"]
        if cat_pk in parent_map and len(parent_map[cat_pk]) > 0:
            cat["fields"]["has_data"] = 0

    return categories, next_pk, used_slugs, pk_to_slug, current_data_pk

if __name__ == "__main__":
    files_and_types = [
        # Each tuple now has 4 items: CSV, category_type, slug_suffix, and video_prefix
        ("input_csv_v2/sequence_menu.csv", "SEQUENCE", "", "odissi/sequence"),
        ("input_csv_v2/unit_menu.csv", "UNIT", "-un", "odissi/unit"),
        ("input_csv_v2/explanation_menu_sequence+explanation_menu_unit.csv", "EXPLANATION", "-un-exp", "odissi/explanation"),
        ("input_csv_v2/context_menu.csv", "CONTEXT", "-un-dev", "odissi/context")
    ]

    all_categories = []
    current_pk = 4000

    used_slugs = set()
    pk_to_slug = {}

    # Lists for the new data fixtures
    all_categories_data = []
    all_categories_data_urls = []

    # Start the new data PK from 7000
    current_data_pk = 7000

    for csv_file, cat_type, slug_suffix, video_prefix in files_and_types:
        new_cats, current_pk, used_slugs, pk_to_slug, current_data_pk = generate_categories(
            csv_file,
            cat_type,
            slug_suffix=slug_suffix,
            video_prefix=video_prefix,   # <-- pass the prefix here
            start_pk=current_pk,
            used_slugs=used_slugs,
            pk_to_slug=pk_to_slug,
            data_pk_start=7000,
            current_data_pk=current_data_pk,
            all_categories_data=all_categories_data,
            all_categories_data_urls=all_categories_data_urls
        )
        all_categories.extend(new_cats)

    # Write out the Categories fixture
    output_file = "odissi_categories.json"
    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(all_categories, outfile, indent=2)
    print(f"JSON output has been saved to {output_file}")

    # Write out the two new fixture files
    data_output_file = "odissi_categories_data.json"
    with open(data_output_file, "w", encoding="utf-8") as outfile:
        json.dump(all_categories_data, outfile, indent=2)
    print(f"JSON output has been saved to {data_output_file}")

    data_urls_output_file = "odissi_categories_data_urls.json"
    with open(data_urls_output_file, "w", encoding="utf-8") as outfile:
        json.dump(all_categories_data_urls, outfile, indent=2)
    print(f"JSON output has been saved to {data_urls_output_file}")
