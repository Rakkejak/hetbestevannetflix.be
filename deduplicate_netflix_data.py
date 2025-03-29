import json

INPUT_FILE = "netflix_data.json"

def deduplicate(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    seen = set()
    unique_data = []
    for item in data:
        key = (item["title"].strip(), item["type"], item["releaseDate"])
        if key not in seen:
            seen.add(key)
            unique_data.append(item)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(unique_data, f, indent=2, ensure_ascii=False)

    print(f"Deduplicated {filepath}: {len(data)} → {len(unique_data)}")

if __name__ == "__main__":
    deduplicate(INPUT_FILE)
