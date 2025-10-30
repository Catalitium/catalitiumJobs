import json
import pandas as pd

# === Load the JSON file ===
with open("jobs.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# === Extract jobs from structure ===
jobs = data.get("generated_jobs", [])

# === Flatten each job into a single dictionary row ===
rows = []
for item in jobs:
    meta = item.get("metadata", {})
    post = item.get("job_post", {})

    row = {
        "job_title": meta.get("job_title"),
        "location": meta.get("location"),
        "industry": meta.get("industry"),
        "employment_type": meta.get("employment_type"),
        "tone": meta.get("tone"),
        "salary_min": meta.get("salary_range", {}).get("min"),
        "salary_max": meta.get("salary_range", {}).get("max"),
        "currency": meta.get("salary_range", {}).get("currency"),
        "keywords": ", ".join(meta.get("keywords", [])),
        "benefits": ", ".join(meta.get("benefits", [])),
        "summary": meta.get("summary"),
        "created_at": meta.get("created_at"),
        "status": meta.get("status"),
        "uuid": meta.get("uuid"),
    }
    rows.append(row)

# === Create a DataFrame ===
df = pd.DataFrame(rows)

# === Basic exploration ===
print("\n=== Sample of parsed data ===")
print(df.head())

print("\n=== Count of jobs by industry ===")
print(df["industry"].value_counts())

print("\n=== Count of jobs by location ===")
print(df["location"].value_counts())

print("\n=== Count of jobs by job title ===")
print(df["job_title"].value_counts())

# Optional: export to CSV for further analysis
df.to_csv("jobs_flat.csv", index=False, encoding="utf-8")
print("\n✅ Saved to jobs_flat.csv")
