import csv
import random
import os
from datetime import datetime, timedelta

# Simulate clinical trial patient records
random.seed(42)

DRUGS = ["DrugA", "DrugB", "Placebo"]
SITES = ["Site_Mumbai", "Site_Delhi", "Site_Chennai", "Site_Bangalore"]
STATUSES = ["active", "completed", "withdrawn", "adverse_event"]

def random_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

output_path = os.path.join(os.path.dirname(__file__), "sample_trials.csv")

with open(output_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "patient_id", "trial_id", "drug", "site",
        "dosage_mg", "age", "weight_kg", "enrollment_date",
        "status", "outcome_score"
    ])

    for i in range(1, 1001):  # 1000 sample records
        writer.writerow([
            f"P{i:05d}",
            f"TRIAL_{random.randint(1, 5):03d}",
            random.choice(DRUGS),
            random.choice(SITES),
            round(random.uniform(10, 500), 1),
            random.randint(18, 75),
            round(random.uniform(45, 120), 1),
            random_date(datetime(2023, 1, 1), datetime(2024, 6, 1)).strftime("%Y-%m-%d"),
            random.choice(STATUSES),
            round(random.uniform(0, 100), 2) if random.random() > 0.1 else None
        ])

print(f"Generated 1000 sample patient records → {output_path}")
