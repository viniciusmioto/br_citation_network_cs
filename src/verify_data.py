import csv

# File name of your CSV data
csv_file = "./citations_edgelist.csv"

# A set to hold all citation edges as (origin_doi, target_doi)
edges = set()

# Open the CSV file and read each row
with open(csv_file, mode="r", newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        origin = row["origin_doi"].strip()  # Remove any surrounding whitespace
        target = row["target_doi"].strip()
        # Add the edge to the set
        edges.add((origin, target))

# A set to store two-way citation pairs.
# We'll use a sorted tuple (min_doi, max_doi) to avoid duplicate printing.
two_way_edges = set()

# Iterate over each edge to check if its reverse exists
for origin, target in edges:
    if (target, origin) in edges:
        # Use sorted tuple so that each bidirectional pair is only added once
        pair = tuple(sorted([origin, target]))
        two_way_edges.add(pair)

# Print the results
if two_way_edges:
    print("Two-way (bidirectional) citations found:")
    for pair in two_way_edges:
        print(pair)
else:
    print("No two-way citations were detected.")
