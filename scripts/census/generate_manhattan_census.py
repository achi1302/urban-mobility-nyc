import csv

# MANUAL INPUT
regions_data = {
    "Lower Manhattan": [
        {"name": "Financial_District-Greenwich Village", "median_property_value": 1590000, "population": 158067, "area": 2.48},
        {"name": "Lower_East_Side-Chinatown", "median_property_value": 820600, "population": 153288, "area": 1.76}
    ],
    "Midtown Manhattan": [
        {"name": "Chelsea-Hells_Kitchen", "median_property_value": 914800, "population": 117099, "area": 1.72},
        {"name": "Midtown-East_Midtown-Flatiron", "median_property_value": 1040000, "population": 196979, "area": 2.68},
        {"name": "Upper_West_Side", "median_property_value": 1370000, "population": 218761, "area": 1.92},
        {"name": "Upper_East_Side-Roosevelt Island", "median_property_value": 1360000, "population": 212308, "area": 1.95}
    ],
    "Upper Manhattan": [
        {"name": "Morningside_Heights-Hamilton_Heights", "median_property_value":772100, "population": 112774, "area": 1.18},
        {"name": "Harlem", "median_property_value":897900, "population": 134893, "area": 2.30},
        {"name": "East_Harlem", "median_property_value":702700, "population": 124499, "area": 1.52},
        {"name": "Washington_Heights-Inwood", "median_property_value":583800, "population": 199120, "area": 2.88}

    ]

}

# CALCULATE REGION DATA
def calculate_region_data(region_name, subregions):
    total_pop = sum(sub["population"] for sub in subregions)
    total_area = sum(sub["area"] for sub in subregions)
   
    weighted_income_sum = sum(sub["median_property_value"] * sub["population"] for sub in subregions)
    aprox_property_value = round(weighted_income_sum / total_pop)
    
    population_density = round(total_pop / total_area)

    return {
        "PURegion": region_name,
        "aprox_property_value": aprox_property_value,
        "population_density": population_density
    }

# CSV CREATION
output_file = [calculate_region_data(region, data) for region, data in regions_data.items()]

with open("data/external/manhattan_region_census.csv", "w", newline="") as csvfile:
    fieldnames = ["PURegion", "aprox_property_value", "population_density"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for row in output_file:
        writer.writerow(row)

