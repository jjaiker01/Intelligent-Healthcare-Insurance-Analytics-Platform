# scripts/presidio/generate_data.py
import json
import csv
import random
import datetime
from faker import Faker
from faker.providers import BaseProvider

fake = Faker('es_CO')  # Colombian Spanish locale

class VehicleProvider(BaseProvider):
    def vehicle_type(self):
        """
        Returns a random vehicle type.

        This method selects a random vehicle type from a predefined list
        which includes 'Motorcycle', 'Private Car', and 'Tractor-Trailer'.

        Returns:
            str: A string representing the vehicle type.
        """

        return random.choice(['Motorcycle', 'Private Car', 'Tractor-Trailer'])

fake.add_provider(VehicleProvider)

def generate_patient_data(num_patients=300000):
    """
    Generates a list of patient records with attributes like patient ID, name, age, address, and medical history.

    Args:
        num_patients (int): The number of patient records to generate. Defaults to 300000.

    Returns:
        list: A list of patient records, each represented as a dictionary with the following keys:
            patient_id (int): Unique identifier for each patient.
            name (str): Full name of the patient.
            age (int): Age of the patient.
            address (str): Full address of the patient.
            medical_history (str): A short text describing the patient's medical history.
    """
    patients = []
    for patient_id in range(1, num_patients + 1):
        patients.append({
            'patient_id': patient_id,
            'name': fake.name(),
            'age': random.randint(18, 85),
            'address': fake.address(),
            'medical_history': fake.text(max_nb_chars=200)
        })
    return patients

def generate_accident_data(patients):
    """
    Generates a list of accident records associated with given patients.

    This function creates accident records for each patient, with 1 to 2 accidents per patient.
    Each accident includes details such as accident ID, date, location, vehicle type, patient ID,
    driver ID, accident cause, and severity.

    Args:
        patients (list): A list of patient records, where each record is a dictionary that includes
                         a 'patient_id' key.

    Returns:
        list: A list of accident records, each represented as a dictionary with the following keys:
            accident_id (int): Unique identifier for each accident.
            accident_date (str): The date of the accident in 'YYYY-MM-DD' format.
            location (str): The location of the accident, set in Antioquia.
            vehicle_type (str): The type of vehicle involved in the accident.
            patient_id (int): The ID of the patient involved in the accident.
            driver_id (int): Unique identifier for the driver.
            accident_cause (str): A short text describing the cause of the accident.
            severity (str): The severity level of the accident ('Minor', 'Moderate', or 'Severe').
    """

    accidents = []
    accident_id = 1
    for patient in patients:
        num_accidents = random.randint(1, 2)
        for _ in range(num_accidents):
            accident_date = fake.date_between(start_date=datetime.date(2000, 1, 1), end_date=datetime.date(2024, 1, 1))
            accidents.append({
                'accident_id': accident_id,
                'accident_date': accident_date.strftime('%Y-%m-%d'),
                'location': fake.city() + ", Antioquia",
                'vehicle_type': fake.vehicle_type(),
                'patient_id': patient['patient_id'],
                'driver_id': random.randint(1000, 9999),
                'accident_cause': fake.sentence(),
                'severity': random.choice(['Minor', 'Moderate', 'Severe'])
            })
            accident_id += 1
    return accidents

def generate_soat_data(accidents):
    """
    Generates a list of SOAT records associated with given accidents.

    This function creates SOAT records for each accident, with a unique SOAT ID, vehicle ID (driver ID for simplicity),
    start date, end date, insurance provider, and coverage details.

    Args:
        accidents (list): A list of accident records, where each record is a dictionary that includes
                         a 'driver_id' key.

    Returns:
        list: A list of SOAT records, each represented as a dictionary with the following keys:
            soat_id (int): Unique identifier for each SOAT.
            vehicle_id (int): The ID of the vehicle associated with the SOAT.
            start_date (str): The start date of the SOAT in 'YYYY-MM-DD' format.
            end_date (str): The end date of the SOAT in 'YYYY-MM-DD' format.
            insurance_provider (str): The name of the insurance provider.
            coverage_details (str): A short text describing the coverage details.
    """
    soats = []
    soat_id = 1
    for accident in accidents:
        start_date = datetime.datetime.strptime(accident['accident_date'], '%Y-%m-%d').date()
        end_date = start_date + datetime.timedelta(days=365)
        soats.append({
            'soat_id': soat_id,
            'vehicle_id': accident['driver_id'], # Driver ID as Vehicle ID for simplicity.
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'insurance_provider': random.choice(['SURA', 'Allianz', 'Mapfre']),
            'coverage_details': fake.sentence()
        })
        soat_id += 1
    return soats

def save_to_json(data, filename):
    """
    Saves given data as a JSON file to the given filename.

    Args:
        data (list or dict): The data to be saved.
        filename (str): The name of the file to save the data to.
    """
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

def save_to_csv(data, filename, header):
    """
    Saves given data as a CSV file to the given filename.

    Args:
        data (list): A list of dictionaries, where each dictionary represents a row in the CSV file.
        filename (str): The name of the file to save the data to.
        header (list): A list of strings representing the column names in the CSV file.
    """
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    patients = generate_patient_data()
    accidents = generate_accident_data(patients)
    soats = generate_soat_data(accidents)

    save_to_json(patients, 'data/raw/json/patients.json')
    save_to_json(accidents, 'data/raw/json/accidents.json')
    save_to_json(soats, 'data/raw/json/soats.json')

    save_to_csv(patients, 'data/raw/csv/patients.csv', patients[0].keys())
    save_to_csv(accidents, 'data/raw/csv/accidents.csv', accidents[0].keys())
    save_to_csv(soats, 'data/raw/csv/soats.csv', soats[0].keys())

print("Data generation complete.")

# Test
def test_data_generation():
    """
    Tests the data generation functions for patients, accidents, and SOAT records.

    This function generates a small number of patient records, corresponding accident
    records, and SOAT records to validate the data generation process. It asserts that:
    - The number of generated patients matches the expected count.
    - Accidents and SOAT records are generated and not empty.
    - Patient records contain the required keys.

    The function prints a success message if all assertions pass.
    """

    patients = generate_patient_data(10)
    accidents = generate_accident_data(patients)
    soats = generate_soat_data(accidents)

    assert len(patients) == 10
    assert len(accidents) > 0
    assert len(soats) > 0

    patient_keys = patients[0].keys()
    assert 'patient_id' in patient_keys
    assert 'name' in patient_keys
    print("Tests passed")

test_data_generation()