import glob
import os
import pandas as pd
from datetime import datetime


class MigrateDb:
    def __init__(self):
        self.path = '/mnt/5a5abb6b-4b3d-4ab8-bfea-9c7fc2ce23e6/linuxuser/Public/University/MSc/DataBases/Project2/archive/'

    street_lights_one_out_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber',
                                    'requestType', 'streetAddress', 'zipcode', 'x_coordinate', 'y_coordinate', 'ward',
                                    'policeDistrict', 'communityArea', 'latitude', 'longitude', 'location']

    graffiti_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber', 'requestType',
                       'surfaceType', 'graffitiLocation', 'streetAddress', 'zipcode', 'x_coordinate', 'y_coordinate',
                       'ward', 'policeDistrict', 'communityArea', 'SSA', 'latitude', 'longitude', 'location',
                       'historical_wards_2003_2015', 'zipcodes', 'communityAreas', 'censusTracts', 'wards']

    potholes_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber', 'requestType',
                        'currentActivity', 'mostRecentAction', 'filledBlockPotholesNum', 'streetAddress',
                        'zipcode', 'x_coordinate', 'y_coordinate', 'ward', 'policeDistrict', 'communityArea',
                        'SSA', 'latitude', 'longitude', 'location', 'historical_wards_2003_2015', 'zipcodes',
                        'communityAreas', 'censusTracts', 'wards']

    garbage_carts_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber', 'requestType',
                            'deliveredBlackCartsNum', 'currentActivity', 'mostRecentAction', 'streetAddress',
                            'zipcode', 'x_coordinate', 'y_coordinate', 'ward', 'policeDistrict', 'communityArea',
                            'SSA', 'latitude', 'longitude', 'location', 'historical_wards_2003_2015', 'zipcodes',
                            'communityAreas', 'censusTracts', 'wards']

    rodent_baiting_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber', 'requestType',
                             'baitedPremisesNum', 'premisesWithGarbageNum', 'premisesWithRatsNum', 'currentActivity',
                             'mostRecentAction', 'streetAddress', 'zipcode', 'x_coordinate', 'y_coordinate', 'ward',
                             'policeDistrict', 'communityArea', 'latitude', 'longitude', 'location',
                             'historical_wards_2003_2015', 'zipcodes', 'communityAreas', 'censusTracts', 'wards']

    tree_trims_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber', 'requestType',
                         'treeLocation', 'streetAddress', 'zipcode', 'x_coordinate', 'y_coordinate', 'ward',
                         'policeDistrict', 'communityArea', 'latitude', 'longitude', 'location',
                         'historical_wards_2003_2015', 'zipcodes', 'communityAreas', 'censusTracts', 'wards']

    street_lights_all_out_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber',
                                    'requestType', 'streetAddress', 'zipcode', 'x_coordinate', 'y_coordinate', 'ward',
                                    'policeDistrict', 'communityArea', 'latitude', 'longitude', 'location',
                                    'historical_wards_2003_2015', 'zipcodes', 'communityAreas', 'censusTracts', 'wards']

    abandoned_vehicle_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber', 'requestType',
                                'licensePlate', 'vehicleMakeModel', 'vehicleColor', 'currentActivity',
                                'mostRecentAction', 'daysReportedParked', 'streetAddress', 'zipcode', 'x_coordinate',
                                'y_coordinate', 'ward', 'policeDistrict', 'communityArea', 'SSA', 'latitude',
                                'longitude', 'location', 'historical_wards_2003_2015', 'zipcodes',
                                'communityAreas', 'censusTracts', 'wards']

    alley_lights_out_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber',
                               'requestType', 'streetAddress', 'zipcode', 'x_coordinate', 'y_coordinate', 'ward',
                               'policeDistrict', 'communityArea', 'latitude', 'longitude', 'location',
                               'historical_wards_2003_2015', 'zipcodes', 'communityAreas', 'censusTracts', 'wards']

    tree_debris_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber', 'requestType',
                          'debrisLocation', 'currentActivity', 'mostRecentAction', 'streetAddress', 'zipcode',
                          'x_coordinate', 'y_coordinate', 'ward', 'policeDistrict', 'communityArea', 'latitude',
                          'longitude', 'location', 'historical_wards_2003_2015', 'zipcodes', 'communityAreas',
                          'censusTracts', 'wards']

    sanitation_code_complaints_fields = ['creationDate', 'statusType', 'completionDate', 'serviceRequestNumber',
                                         'requestType', 'violationNature', 'streetAddress', 'zipcode', 'x_coordinate',
                                         'y_coordinate', 'ward', 'policeDistrict', 'communityArea', 'latitude',
                                         'longitude', 'location', 'historical_wards_2003_2015', 'zipcodes',
                                         'communityAreas', 'censusTracts', 'wards']

    def get_fields(self, file):
        if "abandoned-vehicles" in file:
            return self.abandoned_vehicle_fields
        elif "alley-lights-out" in file:
            return self.alley_lights_out_fields
        elif "garbage-carts" in file:
            return self.garbage_carts_fields
        elif "graffiti-removal" in file:
            return self.graffiti_fields
        elif "pot-holes-reported" in file:
            return self.potholes_fields
        elif "rodent-baiting" in file:
            return self.rodent_baiting_fields
        elif "sanitation-code-complaints" in file:
            return self.sanitation_code_complaints_fields
        elif "street-lights-all-out" in file:
            return self.street_lights_all_out_fields
        elif "street-lights-one-out" in file:
            return self.street_lights_one_out_fields
        elif "debris" in file:
            return self.tree_debris_fields
        elif "tree-trims" in file:
            return self.tree_trims_fields

    def replace_csv_header(self):
        all_files = glob.glob(os.path.join(self.path, "*.csv"))
        for file in all_files:
            if file == "/mnt/5a5abb6b-4b3d-4ab8-bfea-9c7fc2ce23e6/linuxuser/Public/University/MSc/DataBases/Project2" \
                       "/archive/311-service-requests-tree-debris.csv":
                continue
            else:
                print("File: " + file + "\n")
                cur_fields = self.get_fields(file=file)
                df = pd.read_csv(file, header=0, names=cur_fields)
                df.to_csv(path_or_buf=file, index=False)

    def read(self):
        """
        Read a resource from the file specified
        using the appropriate reader for this format
        """
        all_files = glob.glob(os.path.join(self.path, "*.csv"))
        start_time = datetime.now()
        for file in all_files:
            print("\nImporting file: " + file + "\n")
            command = "mongoimport -d ci_311db -c ci_311_incident --type csv --file " + file + " --headerline " \
                                                                                               "--numInsertionWorkers 4"
            os.system(command)
        end_time = datetime.now()
        print("All CSVs imported in collection.\nTotal import time: " + str(end_time-start_time))


if __name__ == "__main__":
    migration = MigrateDb()
    print("Replacing csv field names")
    migration.replace_csv_header()
    print("Start data migration")
    migration.read()
