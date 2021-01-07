#mongoimport -d mydb -c things --type csv --file locations.csv --headerline
import glob
import os


class MigrateDb:
    def __init__(self):
        self.path = '/mnt/5a5abb6b-4b3d-4ab8-bfea-9c7fc2ce23e6/linuxuser/Public/University/MSc/DataBases/Project1/archive/'

    def read(self):
        """
        Read a resource from the file specified
        using the appropriate reader for this format
        """
        all_files = glob.glob(os.path.join(self.path, "*.csv"))
        print(all_files)
        for file in all_files:
            command = "mongoimport -d ci_311db -c Incident --type csv --file " + file + " --headerline"
            os.system(command)
        print("All CSVs imported in collection.\n")


if __name__ == "__main__":
    migration = MigrateDb()
    migration.read()