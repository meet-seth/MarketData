from managers.ParquetManager import MongoToParquet

mtp = MongoToParquet()

print(mtp.convert_all())