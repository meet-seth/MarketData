from managers.ParquetManager import MongoToParquet

mtp = MongoToParquet()

print(mtp.save_all())