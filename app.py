from managers.DatabaseManager import DatabaseManager
from managers.DownloadManager import DownloadManager

class Application:
    def __init__(self) -> None:
        self.database_manager = DatabaseManager()
        self.download_manager = DownloadManager()
    
    def run(self):
        last_recorded_timestamp = self.database_manager.get_last_timestamp()
        data = self.download_manager.download(last_recorded_timestamp)
        self.database_manager.update_database(data)
    
    
if __name__ == "__main__":
    app = Application()
    
    app.run()
    