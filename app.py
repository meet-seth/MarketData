from managers.DownloadManager import DownloadManager

class Application:
    def __init__(self):
        self.download_manager = DownloadManager()
    
    def run(self):
        self.download_manager.download()
    
    
if __name__ == "__main__":
    app = Application()
    
    app.run()
    