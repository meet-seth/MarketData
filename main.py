from managers.DownloadManager import DownloadManager
import constants as const
import logging

class Application:
    def __init__(self):
        self.download_manager = DownloadManager()
        const.LOGGER = logging.getLogger("Market_Data")
        const.LOGGER.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
        filehandler = logging.FileHandler("market_data.log",
                                          encoding='utf-8')
        filehandler.setFormatter(formatter)
        const.LOGGER.addHandler(filehandler)
    
    def run(self):
        self.download_manager.download()
    
    
if __name__ == "__main__":
    app = Application()
    
    app.run()
    