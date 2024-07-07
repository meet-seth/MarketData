from managers.DownloadManager import DownloadManager
import constants as const
import logging
from datetime import datetime

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
        now = datetime.now()
        today530 = now.replace(hour=17, minute=0, second=0, microsecond=0)
        if now > today530:
            const.LOGGER.info(f"Market Data Download started at {now}")
            self.download_manager.download()
        else:
            const.LOGGER.info(f"Download attempted at : {now}. Waiting for the entire data to be available")
            
    
    
if __name__ == "__main__":
    app = Application()
        
    app.run()
    