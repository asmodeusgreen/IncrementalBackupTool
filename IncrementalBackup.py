# This Python file uses the following encoding: utf-8
import os
from pathlib import Path
import sys
import json
import datetime
from shutil import copyfile

from PySide2.QtWidgets import QApplication, QWidget,QPushButton,QProgressBar,QLineEdit,QMessageBox
from PySide2.QtCore import QFile,QThread, Signal
from PySide2.QtUiTools import QUiLoader
import time
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# create a file handler
handler = logging.FileHandler('IncrementalBackupTool.log')
handler.setLevel(logging.INFO)
# create a logging format
formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(filename)s %(lineno)s %(funcName)20s  %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)
# add the file handler to the logger
logger.addHandler(handler)

METADUMP_RETRY = 3
METADATA_FOLDER_NAME = "MetaData"
PROGRESS_PERC = 0


class copyFiles(QThread):
    # Create a counter thread
    change_value = Signal(int)

    def __init__(self,fl_list,tstmp,backlocation,src):
        QThread.__init__(self)
        self.files_list = fl_list
        self.timestamp = tstmp
        self.backupLocation = backlocation
        self.location = src

    def run(self):
        logger.info("Coping files to backup location from the folder %s",self.location)
        parent = os.path.dirname(os.path.normpath(self.location))
        length = len(self.files_list)
        count = 0
        for fl in self.files_list:
            try:
                count+=1
                perc = 100*count/length
                destination = os.path.join(self.backupLocation, self.timestamp,fl)
                source = os.path.join(parent,fl)
                copyfile(source,destination)
                self.change_value.emit(perc)
            except Exception as e:
                logger.error("Error in coping files %s",e)
                return
                #alertBox("Backup Failed Check logs")
                self.change_value.emit(5000)
        #alertBox("Backup Successful","i")
        self.change_value.emit(1000)
        
class RestoreFiles(QThread):
    # Create a counter thread
    change_value = Signal(int)

    def __init__(self,fl_list,tstmp,backlocation,src):
        QThread.__init__(self)
        self.files_list = fl_list
        self.timestamp = tstmp
        self.backupLocation = backlocation
        self.location = src

    def run(self):
        logger.info("Restoring files %s",self.location)
        parent = os.path.dirname(os.path.normpath(self.location))
        length = len(self.files_list)
        count = 0
        for fl in self.files_list:
            try:
                count+=1
                perc = 100*count/length
                source = os.path.join(self.backupLocation, self.timestamp,fl)
                destination = os.path.join(self.location,fl)
                copyfile(source,destination)
                self.change_value.emit(perc)
            except Exception as e:
                logger.error("Error in Restoring files %s",e)
                return
                #alertBox("Backup Failed Check logs")
                self.change_value.emit(6000)
        #alertBox("Backup Successful","i")
        self.change_value.emit(2000)

def alertBox(msg_txt,msg_type="c"):
    logger.info("Sending alert %s",msg_txt)
    msg = QMessageBox()
    msg.setWindowTitle("Alert")
    msg.setText(msg_txt)
    if msg_type == "i":
        msg.setIcon(QMessageBox.Information)
    elif msg_type == "w":
        msg.setIcon(QMessageBox.Warning)
    elif msg_type == "q":
        msg.setIcon(QMessageBox.Question)
    else:
        msg.setIcon(QMessageBox.Critical)
    x = msg.exec_()


def copyFiles1(files_list,timestamp,q,main_obj,backupLocation,location):
    logger.info("Coping files to backup location from the folder %s",location)
    parent = os.path.dirname(os.path.normpath(location))
    length = len(files_list)
    count = 0
    #main_obj.uiElements.ProgressBar.reset()
    #main_obj.uiElements.ProgressStatus.setText("Coping files")                
    for fl in files_list:
        try:
            count+=1
            perc = 100*count/length
            destination = os.path.join(backupLocation, timestamp,fl)
            source = os.path.join(parent,fl)
            copyfile(source,destination)
            q.put(perc)
            main_obj.uiElements.ProgressBar.setValue(perc)
        except Exception as e:
            logger.error("Error in coping files %s",e)
            main_obj.uiElements.ProgressStatus.setText("Failed")
            q.put(-1)
            return
            #alertBox("Backup Failed Check logs")
    #alertBox("Backup Successful","i")
    main_obj.uiElements.ProgressStatus.setText("Done")
    q.put(100)
    

class IncrementalBackup(QWidget):
    def __init__(self):
        super(IncrementalBackup, self).__init__()
        self.uiElements = None
        self.load_ui()
        self.backupLocation = None
        self.folderToBackup = None
        self.oldBackupFilesList = []
        self.oldBackupFoldersList = []
        self.oldBackupDeletedFilesList = []
        self.oldBackupDeletedFoldersList = []
        self.folders_list = []
        self.files_list = []
        self.deleted_files = []
        self.deleted_folders = []
        self.backupTime = 0
        self.uiElements.BackupButton.clicked.connect(self.takeBackup)
        self.uiElements.RestoreButton.clicked.connect(self.restoreData)
        self.uiElements.LoadBackupButton.clicked.connect(self.loadBackup)
        self.uiElements.ProgressBar.reset()

    def loadBackup(self):
        logger.info("Loading backup")
        if self.backupLocation == None or self.backupLocation == "":
            if self.getFoldersName():
                logger.info("Fetched the name of backup fodler and restore folder")
            else:
                logger.error("Something wrong in fetching folders name")
        oldBackup = self.getOldBackupTime()
        oldBackupDate = []
        for x in oldBackup:
            t_frm = datetime.datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S')
            oldBackupDate.append(t_frm)
        self.uiElements.BackupListBox.insertItems(0,oldBackupDate)
        logger.info("Data loaded in list for UI")

    
    def restoreData(self):
        logger.info("Restoring Data")
        if self.backupLocation == None or self.backupLocation == "":
            if self.getFoldersName():
                logger.info("Fetched the name of backup fodler and restore folder")
            else:
                logger.error("Something wrong in fetching folders name")
        p = '%Y-%m-%d %H:%M:%S'
        mytime = str(self.uiElements.BackupListBox.currentText())
        epoch = datetime.datetime(1970, 1, 1,5,30,0)
        ntime = int((datetime.datetime.strptime(mytime, p) - epoch).total_seconds())
        try:
            with open(self.backupLocation+"\\"+METADATA_FOLDER_NAME+"\\"+str(ntime)+".json") as infile:
                data = json.load(infile)
                ret = self.createRestoreDirectories(data["Backup"]["Folders"],str(ntime))
                if not ret:
                    alertBox("Restore Failed check logs")
                    return
                self.uiElements.ProgressStatus.setText("Restoring files")
                self.uiElements.ProgressBar.reset()
                self.thread1 = RestoreFiles(data["Backup"]["Files"],str(ntime),self.backupLocation,self.folderToBackup)
                self.thread1.change_value.connect(self.setProgressVal)
                self.thread1.start()      
        except Exception as e:
            logger.error("Error in loading backup meta file %s",e)
            alertBox("Restore Failed check logs")
            return
        
    def getOldBackupTime(self):
        oldBackupList = []
        for (root,dirs,files) in os.walk(self.backupLocation+"\\"+METADATA_FOLDER_NAME, topdown=True):
            for fn in files:
                t = fn.strip(".json")
                if os.path.isdir(self.backupLocation+"\\"+t): 
                    oldBackupList.append(t)
                else:
                    #self.alertBox("Backup folder inconsistent")
                    alertBox("Backup folder inconsistent")
                    return None
        oldBackupList.sort(reverse=True)
        return oldBackupList

    def list_files(self):
        folders_list = []
        files_list = []
        logger.info("Reading folder hierarchy from path %s",self.folderToBackup)
        try:
            base = os.path.basename(os.path.normpath(self.folderToBackup))
            for (root,dirs,files) in os.walk(self.folderToBackup, topdown=True):
                nroot = root.replace(self.folderToBackup,base)
                folders_list.append(nroot)
                for fn in files:
                    path = os.path.join(nroot, fn)
                    #size = os.stat(path).st_size
                    files_list.append(str(path))
            logger.info("Reading folder hierarchy complete %s",self.folderToBackup)
        except Exception as e:
            logger.error("Error in traversing folder %s",e)
        return folders_list,files_list

    def dumpMetaInfo(self,filename,folders_list,files_list,deleted_folders,deleted_files):
        for x in range(METADUMP_RETRY):
            try:
                logger.info("Writing meta info of backup retry count %d",x)
                with open(self.backupLocation+"\\"+METADATA_FOLDER_NAME+"\\"+filename+".json","w") as outfile:
                    outfile.write(json.dumps({"Backup":{"Folders":folders_list,"Files":files_list,"DeletedFolders":deleted_folders,"DeletedFiles":deleted_files}},indent=4))
                logger.info("Meta info dumped to the disk")
                break
            except Exception as e:
                logger.error("Error dumping meta info to disk %s",e)
                return False
        return True

    def createDirectories(self,folders_list,timestamp):
        logger.info("Creating directories to backup location to location %s",self.backupLocation)
        self.uiElements.ProgressBar.reset()
        self.uiElements.ProgressStatus.setText("Creating Directories")
        length = len(folders_list)
        count = 0
        for folder in folders_list:
            try:
                count+=1
                perc = 100*count/length
                folder_to_create = os.path.join(self.backupLocation, timestamp,folder)
                os.makedirs(folder_to_create, exist_ok=True)
                self.uiElements.ProgressBar.setValue(perc)
                #time.sleep(1)
            except Exception as e:
                logger.error("Error on creating folder %s :: %s",folder_to_create,e)
                return False
        return True


    def createRestoreDirectories(self,folders_list,timestamp):
        logger.info("Creating directories to backup location to location %s",self.backupLocation)
        self.uiElements.ProgressBar.reset()
        self.uiElements.ProgressStatus.setText("Restoring Directories")
        length = len(folders_list)
        count = 0
        for folder in folders_list:
            try:
                count+=1
                perc = 100*count/length
                folder_to_create = os.path.join(self.folderToBackup,folder)
                if not os.path.isdir(folder_to_create): 
                    os.makedirs(folder_to_create, exist_ok=True)
                self.uiElements.ProgressBar.setValue(perc)
                #time.sleep(1)
            except Exception as e:
                logger.error("Error on creating folder %s :: %s",folder_to_create,e)
                return False
        return True

    def createBackupFolder(self,timestamp):
        logger.info("Creating folder and files with timestamp %s",timestamp)
        try:
            folder_to_create = os.path.join(self.backupLocation, timestamp)
            meta_folder = os.path.join(self.backupLocation,METADATA_FOLDER_NAME)
            os.mkdir(folder_to_create)
            if not os.path.isdir(meta_folder): 
                os.mkdir(meta_folder)
            return True
        except Exception as e:
            logger.error("Error creating backup folder with timestamp %s : %s",timestamp,e)
            return False

    def getFoldersName(self):
        logger.info("Reading folders name")
        self.backupLocation = self.uiElements.BackupLocationInput.text()
        self.folderToBackup = self.uiElements.FolderToBackupInput.text()
        if self.backupLocation == "" or self.folderToBackup == "":
            if self.folderToBackup == "":
                logger.error("Folder To Backup Empty")
                #self.alertBox("Folder To Backup can not be empty")
                alertBox("Folder To Backup can not be empty")        
            else:
                logger.error("BackupLocation Empty")
                #self.alertBox("BackupLoaction can not be empty")
                alertBox("BackupLoaction can not be empty")
            return False
        logger.info("Folder names loaded to backend from UI")
        return True  

    def getOldFilesInfo(self):
        logger.info("loading old files and folder information")
        oldBackupTime = self.getOldBackupTime()
        for elem in oldBackupTime:
            try:
                with open(self.backupLocation+"\\"+METADATA_FOLDER_NAME+"\\"+elem+".json") as infile:
                    data = json.load(infile)
                    self.oldBackupFilesList = self.oldBackupFilesList + list(set(data["Backup"]["Files"]) - set(self.oldBackupFilesList))
                    self.oldBackupFoldersList = self.oldBackupFoldersList + list(set(data["Backup"]["Folders"]) - set(self.oldBackupFoldersList))
                    self.oldBackupDeletedFilesList = self.oldBackupDeletedFilesList + list(set(data["Backup"]["DeletedFiles"]) - set(self.oldBackupDeletedFilesList))
                    self.oldBackupDeletedFoldersList = self.oldBackupDeletedFoldersList + list(set(data["Backup"]["Files"]) - set(self.oldBackupDeletedFoldersList))
            except Exception as e:
                logger.error("Error in loading old backup data %s",e)
                return False
        return True

    def checkFilesToBackup(self,timestamp,folders_list,files_list):
        logger.info("Checking files and folder to take backup")
        deleted_files = []
        deleted_folders = []
        filesToRemove = []
        parent = os.path.dirname(os.path.normpath(self.folderToBackup))
        oldBackupTime = self.getOldBackupTime()
        lastBackup = 0
        if len(oldBackupTime) > 0:
            lastBackup = int(oldBackupTime[0])
        for fl_elem in files_list:
            if fl_elem in self.oldBackupFilesList:
                modTimesinceEpoc = os.path.getmtime(parent+"\\"+fl_elem)
                if modTimesinceEpoc < lastBackup:
                    filesToRemove.append(fl_elem)
        #finalFoldersToBackup = self.oldBackupFoldersList + list(set(folders_list) - set(self.oldBackupFoldersList))
        if len(self.oldBackupFoldersList) > 0: 
            deleted_folders = list(set(self.oldBackupFoldersList) - set(folders_list))
        if len(self.oldBackupFilesList) > 0: 
            deleted_files = list(set(self.oldBackupFilesList) - set(files_list))
        for del_item in filesToRemove:
            files_list.remove(del_item)
        return folders_list,files_list,deleted_folders,deleted_files

    
    def setProgressVal(self, val):
        if val == 5000:
            self.uiElements.ProgressStatus.setText("Failed")
            alertBox("Backup Failed check logs")
        elif val == 1000:
            self.uiElements.ProgressStatus.setText("Done") 
            alertBox("Backup Successful","i")
        elif val == 2000:
            self.uiElements.ProgressStatus.setText("Done")
            alertBox("Restore Successful","i")
        elif val == 6000:
            self.uiElements.ProgressStatus.setText("Failed")
            alertBox("Restore Failed check logs")
        else:
            self.uiElements.ProgressBar.setValue(val)
        
    def takeBackup(self):
        logger.info("Taking Backup")
        self.backupTime = str(int(time.time()))
        if self.getFoldersName():
            if self.getOldFilesInfo():
                self.folders_list,self.files_list = self.list_files()
                self.folders_list,self.files_list,self.deleted_folders,self.deleted_files = self.checkFilesToBackup(self.backupTime,self.folders_list,self.files_list)
                if self.createBackupFolder(self.backupTime):
                    ret = self.createDirectories(self.folders_list,self.backupTime)
                    if not ret:
                        #self.alertBox("Backup Failed Check logs")
                        alertBox("Backup Failed Check logs")
                        return
                    self.uiElements.ProgressBar.reset()
                    self.uiElements.ProgressStatus.setText("Coping files") 
                    #x = threading.Thread(target=copyFiles, args=(self.files_list,self.backupTime,q,self,self.backupLocation,self.folderToBackup,))
                    self.thread = copyFiles(self.files_list,self.backupTime,self.backupLocation,self.folderToBackup)
                    self.thread.change_value.connect(self.setProgressVal)
                    self.thread.start()
                    #copyFiles(self.files_list,self.backupTime,self,self.backupLocation,self.folderToBackup)
                    ret = self.dumpMetaInfo(self.backupTime,self.folders_list,self.files_list,self.deleted_folders,self.deleted_files)
                else:
                    #self.alertBox("Backup Failed Check logs")
                    alertBox("Backup Failed Check logs")
                    return
            else:
                self.alertBox("Backup Failed Check logs")
                

    def load_ui(self):
        loader = QUiLoader()
        path = os.fspath("./UIFile.ui")
        ui_file = QFile(path)
        ui_file.open(QFile.ReadOnly)
        self.uiElements = loader.load(ui_file, self)
        ui_file.close()



if __name__ == "__main__":
    time.sleep(1)
    app = QApplication([])
    widget = IncrementalBackup()
    widget.show()
    sys.exit(app.exec_())
