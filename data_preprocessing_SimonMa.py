"""
Created on Mon Oct 24 18:41:40 2022
​
@author: Simon
@email : jma48@uic.edu
"""
import sys
import pandas as pd
import copy
import csv
from os import mkdir, remove, listdir
from os.path import join, isdir, isfile
​
path_csv = "/mnt/nas/ioda/Metadata/de_images_diagnosis_joined.csv"
# path_csv = "/Volumes/aio/Metadata/de_images_diagnosis_joined.csv"
path_save = "../glaucoma_suspect_metadata.csv"
​
querry = ['H40.0', 'H40.003', 'H40.001', 'H40.002']
tosave = ['de_FileName', 'GenericID', 'MRN', 'de_FolderName', 'de_SubfolderName', 'de_ExamDate', 'img_type_abrv']
​
df = pd.DataFrame()
chunkSize = 20000
idx = -1; nrows = 0
for chunk in pd.read_csv(path_csv, sep = '\t', header=0, chunksize = chunkSize):
    if len(chunk) == chunkSize:
        nrows += chunkSize
        # ----------- Iterate on each row of the chunck size (Iterator = ii).
        for ii in range(chunkSize):
            try:
                icd_codes = chunk['DiagnosisCode'].iloc[[ii]].values[0].replace(' ', '').split('|')
            except:
                break
            
            # ----------- I only want to save relavent info for Fundus images
            image_type = chunk['img_type_abrv'].iloc[[ii]].values[0]
            
            # ----------- For a row ii of chunck, if its ICD code was in querry, we set "save" to True
            save = False
            for code in icd_codes:
                if code in querry and image_type == 'Fundus':
                    save = True   
                    break
                
            # ----------- Get a subset of chuck by only using "tosave" columns                
            if save:
                irow_chunck = [chunk[name].iloc[[ii]].values[0] for name in tosave]
                idx += 1
                # ----------- Iterate on columns of the "tosave" list up to "examdate" and "imagetype" elements
                for jj, name in enumerate(tosave[:-2]):
                    df.loc[idx, name] = irow_chunck[jj]
                
                if idx % 2 ==0:
                    print(f'=================> Counter= {idx}')
                    df.to_csv(path_save, sep=',', index = False)
    
                # if idx>3:
                #     sys.exit()
    else:
        nrows = nrows + len(chunk)
        print(f'{idx}: Length of the Chunck ({len(chunk)}) is less than chunck size = {chunkSize}')
        print(f'In total the metadata file has {nrows} rows of data!')
        continue
​