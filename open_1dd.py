# -*- coding:utf-8 -*-


'''
The codes below were tested with GPCP data downloaded from 
ftp://meso.gsfc.nasa.gov/pub/1dd-v1.2/ .

Infact, if you clone the above archive (../1dd-v1.2/) to a
directory called 'data/', unzip all the files and just run 
this file (python open_1dd.py) it should just work; 
at the end, a directory called 'proc/' should contain the 
monthly average of each original file.
'''

import sys
import numpy as np

def read_1dd_num_days(filepath):
    """
    The "days" metadata is days=1-nn, where nn is the number
    of days in the month, which is what we want for the 3rd
    dimension. Hence the "+7".
    """


    num_lon = 360
    with open(filepath, 'rb') as filein:
        #filein = open(filepath, 'rb')
        header = filein.read(num_lon*4)


        header = header.rstrip()
    days = header.find(b'days=')+7
    num_days = int(header[days:days+2])
    return num_days, header



def byte_swap_1DD_struct(filepath, num_days, header):
    """
    Uses output of read_1dd_num_day to determine if byte-swap is needed
    """

    num_lon = 360 # rows
    num_lat = 180 # cols


    # see if file is written in big-endian (indicated by Silicon machine)
    if b'Silicon' in header:
        file_byte_order = 'big'
    else:
        file_byte_order = 'little'
        #file_byte_order = 'little'


    if sys.byteorder == 'little' and file_byte_order == 'big':
        # open file using big endian dtype and select all values that correspond with to the metadata
        data = np.fromfile(filepath, dtype='>f')[-(num_lon*num_lat*num_days)::]
    else:
        data = np.fromfile(filepath, dtype='f')[-(num_lon*num_lat*num_days)::]

    data = data.reshape((num_days,num_lat,num_lon))

    return data


def read_1DD(filepath, inheader=False):
    '''
    The main procedure; read both
    header and data, and swap bytes if needed.

    input: filepath
    inheader: True/False (False is default), determines if header is included in output

    output:
    if inheader = False, no header is included, output is numpy array (default)
    if inheader = True, header is included output is dictionary
    '''

    num_days, header = read_1dd_num_days(filepath)
    data = byte_swap_1DD_struct(filepath, num_days, header)

    if inheader == False:
        struct = data
    elif inheader == True:
        struct = { 'header': header, 'data': data }
    return struct


def read_1DD_header(filepath):
    '''
    Just read the header
    '''

    num_lon = 360
    with open(filepath, 'rb') as filein:
        header = filein.read(num_lon*4)
        header = header.rstrip()
    return header

def note_metadata(header,metatable,data_filename=None):
    '''
    Get metadata from header and add to 'metatable'

    'metatable' column names are keys found in 'header'.
    '''
    import pandas
    _meta = {}
    header_words = header.split()
    for word in header_words:
        if not '=' in word:
            continue
        key,value = word.split('=')
        if key in metatable.columns:
            _meta[key] = value
    if data_filename != None:
        _meta['file'] = data_filename
    metatable = metatable.append( pandas.DataFrame(_meta, columns=_meta.keys(), index=[0]), ignore_index=True)
    return metatable

def main(filename,metatable,outdir=''):
    '''
    Run the function to extract dat and header,
    cmputes the monthly mean and write them to 
    accordingly named files
    '''
    OUTDIR = outdir

    f1dd = read_1DD(filename, inheader=True)
    data = f1dd['data']
    header = f1dd['header']

    # data is a (31,180,360) shaped numpy array
    # we want the monthly mean, which means we
    # want the first-dimension simple mean value
    num_days = data.shape[0]
    data_mean = np.sum(data,axis=0)/num_days

    # 'data_mean' is now a 2-dimensional array
    #header_filename = filename + '_header.txt'
    #with open(header_filename,'w') as hdrf:
    #    hdrf.write(header)
    #data_filename = filename + '_data_monthlyMean.csv'
    #np.savetxt(data_filename, data_mean, delimiter=',')
    import os
    data_filename = os.path.basename(filename) + '_monthlyAverage.npy'
    np.save(OUTDIR + data_filename, data_mean, allow_pickle=False)
    metatable = note_metadata(header,metatable,data_filename)
    return metatable

def run(indir,outdir):
    '''
    Run 'main' in a directory containing lots of gpcp_1dd_v1.2_p1d.* files
    '''
    import pandas
    df = pandas.DataFrame(columns=['year','month','file','missing_value','unit'])

    import os
    from glob import glob
    fls = glob(os.path.join(indir,'gpcp_1dd_v1.2_p1d.*'))

    OUTDIR = outdir
    import os
    if os.path.isdir(OUTDIR):
        import shutil
        shutil.rmtree(OUTDIR)
    os.mkdir(OUTDIR)

    for i,f in enumerate(fls):
        print 'Processing file {} [{}/{}]'.format(f,i+1,len(fls))
        df = main(f,df,OUTDIR)
        print 'done'
    df.to_csv(OUTDIR + 'table_files.csv',index=False)
    
if __name__ == '__main__':
    import os
    indir = 'data'
    outdir = os.path.join(indir,'numpy_arrays')
    run(indir,outdir)
    