import os
import sys
import time
from datetime import datetime
from datetime import date

## File Tools ##
def fcheck( path ):
    # "path" can either be a full path to a file or to a directory
    # Return True if it exists and return False if not
    if os.access(path, os.F_OK):
        return True
    else:
        return False

def rwxcheck( path ):
    # "path" can either be a full path to a file or to a directory
    # Raise exception if file does not exist
    # Return True if access is allowed and return False if not
    if fcheck(path):
        if os.access(path, os.R_OK and os.W_OK and os.X_OK):
            return True
        else:
            return False
    else:
        raise OSError('File or directory not found: {}'.format(path))

def rwcheck( path ):
    # "path" can either be a full path to a file or to a directory
    # Raise exception if file does not exist
    # Return True if access is allowed and return False if not
    if fcheck(path):
        if os.access(path, os.R_OK and os.W_OK):
            return True
        else:
            return False
    else:
        raise OSError('File or directory not found: {}'.format(path))

def rxcheck( path ):
    # "path" can either be a full path to a file or to a directory
    # Raise exception if file does not exist
    # Return True if access is allowed and return False if not
    if fcheck(path):
        if os.access(path, os.R_OK and os.X_OK):
            return True
        else:
            return False
    else:
        raise OSError('File or directory not found: {}'.format(path))

def rcheck( path ):
    # "path" can either be a full path to a file or to a directory
    # Raise exception if file does not exist
    # Return True if access is allowed and return False if not
    if fcheck(path):
        if os.access(path, os.R_OK):
            return True
        else:
            return False
    else:
        raise OSError('File or directory not found: {}'.format(path))

def usercheck( user ):
    if os.getlogin() == user:
        return True
    else:
        return False

def readfile( file ):
    # "file" should either be a full path to a file
    # or a file in the working directory
    # Returns a file object, this call should be stored in a variable
    # as it is very difficult to access the file object otherwise
    # If the file exists open it for reading otherwise throw exception
    if rcheck(file):
        return open(file, 'r')
    else:
        raise OSError('File not found: {}'.format(file))

def writefile( file ):
    # "file" should either be a full path to a file
    # or a file in the working directory
    # Returns a file object, this call should be stored in a variable
    # as it is very difficult to access the file object otherwise
    # If the file exists open it for writing otherwise throw exception
    if rwcheck(file):
        return open(file, 'w')
    else:
        raise OSError('File not found: {}'.format(file))

def appendfile( file ):
    # Not intended for logging, use logtools
    # "file" should either be a full path to a file
    # or a file in the working directory
    # Returns a file object, this call should be stored in a variable
    # as it is very difficult to access the file object otherwise
    # If the file exists open it for append otherwise throw exception
    if fcheck(file):
        return open(file, 'a')
    else:
        raise OSError('File not found: {}'.format(file))

def getlocation():
    # EssentiallY is nothing more than an alias for os.getcwd()
    # Leaving it for now in case of future directory functions being added
    loc = os.getcwd()
    return loc

def changedir( path ):
    # "path" should be a full path to a directory
    # If read and execute permissions are available
    # change to the "path"
    # If the directory does not exist, the fcheck() should raise exception
    # If the directory exists and the permissions do not, it will raise
    # an exception indicating the permissions are insufficient.
    if rxcheck(path):
        os.chdir(path)
    else:
        raise OSError('Insufficient privileges on: {}'.format(path))
def dircheck( path ):
    if os.is_dir(path, follow_symlinks=True):
        return True
    else:
        return False