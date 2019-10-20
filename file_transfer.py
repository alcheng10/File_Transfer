import boto3
import s3fs
import os
import shutil
import sys
import glob
import logging
import argparse
import re
from datetime import datetime
from base64 import b64decode
from smb.SMBConnection import SMBConnection
from io import BytesIO

#s3 = s3fs.S3FileSystem(anon=False)
s3 = boto3.client('s3')
ddb = boto3.client('dynamodb')

logger = logging.getLogger()


def decrypt_KMS_credentials(username, password):
    ''' Decrypt credentials using KMS 
    
        Parameters:
        Encrypted username and password (string)

        Returns:
        Plaintext username and password (string)
    '''
    username = boto3.client('kms').decrypt(CiphertextBlob=b64decode(username))['Plaintext'].decode('utf-8')
    password = boto3.client('kms').decrypt(CiphertextBlob=b64decode(password))['Plaintext'].decode('utf-8')

    return username, password

def identify_location_type(directory):
    '''
        Returns the location type of the location directory.

        Parameters
        ---------
        directory: string
        Either on-prem full URI or S3 path
        (e.g. 10.21.13.12/Matillion_Output/hello.csv or s3://bucket-test/hello.csv)

        Returns
        ----------
        type: string
        on-prem or S3.
    '''
    
    if 's3://' in directory:
        return 's3'

    # If valid IP address in root
    rootURL = directory.split("/")[0]
    if re.search('^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$', rootURL) != None:
        # Check if there is actually a file directory after IP
        try:
            subURL = directory.split("/")[1]
        except:
            raise Exception
        return 'on-prem'

    # Else raise exception if not valid
    raise Exception


def _move_s3_to_s3(source_location, target_location):
    '''
        To move files between S3 locations (either within same bucket or in different buckets).

        Does not support cross-account S3.

        S3 location must be in nonprod.

        Uses S3FS.

        Parameters
        --------
        source_location: string
        target_location: string
        format 's3://[name of bucket]'

        Returns
        --------
        True - successful
        Exception - if failed
    '''
    logger.debug('moving S3 to S3 file...')

    s3_source_directory = source_location.split("s3://")[1]
    s3_target_directory = target_location.split("s3://")[1]
    
    try: 
        s3.mv(s3_source_directory, s3_target_directory)
    except Exception as e:
        raise Exception
    
    return True


def _move_on_prem_to_on_prem(source_location, target_location):
    '''
        To move files between on-prem locations - (for now keep to same drive).

        Parameters
        --------
        source_location: string
        target_location: string

        Returns
        --------
        True - successful
        Exception - if failed
    '''
    logger.debug('moving on-prem to on-prem file...')


    return Exception


def _move_on_prem_to_s3(source_location, target_location):
    '''
        To move files from on-prem to S3.

        S3 location must be in nonprod.

        Parameters
        --------
        source_location: string
        target_location: string

        Returns
        --------
        True - successful
        Exception - if failed
    '''
    logger.debug('moving on-prem to S3 file...')

    username, password = decrypt_KMS_credentials(os.environ['AD_username'], os.environ['AD_key'])

    # Get other parameters for on-prem
    domain = 'XXX'
    local_name = 'FileTransferService'
    server_name = 'XXXX'  # NetBIOS machine name

    # Deconstruct on-prem directory
    server_ip = source_location.split("/", 2)[0]  # IP
    root_folder = source_location.split("/", 2)[1]  # Root folder
    file_dir = source_location.split("/", 2)[2]  # Remaining file directory
    filename = source_location.split("/")[-1]  # Remaining file directory

    # Deconstruct S3 directory
    s3_bucket = target_location.split("s3://")[1]

    # Set up on-prem and S3 connections
    try:
        s3 = boto3.client('s3')
        logger.info("Connected to S3.")
    except Exception as e:
        logger.error(f"Cannot connect to S3: {str(e)}")
        return False

    conn = SMBConnection(
            username
            ,password
            ,local_name
            ,server_name
            ,domain=domain
            ,use_ntlm_v2=True
            ,is_direct_tcp=True
        )
    response = conn.connect(server_ip, 445)
    if response is False:
        logger.error('Failed to connect to on-prem server.')
        return False

    if response is True:
        logger.info("Connected to S3.")
        start_time = datetime.now()
        buffer = BytesIO()
        try:
            logger.info("Retrieving file from on-prem...")
            conn.retrieveFile(root_folder, file_dir, buffer)
        except Exception as e:
            logger.info(f"Failed to retrieve file: {str(e)}")

        try:
            buffer.seek(0)
            response = s3.upload_fileobj(buffer, s3_bucket, filename)
            buffer.close()
        except Exception as e:
            logger.info(f"Failed to write to S3: {str(e)}")

        end_time = datetime.now()
        elapsed_time = end_time - start_time
        logger.info(f"Completed transfer. Elapsed time: {elapsed_time.seconds} seconds.")
        return True
    else:
        print('Failed to connect to on-prem server.')
        return False

    


def _move_s3_to_on_prem(source_location, target_location):
    '''
        To move files between S3 to on-prem.

        S3 location must be in nonprod.

        Parameters
        --------
        source_location: string
        target_location: string

        Returns
        --------
        True - successful
        Exception - if failed
    '''
    logger.debug('moving S3 to on-prem file...')

    return Exception


def move_files(source_location, target_location):
    '''
        Uses DynamoDB as persistent event store for files processed.

        Depending on source and location, will use different internal function.
    '''
    source_type = identify_location_type(source_location)
    target_type = identify_location_type(target_location)

    if source_type == 's3' and target_type == 's3':
        _move_s3_to_s3(source_location, target_location)
    elif source_type == 's3' and target_type == 'on-prem':
        _move_s3_to_on_prem(source_location, target_location)
    elif source_type == 'on-prem' and target_type == 'on-prem':
        _move_on_prem_to_on_prem(source_location, target_location)
    elif source_type == 'on-prem' and target_type == 's3':
        _move_on_prem_to_s3(source_location, target_location)


if __name__ == "__main__":
    '''Only initiate argument parsing if invoke directly/standalone'''

    # initiate the argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="set source location")
    parser.add_argument("--target", help="set target location")

    # Parse arguments
    args = parser.parse_args()
    if args.source and args.target:
        source_location = args.source
        target_location = args.target
        logger.info(f"Moving file {source_location} to {target_location}.")
        move_files(source_location, target_location)
    else:
        raise SystemExit("Please provide source and target as --source [source] and --target [target].")