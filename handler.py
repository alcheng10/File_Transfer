#######################################################################################
# Lambda function that spins-up EC2 instance to handle on-prem to/from S3 transfers.
#######################################################################################
import boto3
import os
from datetime import datetime
import logging
from base64 import b64decode
import file_transfer

region = 'ap-southeast-2'
ec2 = boto3.client('ec2', region_name=region)

# Using the Python built-in logger function 
# Auto creates a timestamp and request ID for every log entry
# Uses std out (ie print) as output for logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Suppress the more verbose modules
logging.getLogger('botocore').setLevel(logging.WARN)
logging.getLogger('urllib3').setLevel(logging.WARN)


def create_s3_onprem_bootstrap_script(source_location, target_location, python_handler):
    '''
        Creates the Bash script that will execute upon Linux instance launch.

        Bash script will move all files specified in S3 path to on-prem path.
    
        Parameters
        -------
        source_location: string
            S3 location of source
        target_location: string 
            URI of on-prem network drive
        python_handler: string 
            name of python handler file (e.g. main.py)

        Returns
        userdata: string
            Bash script in string form
    '''
    username, password= decrypt_KMS_credentials[os.environ['AD_username'], os.environ['AD_key']]
    
    bash_bootstrap_script = f"""
    #!/bin/bash 
    sudo su;
    
    # Create mounted directory
    mounted_directory=/mnt/drive;
    mkdir $mounted_directory;
    cd $mounted_directory;
    echo "//{target_location}  $mounted_directory  cifs  username={username},password={password},dir_mode=0777,file_mode=0777,noperm  0  0" >> /etc/fstab;
    mount -a;
    
    #Update to Python3
    yum install python36 -y;
    pip install -r requirements.txt;

    #Create log file
    mkdir /log;
    filename=`date "+%Y%m%d-%H%M"`_log.out;
    echo `date` > /log/$filename;
    
    #Execute file transfer
    #python3 {python_handler} --source {source_location} --target $mounted_directory > /log/$filename;
    aws s3 cp /log/$filename s3://bucket-test/ec2-log/;
    
    #Self-terminate
    shutdown -h now;
    """
    return bash_bootstrap_script


def create_EC2(source_location, target_location, python_handler):
    '''
        Creates an EC2 with mounting using the specified source and target locations.

        Deploys Python package to the EC2.

        Parameters
        ----
        source_location: string
        target_location: string
        python_handler: string
            name of python function in package.

        Returns
        -----
        ec2_id: string
            Id of EC2 instance
    '''
    userdata = create_s3_onprem_bootstrap_script(source_location, target_location, python_handler)

    ec2_response = ec2.run_instances(
        ImageId='ami-07cc15c3ba6f8e287',  # Amazon Linux (ami-07cc15c3ba6f8e287)
        IamInstanceProfile={'Name': 'nonprod-dataanalytics-filescheduler-ec2'},
        InstanceInitiatedShutdownBehavior='terminate',
        MinCount=1,
        MaxCount=1,
        KeyName=os.environ['EC2_PEM_KEY'],
        InstanceType=os.environ['EC2_INSTANCE_TYPE'],
        SecurityGroupIds=[os.environ['SECURITY_GROUP']], 
        SubnetId=os.environ['VPC_SUBNET'], 
        UserData=userdata,
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'Name',
                        'Value': f'nonprod-dataanalytics-filescheduler-ec2-{datetime.now()}'
                    },
                    {
                        'Key': 'squad',
                        'Value': 'ninja'
                    },
                    {
                        'Key': 'platform',
                        'Value': 'dataanalytics'
                    }
                ]
            }
        ]
    )
    ec2_id = ec2_response['Instances'][0]['InstanceId']
    logging.info(f"EC2 instance created - now executing in EC2 {ec2_id}.")

    return ec2_id


def lambda_handler(event, context):
    '''
        Lambda function that turns off Matillion EC2 based on event source.

        Parameters
        ---------
        event: dict
        Source event - either CloudWatch cronjob or AWS SQS
        context: dict
        Lambda context

        Event must conform as:
            source_location: s3 or on-prem location
            target_location: s3 or on-prem location
            Location format must be either s3://[bucket name] or XX.XX.XX.XX/[Name of directory]

        Returns
        ----------
        response: dict
        AWS API Gateway compliant format, with statusCode etc.
    '''
    target_type = file_transfer.identify_location_type(event['target_location'])
    target_location = event['target_location']
    source_type = file_transfer.identify_location_type(event['source_location'])
    source_location = event['source_location']
    python_handler = os.environ['PYTHON_HANDLER']

    message = ''

    if source_type == 's3' and target_type == 's3':
        message += 'AWS transfer only - not required.'
    elif source_type == 'on-prem' or target_type == 'on-prem':
        ec2_id = create_EC2(source_location, target_location, python_handler)
        message = f'EC2 file transfer spun up. instance id is {ec2_id}'

    # Set up API Gateway response template
    response = {
        "statusCode": 200,
        "body": f"File transfer started. {message}"
    }

    return response

# Standalone - execute handler with dummy event and env variables
if __name__ == "__main__":
    event = {
        "source_location": "s3://bucket-test",
        "target_location": "10.10.10.100/Matillion_Output"
    }
    response = lambda_handler(event, None)
    print(response)