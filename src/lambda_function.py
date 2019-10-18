import boto3
import sys

sys.path.append('/opt')
import pandas as pd
import io
import urllib.parse
import json
from ua_parser_next import user_agent_parser
import pyarrow as pa
import pyarrow.parquet as pq


# from s3fs import S3FileSystem
# Testing

def getCloudFrontFile(bucket, key):
    s3_client = boto3.client('s3')

    s3 = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)

    df1 = pd.read_csv(io.BytesIO(obj['Body'].read()), compression='gzip', sep='\t', skiprows=2,
                      names=['date', 'time', 'x-edge-location', 'sc-bytes', 'cs-ip', 'cs-method', 'cs-host', 'cs-uri',
                             'sc-status', 'cs-referer', 'cs-user-agent', 'cs-uri-query', 'cs-cookie',
                             'x-edge-result-type', 'x-edge-request-id', 'x-host-header', 'cs-protocol', 'cs-bytes',
                             'time-taken', 'x-forwarded-for', 'ssl-protocol', 'ssl-cipher',
                             'x-edge-response-result-type', 'cs-protocol-version', 'fle-status',
                             'fle-encrypted-fields'])
    return df1;


def writeParquet(df, filename, year, month, day):
    table = pa.Table.from_pandas(df)
    s3 = boto3.client('s3')

    file = filename.strip('.gz')

    with io.BytesIO() as f:
        pq.write_table(table, f)
        s3.put_object(Bucket='redbox-datalake-data',
                      Key='cloudfront/processed/year={0}/month={1}/day={2}/{3}.parquet'.format(year, month, day, file),
                      Body=f.getvalue(), ACL='bucket-owner-full-control')


def version(major, minor, patch):
    # make a nicely formated string for version - this handles missing values as well.
    if major is None:
        version = ''
    elif minor is None:
        version = "{}".format(major)
    elif patch is None:
        version = "{}.{}".format(major, minor)
    else:
        version = "{}.{}.{}".format(major, minor, patch)
    return version;


def processAgentForOs(user_agent):
    # obtain the OS family from the user agent.
    if user_agent.startswith('Droid'):
        os = 'Android';
    elif user_agent.startswith('iOSRB'):
        os = 'iOS';
    else:
        os = _parse_cache.get(user_agent)['os']['family']

    return os;


def processAgentAppType(user_agent):
    # obtain the Application type from the user agent.
    if user_agent.startswith('Droid'):
        app_type = 'Redbox';
    elif user_agent.startswith('iOSRB'):
        app_type = 'Redbox';
    else:
        app_type = _parse_cache.get(user_agent)['user_agent']['family']

    return app_type;


def processAgentOsVersion(user_agent):
    # obtain the OS version from the user agent.
    if user_agent.startswith('Droid'):
        try:
            os_version = user_agent.split('|')[3]
        except IndexError:
            # print(user_agent)
            os_version = '';
    elif user_agent.startswith('iOSRB'):
        # apple
        try:
            os_version = user_agent.split('|')[2]
        except IndexError:
            # print(user_agent)
            os_version = '';
    else:
        x = _parse_cache.get(user_agent)
        if x['os']['major'] is None:
            os_version = ''
        elif x['os']['minor'] is None:
            os_version = "{}".format(x['os']['major'])
        elif x['os']['patch'] is None:
            os_version = "{}.{}".format(x['os']['major'], x['os']['minor'])
        else:
            os_version = "{}.{}.{}".format(x['os']['major'], x['os']['minor'], x['os']['patch'])

    return os_version;


def processAgentModel(user_agent):
    # get the device model from the user agent
    if user_agent.startswith('Droid'):
        try:
            model = user_agent.split('|')[1]
        except IndexError:
            # print(user_agent)
            model = '';
    elif user_agent.startswith('iOSRB'):
        # apple
        try:
            model = user_agent.split('|')[1]
        except:
            model = '';
    else:
        model = _parse_cache.get(user_agent)['device']['model']

    return model;


def processAgentDeviceType(user_agent):
    # obtain the device type from the user agent.
    if user_agent.startswith('Droid'):
        try:
            device_type = user_agent.split('|')[2]
        except IndexError:
            # print(user_agent)
            device_type = '';
    elif user_agent.startswith('iOSRB'):
        # apple
        try:
            model = user_agent.split('|')[1]
            device_type = model
        except:
            device_type = '';
    else:
        device_type = _parse_cache.get(user_agent)['device']['family']

    return device_type;

def safe_parse_Driod_user_agent(user_agent):
    """
    :param user_agent:
    :return:

    Takes a user_agent string and tries to parse it
    when the parse  fails on  IndexError returns a default value.
    """
    try:
        app_version = user_agent.split('|')[0].split('/')[1]
        return app_version
    except IndexError:
        print("Exception caught for Driod user_agent '{0}'. Accepted format '{1}'".format(user_agent,
                                                                                         "DroidRBMobile/9190|SAMSUNG_SM-J327T1|Smartphone_5.0_in|N_7.0"))
        return "default"


def safe_parse_iOSRB_user_agent(user_agent):
    """
    :param user_agent:
    :return:

    Takes a user_agent string and tries to parse it
    when the parse fails catches the exception and  returns a default value.
    """
    try:
        app_version = user_agent.split('|')[3]
        return app_version
    except:
        print("Exception caught for iOSRB user_agent '{0}'. Accepted format '{1}'".format(user_agent,
                                                                                         "iOSRBMobile|iPhone|iOS13.1.2|7.37.0"))
        return ''


def processAgentAppVersion(user_agent):
    # for correct useragent this is the Browser version, for apps its the release number from redbox.
    if user_agent.startswith('Droid'):
        return safe_parse_Driod_user_agent(user_agent)
    elif user_agent.startswith('iOSRB'):
        return safe_parse_iOSRB_user_agent(user_agent)
    else:
        app_version_meta = _parse_cache.get(user_agent)['user_agent']
        app_version = version(app_version_meta['major'], app_version_meta['minor'], app_version_meta['patch'])

    return app_version;


_parse_cache = {}
_parse_cache.clear()


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    bucket = event['Records'][0]['s3']['bucket']['name']
    print("Bucket name:" + bucket)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("Key name:" + key)
    filename = key.split('/')[-1]
    distribution = filename.split('.')[0]

    if filename.find("aws-waf-logs") == -1:
        print("Processing Non WAF file")
        df1 = getCloudFrontFile(bucket, key)

        # seems we have to do this 2x to correct encoding issues.
        df1['cs-user-agent'] = df1['cs-user-agent'].apply(urllib.parse.unquote)
        df1['cs-user-agent'] = df1['cs-user-agent'].apply(urllib.parse.unquote)

        # testing found the user agent parser has an internal cache of only 20 agents and that the speed of parsing the logs was terrible, so created own cache.
        agents = df1['cs-user-agent'].unique()

        # _parse_cache = {}
        # _parse_cache.clear()

        for ua in agents:
            v = user_agent_parser.Parse(ua)
            _parse_cache[ua] = v

        df1['os'] = df1['cs-user-agent'].apply(processAgentForOs)
        df1['os-version'] = df1['cs-user-agent'].apply(processAgentOsVersion)
        df1['model'] = df1['cs-user-agent'].apply(processAgentModel)
        df1['device-type'] = df1['cs-user-agent'].apply(processAgentDeviceType)
        df1['app-type'] = df1['cs-user-agent'].apply(processAgentAppType)
        df1['app-version'] = df1['cs-user-agent'].apply(processAgentAppVersion)

        ##Kiosk - S3 folder is redbox-cloudfrontlogs-prodp/prodp/kiosk with file prefix of E35OTL44WSFEJ6
        ##Digital (OnDemand) - S3 folder is redbox-cloudfrontlogs-prodp/prodp/digital with file prefix of E3GT33MMTXYBO8
        ##Mobile - S3 folder is redbox-cloudfrontlogs-prodp/prodp/mobile with file prefix of E1LWQHLX8BWR1Z
        ##Web - - S3 folder is redbox-cloudfrontlogs-prodp/prodp/web with file prefix of E20DT9KSQOO78L

        if distribution == "E35OTL44WSFEJ6":
            cdn = "kiosk"
        elif distribution == "E3GT33MMTXYBO8":
            cdn = "digital"
        elif distribution == "E1LWQHLX8BWR1Z":
            cdn = "mobile"
        elif distribution == "E20DT9KSQOO78L":
            cdn = "web"
        elif distribution == "E3NM4S0JYJ1FS8":
            cdn = "images"
        elif distribution == "E2HOHZUAFVGDPT":
            cdn = "vizio"
        elif distribution == "E2G5Z989J9YP7Q":
            cdn = "lgwebostv"
        elif distribution == "E2DZAIRGUM65O0":
            cdn = "alexa"
        else:
            cdn = "unknown"

        fileyear = filename.split('.')[1].split('-')[0]
        filemonth = filename.split('.')[1].split('-')[1]
        fileday = filename.split('.')[1].split('-')[2]

        df1['cdn'] = cdn

        writeParquet(df1, filename, fileyear, filemonth, fileday)

    else:
        print("Discarding WAF File..")
