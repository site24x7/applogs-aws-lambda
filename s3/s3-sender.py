import os, re, gzip, boto3, json, urllib.parse, urllib.request, traceback, datetime, calendar
from base64 import b64decode

s3 = boto3.client('s3')

logtype_config = json.loads(b64decode(os.environ['logTypeConfig']).decode('utf-8'))

s247_custom_regex = re.compile(logtype_config['regex']) if 'regex' in logtype_config else None

s247_ignored_fields = logtype_config['ignored_fields'] if 'ignored_fields' in logtype_config else []

s247_tz = {'hrs': 0, 'mins': 0} #UTC

s247_datetime_format_string = logtype_config['dateFormat']

if 'unix' not in s247_datetime_format_string:
    is_year_present = True if '%y' in s247_datetime_format_string or '%Y' in s247_datetime_format_string else False
    if is_year_present is False:
        s247_datetime_format_string = s247_datetime_format_string+ ' %Y'
        
    is_timezone_present = True if '%z' in s247_datetime_format_string else False
    if not is_timezone_present and 'timezone' in logtype_config:
        tz_value = logtype_config['timezone']
        if tz_value.startswith('+'):
            s247_tz['hrs'] = int('-' + tz_value[1:3])
            s247_tz['mins'] = int('-' + tz_value[3:5])
        elif tz_value.startswith('-'):
            s247_tz['hrs'] = int('+' + tz_value[1:3])
            s247_tz['mins'] = int('+' + tz_value[3:5])

def get_timestamp(datetime_string):
    try:
        ''' If the date value is in unix format the no need to process the date string '''
        if 'unix' in s247_datetime_format_string:
            return datetime_string+'000' if s247_datetime_format_string == 'unix' else datetime_string

        if is_year_present is False:
            from datetime import date
            year = date.today().year
            datetime_string = datetime_string + ' ' + str(year)

        ''' check added to replace +05:30 to +0530 '''
        if is_timezone_present:
            datetime_string = re.sub(r'([+-])(\d{2}):(\d{2})', r'\1\2\3', datetime_string)
            
        datetime_data = datetime.datetime.strptime(datetime_string, s247_datetime_format_string)
        
        if is_timezone_present is False:
            datetime_data += datetime.timedelta(hours=s247_tz['hrs'], minutes=s247_tz['mins'])
        timestamp = calendar.timegm(datetime_data.utctimetuple()) *1000 + int(datetime_data.microsecond/1000)
        return int(timestamp)
    except Exception as e:
        return 0

def parse_lines(lines_read, bucket_name):
    removed_log_size = 0
    parsed_lines = []
    for line in lines_read:
        line = line.decode('utf-8', 'ignore').strip()
        if line:
            try:
                matcher = s247_custom_regex.search(line)
                if matcher:
                    log_fields = matcher.groupdict(default='-')
                    for field_name in s247_ignored_fields:
                        removed_log_size += len(log_fields.pop(field_name, ''))
                    
                    formatted_line = {'_zl_timestamp' : get_timestamp(log_fields[logtype_config['dateField']]), 's247agentuid' : bucket_name}
                    formatted_line.update(log_fields)
                    parsed_lines.append(formatted_line)
            except Exception as e:
                traceback.print_exc()
    return parsed_lines, removed_log_size

def is_filters_matched(formatted_line):
    if 'filterConfig' in logtype_config:
        for config in logtype_config['filterConfig']:
            if config in formatted_line and (filter_config[config]['match'] ^ (formatted_line[config] in filter_config[config]['values'])):
                return False
    return True

def get_json_value(obj, key):
    if key in obj:
        return obj[key]
    elif '.' in key:
        parent_key = key[:key.index('.')]
        child_key = key[key.index('.')+1:]
        return get_json_value(obj[parent_key], child_key)

def json_log_parser(lines_read, bucket_name):
    log_size = 0;
    parsed_lines = []
    records_obj = json.loads(lines_read.decode('utf-8'))
    for event_obj in records_obj['Records']:
        formatted_line = {}
        for path_obj in logtype_config['jsonPath']:
            value = get_json_value(event_obj, path_obj['key' if 'key' in path_obj else 'name'])
            if value:
                formatted_line[path_obj['name']] = value 
                log_size+= len(str(value))
        if not is_filters_matched(formatted_line):
            continue
        formatted_line['_zl_timestamp'] = get_timestamp(formatted_line[logtype_config['dateField']])
        formatted_line['s247agentuid'] = bucket_name
        parsed_lines.append(formatted_line)
    return parsed_lines, log_size
    


def send_logs_to_s247(gzipped_parsed_lines, log_size):
    header_obj = {'X-DeviceKey': logtype_config['apiKey'], 'X-LogType': logtype_config['logType'],
                  'X-StreamMode' :1, 'Log-Size': log_size, 'Content-Type' : 'application/json', 'Content-Encoding' : 'gzip', 'User-Agent' : 'AWS-Lambda'
    }
    upload_url = 'https://'+logtype_config['uploadDomain']+'/upload'
    request = urllib.request.Request(upload_url, headers=header_obj)
    s247_response = urllib.request.urlopen(request, data=gzipped_parsed_lines)
    dict_responseHeaders = dict(s247_response.getheaders())
    if s247_response and s247_response.status == 200:
        print('{}:All logs are uploaded to site24x7'.format(dict_responseHeaders['x-uploadid']))
    else:
        print('{}:Problem in uploading to site24x7 status {}, Reason : {}'.format(dict_responseHeaders['x-uploadid'], s247_response.status, s247_response.read()))

def lambda_handler(event, context):
    
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("response received for key: {}".format(key))
        if response['ContentType'] =='application/x-gzip' or key.endswith('.gz'):
            lines_read = gzip.decompress(response['Body'].read())
        
        if 'jsonPath' in logtype_config:
            parsed_lines, log_size = json_log_parser(lines_read, bucket)
        else:
            parsed_lines, removed_log_size = parse_lines(lines_read.split(b'\n'), bucket)
            log_size = response['ContentLength'] - removed_log_size
        print('log_size : {}'.format(log_size))
        if parsed_lines:
            gzipped_parsed_lines = gzip.compress(json.dumps(parsed_lines).encode())
            send_logs_to_s247(gzipped_parsed_lines, log_size)
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Logs Uploaded Successfully')
    }
