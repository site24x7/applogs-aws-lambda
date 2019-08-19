import sys, os, re, gzip, boto3, json, urllib.parse, urllib.request, traceback, datetime, calendar
from base64 import b64decode


logtype_config = json.loads(b64decode(os.environ['logTypeConfig']).decode('utf-8'))

s247_custom_regex = re.compile(logtype_config['regex']) if 'regex' in logtype_config else None

s247_ml_regex = re.compile(logtype_config['ml_regex']) if 'ml_regex' in logtype_config else None

s247_ignored_fields = logtype_config['ignoredFields'] if 'ignoredFields' in logtype_config else []

s247_tz = {'hrs': 0, 'mins': 0} #UTC

s247_datetime_format_string = logtype_config['dateFormat']

if 'unix' not in s247_datetime_format_string:
    is_year_present = True if '%y' in s247_datetime_format_string or '%Y' in s247_datetime_format_string else False
    if is_year_present is False:
        s247_datetime_format_string = s247_datetime_format_string+ ' %Y'
        
    is_timezone_present = True if ('%z' in s247_datetime_format_string or 'T' in s247_datetime_format_string) else False
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

def parse_lines(lines_read, log_group):
    removed_log_size = 0
    parsed_lines = []
    for line in lines_read:
        if line['message']:
            try:
                matcher = s247_custom_regex.search(line['message'])
                if matcher:
                    log_fields = matcher.groupdict(default='-')
                    for field_name in s247_ignored_fields:
                        removed_log_size += len(log_fields.pop(field_name, ''))
                    
                    formatted_line = {'_zl_timestamp' : get_timestamp(log_fields[logtype_config['dateField']]), 's247agentuid' : log_group}
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

def json_log_parser(lines_read, log_group):
    log_size = 0;
    parsed_lines = []
    for event_obj in lines_read:
        formatted_line = {}
        for path_obj in logtype_config['jsonPath']:
            value = get_json_value(event_obj, path_obj['key' if 'key' in path_obj else 'name'])
            if value:
                formatted_line[path_obj['name']] = value 
                log_size+= len(str(value))
        if not is_filters_matched(formatted_line):
            continue
        formatted_line['_zl_timestamp'] = get_timestamp(formatted_line[logtype_config['dateField']])
        formatted_line['s247agentuid'] = log_group
        parsed_lines.append(formatted_line)
    return parsed_lines, log_size

def ml_log_parser(lines_read, log_group):
    removed_log_size = 0
    parsed_lines = []
    for line in lines_read:
        if line['message']:
            try:
                ml_trace = ''
                ml_line = line['message'].split('\n')
                ml_data = s247_ml_regex.match(ml_line[0]).groupdict()
                if ml_data:
                    ml_trace =  '<NewLine>'+'<NewLine>'.join(ml_line[1:])
                    for matcher in re.finditer(s247_custom_regex, ml_trace):
                        log_fields = matcher.groupdict(default='-')
                        log_fields.update(ml_data)
                        for field_name in s247_ignored_fields:
                            removed_log_size += len(log_fields.pop(field_name, ''))
                        formatted_line = {'_zl_timestamp' : get_timestamp(log_fields[logtype_config['dateField']]), 's247agentuid' : log_group}
                        formatted_line.update(log_fields)
                        parsed_lines.append(formatted_line)
            except Exception as e:
                print(e)
    return parsed_lines, removed_log_size

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
    try:
        cw_data = event['awslogs']['data']
        compressed_payload = b64decode(cw_data)
        uncompressed_payload = gzip.decompress(compressed_payload)
        payload = json.loads(uncompressed_payload)

        log_group = payload['logGroup']
        log_events = payload['logEvents']
        if 'jsonPath' in logtype_config:
            parsed_lines, log_size = json_log_parser(log_events, log_group)
        elif s247_ml_regex:
            parsed_lines, removed_log_size = ml_log_parser(lines_read, log_group)
            log_size = sys.getsizeof(log_events) - removed_log_size
        else:
            parsed_lines, removed_log_size = parse_lines(log_events, log_group)
            log_size = sys.getsizeof(log_events) - removed_log_size

        if parsed_lines:
            gzipped_parsed_lines = gzip.compress(json.dumps(parsed_lines).encode())
            send_logs_to_s247(gzipped_parsed_lines, log_size)
    except Exception as e:
        print(e)
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Logs Uploaded Successfully')
    }
