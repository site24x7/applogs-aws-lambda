import os, re, gzip, boto3, json, urllib.parse, urllib.request, traceback, datetime, calendar,hashlib,sys
from base64 import b64decode

s3 = boto3.client('s3')

logtype_config = json.loads(b64decode(os.environ['logTypeConfig']).decode('utf-8'))

s247_custom_regex = re.compile(logtype_config['regex']) if 'regex' in logtype_config else None

s247_ignored_fields = logtype_config['ignoredFields'] if 'ignoredFields' in logtype_config else []

s247_tz = {'hrs': 0, 'mins': 0} #UTC

s247_datetime_format_string = logtype_config['dateFormat']
s247_ml_regex = re.compile(logtype_config['ml_regex']) if 'ml_regex' in logtype_config else None
s247_ml_end_regex = re.compile(logtype_config['ml_end_regex']) if 'ml_end_regex' in logtype_config else None
s247_max_ml_count = s247_custom_regex.pattern.count('\<NewLine\>') if 'ml_regex' in logtype_config else None
s247_max_trace_line = 100
s247_datetime_regex = re.compile(logtype_config['dateRegex'])

log_size = 0

masking_config = logtype_config['maskingConfig'] if 'maskingConfig' in logtype_config else None
hashing_config = logtype_config['hashingConfig'] if 'hashingConfig' in logtype_config else None
derived_eval = logtype_config['derivedConfig'] if 'derivedConfig' in logtype_config else None

if derived_eval:
    try:
        derived_fields = {}
        for key in derived_eval:
            derived_fields[key] = []
            for values in derived_eval[key]:
                derived_fields[key].append(re.compile(values.replace('\\\\', '\\').replace('?<', '?P<')))
    except Exception as e:
        traceback.print_exc()
if masking_config:
    for key in masking_config:
        masking_config[key]["regex"] = re.compile(masking_config[key]["regex"])

if hashing_config:
    for key in hashing_config:
        hashing_config[key]["regex"] = re.compile(hashing_config[key]["regex"])

if "filterConfig" in logtype_config:
    for field in logtype_config['filterConfig']:
        temp = []
        for value in logtype_config['filterConfig'][field]['values']:
            temp.append(re.compile(value))
        logtype_config['filterConfig'][field]['values'] = '|'.join(x.pattern for x in temp)

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

def log_line_filter(formatted_line):
    if masking_config:
        apply_masking(formatted_line)
    if hashing_config:
        apply_hashing(formatted_line)
    if derived_eval:
        derivedFields(formatted_line)

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
    global log_size
    parsed_lines = []
    for line in lines_read:
        line = line.decode('utf-8', 'ignore').strip()
        if line:
            try:
                matcher = s247_custom_regex.search(line)               
                if matcher:
                    log_size += len(line)
                    log_fields = matcher.groupdict(default='-')
                    for field_name in s247_ignored_fields:
                        log_size -= len(log_fields.pop(field_name, ''))
                    formatted_line={}
                    formatted_line.update(log_fields)
                    log_line_filter(formatted_line)
                    add_message_metadata(formatted_line,bucket_name)
                    parsed_lines.append(formatted_line)               
                else:
                    is_date_present = s247_datetime_regex.search(line)
                    if is_date_present is None:
                        parsed_lines[-1][message_key] += '\n' + line
                        log_size += len(line)
            except Exception as e:
                traceback.print_exc()
    return parsed_lines, log_size

def add_message_metadata(formatted_line,bucket_name):
    formatted_line.update({'_zl_timestamp' : get_timestamp(formatted_line[logtype_config['dateField']]), 's247agentuid' : bucket_name})

def is_filters_matched(formatted_line):
    if 'filterConfig' in logtype_config:
        for config in logtype_config['filterConfig']:
            if re.findall(logtype_config['filterConfig'][config]['values'],formatted_line[config]) :
                val = True 
            else:
                val = False
            if config in formatted_line and (logtype_config['filterConfig'][config]['match'] ^ (val)):
                return False
    return True

def get_json_value(obj, key):
    if key in obj:
        return obj[key]
    elif '.' in key:
        parent_key = key[:key.index('.')]
        child_key = key[key.index('.')+1:]
        return get_json_value(obj[parent_key], child_key)

def json_log_parser(records_obj, bucket_name):
    global log_size
    parsed_lines = []
    for event_obj in records_obj['Records']:
        formatted_line = {}
        json_log_size = 0
        for path_obj in logtype_config['jsonPath']:
            value = get_json_value(event_obj, path_obj['key' if 'key' in path_obj else 'name'])
            if value:
                formatted_line[path_obj['name']] = value
                json_log_size+= len(str(value))
        if not is_filters_matched(formatted_line):
            continue
        log_size += json_log_size    
        log_line_filter(formatted_line)
        add_message_metadata(formatted_line,bucket_name)
        parsed_lines.append(formatted_line)
    return parsed_lines, log_size

def get_last_group_inregex(regex):
    for group_name in regex.groupindex:
        if regex.groupindex[group_name] == regex.groups:
            return group_name
    ''' if last group is empty then we need to get previous one'''
    for group_name in regex.groupindex:
        if regex.groupindex[group_name] == regex.groups - 1:
            return group_name

if 'jsonPath' not in logtype_config:
    message_key = get_last_group_inregex(s247_custom_regex)

def ml_regex_applier(line, ml_data,formatted_line):
    global log_size
    try:
        for matcher in re.finditer(s247_custom_regex, line):
            log_fields = matcher.groupdict(default='-')
            log_fields.update(ml_data)
            for field_name in s247_ignored_fields:
                log_size -= len(log_fields.pop(field_name, ''))
            formatted_line.update(log_fields)
    except Exception:
        traceback.print_exc()
        formatted_line = {}

def ml_log_parser(lines_read,bucket_name):
    global log_size
    log_size = 0
    parsed_lines = []
    ml_trace = ''
    ml_trace_buffer = ''
    ml_found = False
    ml_end_line_found = False
    ml_data = None
    ml_count = 0
    for line in lines_read:
        line = line.decode('utf-8', 'ignore').strip()
        if line:
            try:
                ml_start_matcher = s247_ml_regex.match(line)
                if ml_start_matcher or ml_end_line_found:
                    ml_found = ml_start_matcher
                    ml_end_line_found = False
                    formatted_line = {}
                    if len(ml_trace) > 0:
                        try:
                            log_size = log_size+len(ml_trace)
                            ml_regex_applier(ml_trace, ml_data,formatted_line)
                            if ml_trace_buffer and formatted_line:
                                formatted_line[message_key] = formatted_line[message_key] + ml_trace_buffer
                                log_size+=len(ml_trace_buffer)
                            log_line_filter(formatted_line)
                            add_message_metadata(formatted_line,bucket_name)
                            parsed_lines.append(formatted_line)
                            ml_trace = ''
                            ml_trace_buffer = ''
                            if ml_found:
                                ml_data = ml_start_matcher.groupdict()
                                log_size += len(line)
                            else:
                                ml_data = {}
                            ml_count = 0
                        except Exception as e:
                            traceback.print_exc()
                    elif ml_found:
                        log_size += len(line)
                        ml_data = ml_start_matcher.groupdict()
                elif ml_found:
                    if ml_count < s247_max_ml_count:
                        ml_trace += '<NewLine>' + line
                    elif s247_ml_end_regex and s247_ml_end_regex.match(line):
                        ml_end_line_found = True
                    elif (ml_count - s247_max_ml_count) < s247_max_trace_line:
                        ml_trace_buffer += "\n" + line
                    ml_count += 1

            except Exception as e:
                traceback.print_exc()

    if len(ml_trace) > 0:
        try:
            log_size = log_size+len(ml_trace)
            formatted_line={}
            ml_regex_applier(ml_trace, ml_data, formatted_line)
            if ml_trace_buffer and formatted_line:
                formatted_line[message_key] = formatted_line[message_key] + ml_trace_buffer
                log_size+=len(ml_trace_buffer)
            log_line_filter(formatted_line)
            add_message_metadata(formatted_line,bucket_name)
            parsed_lines.append(formatted_line)
        except Exception as e:
            traceback.print_exc()


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

def apply_masking(formatted_line):
    global log_size
    try:
        for config in masking_config:
            adjust_length = 0
            mask_regex = masking_config[config]['regex']
            if config in formatted_line:
                field_value = str(formatted_line[config])
                for matcher in re.finditer(mask_regex, field_value):
                    if matcher:
                        for i in range(mask_regex.groups):
                            matched_value = matcher.group(i + 1)
                            if matched_value:
                                start = matcher.start(i + 1)
                                end = matcher.end(i + 1)
                                if start >= 0 and end > 0:
                                    start = start - adjust_length
                                    end = end - adjust_length
                                    adjust_length += (end - start) -len(masking_config[config]['string'])
                                    field_value = field_value[:start] + masking_config[config]['string'] + field_value[end:]
                formatted_line[config] = field_value
                log_size -= adjust_length
    except Exception as e:
        traceback.print_exc()

def apply_hashing(formatted_line):
    global log_size
    try:
        for config in hashing_config:
            adjust_length = 0
            mask_regex = hashing_config[config]['regex']
            if config in formatted_line:
                field_value = str(formatted_line[config])
                for matcher in re.finditer(mask_regex, field_value):
                    if matcher:
                        for i in range(mask_regex.groups):
                            matched_value = matcher.group(i + 1)
                            if matched_value:
                                start = matcher.start(i + 1)
                                end = matcher.end(i + 1)
                                if start >= 0 and end > 0:
                                    start = start - adjust_length
                                    end = end - adjust_length
                                    hash_string = hashlib.sha256(matched_value.encode('utf-8')).hexdigest()
                                    adjust_length += (end - start) - len(hash_string)
                                    field_value = field_value[:start] + hash_string + field_value[end:]
                formatted_line[config] = field_value
                log_size -= adjust_length
    except Exception as e:
        traceback.print_exc()

def derivedFields(formatted_line):
    global log_size
    try:
        for items in derived_fields:
            for each in derived_fields[items]:
                if items in formatted_line:
                    match_derived = each.search(formatted_line[items])
                    if match_derived:
                        match_derived_field = match_derived.groupdict(default='-')
                        formatted_line.update(match_derived_field)
                        for field_name in match_derived_field:
                            log_size += len(formatted_line[field_name])
                        break
    except Exception as e:
        traceback.print_exc()

def lambda_handler(event, context):
    global log_size
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("response received for key: {}".format(key))
        if response['ContentType'] =='application/x-gzip' or key.endswith('.gz'):
            lines_read = gzip.decompress(response['Body'].read())
        else:
            lines_read = response['Body'].read()

        if 'jsonPath' in logtype_config:
            try:
                records_obj = json.loads(lines_read.decode('utf-8'))
            except Exception as e:
                lines_read = lines_read.split(b'\n')
                records_obj = {"Records" :[]}
                for each in lines_read:
                    if each:
                        records_obj["Records"].append(json.loads(each))

            parsed_lines, log_size = json_log_parser(records_obj, bucket)
        elif s247_ml_regex:
            parsed_lines, log_size = ml_log_parser(lines_read.split(b'\n'), bucket)
        else:
            parsed_lines, log_size = parse_lines(lines_read.split(b'\n'), bucket)

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
