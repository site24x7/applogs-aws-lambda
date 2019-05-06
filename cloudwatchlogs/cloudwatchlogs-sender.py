import os, gzip, json, urllib.request, traceback
from base64 import b64decode

logtype_config = json.loads(b64decode(os.environ['logTypeConfig']).decode('utf-8'))

def is_filters_matched(formatted_line):
    if 'filterConfig' in logtype_config:
        for config in logtype_config['filterConfig']:
            if config in formatted_line and (filter_config[config]['match'] ^ (formatted_line[config] in filter_config[config]['values'])):
                return False
    return True

def parse_events(log_events, log_group):
    log_size = 0;
    parsed_lines = []
    formatted_line = {}
    for log_event in log_events:
        for path_obj in logtype_config['jsonPath']:
            value = log_event[path_obj['key' if 'key' in path_obj else 'name']]
            if value:
                formatted_line[path_obj['name']] = value 
                log_size+= len(str(value))
        if not is_filters_matched(formatted_line):
            continue
        formatted_line['_zl_timestamp'] = logtype_config['dateField']
        formatted_line['s247agentuid'] = log_group
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
    try:
        cw_data = event['awslogs']['data']
        compressed_payload = b64decode(cw_data)
        uncompressed_payload = gzip.decompress(compressed_payload)
        payload = json.loads(uncompressed_payload)

        log_group = payload['logGroup']
        log_events = payload['logEvents']
        parsed_lines, log_size = parse_events(log_events, log_group)
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
