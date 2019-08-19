import sys, os, re, gzip, json, urllib.parse, urllib.request, traceback, datetime, calendar
from base64 import b64decode
import xml.etree.ElementTree as ET

logtype_config = json.loads(b64decode(os.environ['logTypeConfig']).decode('utf-8'))

s247_ignored_fields = logtype_config['ignoredFields'] if 'ignoredFields' in logtype_config else []


def parse_events(log_events, log_group):
    log_size = 0
    parsed_lines = []
    for event in log_events:
        if event['message']:
            try:
                formatted_line = parse_event_messge(event['message'])
                if formatted_line and is_filters_matched(formatted_line):
                    if s247_ignored_fields:
                        for field_name in s247_ignored_fields:
                            formatted_line.pop(field_name)
                    log_size += sum([len(str(val)) for val in formatted_line.values()])
                    formatted_line['s247agentuid'] = log_group
                    parsed_lines.append(formatted_line)
            except Exception as e:
                traceback.print_exc()
    return parsed_lines, log_size

def get_namespace(element):
    m = re.match('\{.*\}', element.tag)
    return m.group(0) if m else ''

def parse_event_messge(event_message):
    try:
        event_message = event_message[:event_message.rindex('>')+1]
        root = ET.fromstring(event_message)
        name_space = get_namespace(root)
        root_name = root.tag
        message_props = {}
        system_node = root.find('./'+name_space+'System')
        message_props['EventId'] = int(system_node.find(name_space+'EventID').text)
        message_props['Type'] = system_node.find(name_space+'Channel').text
        message_props['Source'] = system_node.find(name_space+'Provider').get('Name')
        message_props['ComputerName'] = system_node.find(name_space+'Computer').text
        matched_datetime_string = system_node.find(name_space+'TimeCreated').get('SystemTime')[:-4]+'Z'
        datetime_data = datetime.datetime.strptime(matched_datetime_string, '%Y-%m-%dT%H:%M:%S.%fZ')
        message_props['_zl_timestamp'] = (calendar.timegm(datetime_data.utctimetuple()) *1000 + int(datetime_data.microsecond/1000))
        rendering_info_node =  root.find('./'+name_space+'RenderingInfo')
        if  rendering_info_node:
            message_props['Level'] = rendering_info_node.find(name_space+'Level').text
            message_props['TaskCategory'] = rendering_info_node.find(name_space+'Task').text
            message_props['Message'] = rendering_info_node.find(name_space+'Message').text
        return message_props
    except Exception as e:
        traceback.print_exc()
        print(event_message)
    return None

def is_filters_matched(formatted_line):
    if 'filterConfig' in logtype_config:
        for config in logtype_config['filterConfig']:
            if config in formatted_line and (filter_config[config]['match'] ^ (formatted_line[config] in filter_config[config]['values'])):
                return False
    return True

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
