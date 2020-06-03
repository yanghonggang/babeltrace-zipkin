#!/usr/bin/python3

import babeltrace
import sys

debug = False
trace_path = '../data/good'

def debug_print(*args):
    if debug:
        print('>>>', args)

def reap(times, span_id):
    '''
    calc timecost of each stage
    '''
    if len(times) == 6:
        txc_prepare = 0
        fill_read = 0
        sync_db = 0
        other = 0

        txc_prepare += times['1'] - times['0']
        fill_read += times['2'] - times['1']
        other += times['3'] - times['2']
        sync_db += times['4'] - times['3']
        other += times['5'] - times['4']

        print(span_id, times, "<txc, read, sync_db, other>:",
              txc_prepare, fill_read, sync_db, other)
    else:
        debug_print(span_id, "ERROR", times)

def fire():
    trace_collection = babeltrace.TraceCollection()
    trace_collection.add_traces_recursive(trace_path, 'ctf')
    
    collector = {}
    for event in trace_collection.events:
        #Extract event information
        name = event.name
        span_id = int(event["span_id"])
        trace_id = int(event["trace_id"])
        parent_span_id = int(event["parent_span_id"])
        port = event["port_no"]
        trace_name = event["trace_name"]
        service_name = event["service_name"]
        ip = event["ip"]
        #Use CS as default value for Zipkin annotations
        value = "cs"
        priority = 0
        if "core_annotation" in event:
            value =  event["core_annotation"]
        if "event" in event:
            event_name = event["event"]
        timestamp = int(str(event.timestamp)[:-3])
    
        if 'val' in event:
            priority = event['val']
        e = (span_id, parent_span_id, trace_id, trace_name, service_name, ip, 
               port, timestamp, event_name, priority)
        debug_print(e)
        if event_name == 'txc create':
            collector[span_id] = {'0' : timestamp}
        else:
            if span_id in collector:
                if (event_name == 'txc encode finished' and
                    '1' not in collector[span_id]):
                    collector[span_id]['1'] = timestamp
                elif (event_name == 'io_done' and
                      '2' not in collector[span_id]):
                    collector[span_id]['2'] = timestamp
                elif (event_name == 'kv_submitted' and
                      '3' not in collector[span_id]):
                    collector[span_id]['3'] = timestamp
                elif (event_name == 'db sync submit' and
                      '4' not in collector[span_id]):
                    collector[span_id]['4'] = timestamp
                elif (event_name == 'kv_done' and
                      '5' not in collector[span_id]):
                   collector[span_id]['5'] = timestamp
           
                   # reap
                   debug_print(span_id, collector[span_id])
                   reap(collector[span_id], span_id)
                   collector.pop(span_id)
    print (len(collector), collector)
if __name__ == '__main__':
    fire()
