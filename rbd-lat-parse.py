#!/usr/bin/python3

import babeltrace
import sys

debug = False
trace_path = '../data/good'

def debug_print(*args):
    if debug:
        print('>>>', args)

def reap(times, trace_id):
    '''
    calc timecost of each stage
    '''
    if len(times) == 6:
        other = 0
        iostack = 0
        msg = 0

        other += times['1'] - times['0']
        msg += times['2'] - times['1']
        iostack += times['3'] - times['2']
        msg += times['4'] - times['3']
        other += times['5'] - times['4']

        print(trace_id, times, "<msg, iostack, other>:", msg, iostack, other)
    else:
        debug_print(trace_id, "ERROR", times)

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
        if parent_span_id == 0 and event_name == 'init':
            collector[trace_id] = {'0' : timestamp}
        else:
            if trace_id in collector:
                if (trace_name == 'op msg' and 
                    service_name == 'Objecter' and
                    event_name == 'async enqueueing message'):
                    collector[trace_id]['1'] = timestamp
                elif trace_name == 'osd op':
                    if (event_name == 'message destructed' or
                       event_name == 'enqueue op'):
                        debug_print(trace_id, e) 
                        # primary osd op(priority = 63)
                        if priority == 63:
                            collector[trace_id]['2'] = timestamp
                elif event_name == 'sup_op_commit':
                    collector[trace_id]['3'] = timestamp
                elif event_name == 'osd op reply':
                    collector[trace_id]['4'] = timestamp
                elif (trace_name.startswith('write rbd_data') and
                     event_name == 'finish'):
                   collector[trace_id]['5'] = timestamp
           
                   # reap
                   debug_print(trace_id, collector[trace_id])
                   reap(collector[trace_id], trace_id)
                   collector.pop(trace_id)
    print (len(collector), collector)
if __name__ == '__main__':
    fire()
