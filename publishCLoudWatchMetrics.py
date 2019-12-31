#!/bin/python
import requests
import csvmapper
import json
import boto3


metrics_list = ['pxname','svname','qcur','qmax','scur','smax','slim','stot','bin','bout','dreq','dresp','ereq','econ','eresp','wretr','wredis','status','weight','act','bck','chkfail','chkdown','lastchg','downtime','qlimit','pid','iid','sid','throttle','lbtot','tracked','type','rate','rate_lim','rate_max','check_status','check_code','check_duration','hrsp_1xx','hrsp_2xx','hrsp_3xx','hrsp_4xx','hrsp_5xx','hrsp_other','hanafail','req_rate','req_rate_max','req_tot','cli_abrt','srv_abrt','comp_in','comp_out','comp_byp','comp_rsp','lastsess','last_chk','last_agt','qtime','ctime','rtime','ttime','agent_status','agent_code','agent_duration','check_desc','agent_desc','check_rise','check_fall','check_health','agent_rise','agent_fall','agent_health','addr','cookie','mode','algo','conn_rate','conn_rate_max','conn_tot','intercepted','dcon','dses']
# Create CloudWatch client
cloudwatch = boto3.client('cloudwatch')

class dataObject:
    def __init__(self, pxname, svname, qcur, qmax, scur, smax, slim, stot, bin, bout, dreq, dresp, ereq, econ, eresp, wretr, wredis, status, weight, act, bck, chkfail, chkdown, lastchg, downtime, qlimit, throttle, lbtot):
        self.pxname = pxname
        self.svname = svname
        self.qcur = qcur
        self.qmax = qmax
        self.scur = scur
        self.smax = smax
        self.slim = slim
        self.stot = stot
        self.bin = bin
        self.bout = bout
        self.dreq = dreq
        self.dresp = dresp
        self.ereq = ereq
        self.econ = econ
        self.eresp = eresp
        self.wretr = wretr
        self.wredis = wredis
        self.status = status
        self.weight = weight
        self.act = act
        self.bck = bck
        self.chkfail = chkfail
        self.chkdown = chkdown
        self.lastchg = lastchg
        self.downtime = downtime
        self.qlimit = qlimit
        self.throttle = throttle
        self.lbtot = lbtot


csv_data = requests.get('http(s)://url/(uri);csv',
                        auth=('user', 'password'))
with open('data.csv', 'w') as f:
    # f.seek(0)
    f.write(csv_data.text)
    f.close()

mapper = csvmapper.DictMapper([
    [
        {'name': 'pxname'}, {'name': 'svname'}, {'name': 'qcur'}, {'name': 'qmax'}, {'name': 'scur'}, {'name': 'smax'}, {'name': 'slim'}, {'name': 'stot'}, {'name': 'bin'}, {'name': 'bout'}, {'name': 'dreq'}, {'name': 'dresp'}, {'name': 'ereq'}, {'name': 'econ'}, {'name': 'eresp'}, {'name': 'wretr'}, {'name': 'wredis'}, {'name': 'status'}, {'name': 'weight'}, {'name': 'act'}, {'name': 'bck'}, {'name': 'chkfail'}, {'name': 'chkdown'}, {'name': 'lastchg'}, {'name': 'downtime'}, {'name': 'qlimit'}, {'name': 'pid'}, {'name': 'iid'}, {'name': 'sid'}, {'name': 'throttle'}, {'name': 'lbtot'}, {'name': 'tracked'}, {'name': 'type'}, {'name': 'rate'}, {'name': 'rate_lim'}, {'name': 'rate_max'}, {'name': 'check_status'}, {'name': 'check_code'}, {'name': 'check_duration'}, {'name': 'hrsp_1xx'}, {'name': 'hrsp_2xx'}, {'name': 'hrsp_3xx'}, {'name': 'hrsp_4xx'}, {'name': 'hrsp_5xx'}, {
            'name': 'hrsp_other'}, {'name': 'hanafail'}, {'name': 'req_rate'}, {'name': 'req_rate_max'}, {'name': 'req_tot'}, {'name': 'cli_abrt'}, {'name': 'srv_abrt'}, {'name': 'comp_in'}, {'name': 'comp_out'}, {'name': 'comp_byp'}, {'name': 'comp_rsp'}, {'name': 'lastsess'}, {'name': 'last_chk'}, {'name': 'last_agt'}, {'name': 'qtime'}, {'name': 'ctime'}, {'name': 'rtime'}, {'name': 'ttime'}, {'name': 'agent_status'}, {'name': 'agent_code'}, {'name': 'agent_duration'}, {'name': 'check_desc'}, {'name': 'agent_desc'}, {'name': 'check_rise'}, {'name': 'check_fall'}, {'name': 'check_health'}, {'name': 'agent_rise'}, {'name': 'agent_fall'}, {'name': 'agent_health'}, {'name': 'addr'}, {'name': 'cookie'}, {'name': 'mode'}, {'name': 'algo'}, {'name': 'conn_rate'}, {'name': 'conn_rate_max'}, {'name': 'conn_tot'}, {'name': 'intercepted'}, {'name': 'dcon'}, {'name': 'dses'}
    ]])
parser = csvmapper.CSVParser('data.csv', mapper)
converter = csvmapper.JSONConverter(parser)
string_data = converter.doConvert(pretty=True)
json_data = json.loads(str(string_data))

# print(len(metrics_list))
for metric_item in json_data:
    metric = dataObject(pxname= metric_item['pxname'] , svname= metric_item['svname'] , qcur= metric_item['qcur'] , qmax= metric_item['qmax'] , scur= metric_item['scur'] , smax= metric_item['smax'] , slim= metric_item['slim'] , stot= metric_item['stot'] , bin= metric_item['bin'] , bout= metric_item['bout'] , dreq= metric_item['dreq'] , dresp= metric_item['dresp'] , ereq= metric_item['ereq'] , econ= metric_item['econ'] , eresp= metric_item['eresp'] , wretr= metric_item['wretr'] , wredis= metric_item['wredis'] , status= metric_item['status'] , weight= metric_item['weight'] , act= metric_item['act'] , bck= metric_item['bck'] , chkfail= metric_item['chkfail'] , chkdown= metric_item['chkdown'] , lastchg= metric_item['lastchg'] , downtime= metric_item['downtime'] , qlimit= metric_item['qlimit'] , throttle= metric_item['throttle'] , lbtot= metric_item['lbtot'])
    if metric_item['pxname'] == '# pxname':
        continue
    # print('===================================================================')
    # print('downtime: {}'.format(metric_item['downtime']))
    for i in range(0,len(metrics_list)):
        key = metrics_list[i].encode("utf-8")
        value = metric_item[metrics_list[i]]
        if key in ['pxname','svname','check_status','check_desc','agent_desc','mode'] or value == '':
            continue
        
        elif key == 'status':
            if value == 'DOWN':
                value = float(-1)
            elif value == 'MAINT':
                value = float(0)
            elif value == 'UP' or value == 'OPEN':
                value = float(1)

        try:
            cloudwatch.put_metric_data(
                MetricData=[
                    {
                        'MetricName': key,
                        'Dimensions': [
                            {
                                'Name': 'Services Stats',
                                'Value': '{}-{}'.format(metric_item['pxname'],metric_item['svname'])
                            },
                        ],
                        'Unit': 'None',
                        'Value':float(value)
                        
                    },
                ],
                Namespace='HAProxy/Stats')
            # print('convertable key {}, value is: {}'.format(key, value))

        except ValueError:
            # print('not convertable key {}, value is: {}'.format(key, value))
            continue