
def _generate_functions(function_names):
    functions = {}
    for fname in set(function_names):
        function = eval(fname)
        if fname not in ['ifexist', 'last']:
            functions[fname] = functools.partial(function, transformer=identity(), rtype=float)
        else:
            functions[fname] = functools.partial(function, rtype=float)
    return functions

def run(t1, context_string):
    try:
        sc = SparkContext._active_spark_context
    except:
        sc = SparkContext()
    sqlContext = HiveContext(sc)

    NUM_SUBSETS_SHORT = 20
    NUM_SUBSETS_LONG = 1
    NUM_PARTITIONS = 200
    HOT_SPOT_THRESHOLD = 10000
    windows_short = [
        {'name':'10s','range':(10,0),'row':(None,None)},
        {'name':'1m','range':(60,0),'row':(None,None)},
        {'name':'5m','range':(60*5,0),'row':(None,None)},
        {'name':'30m','range':(60*30,0),'row':(None,None)},
        {'name':'2h','range':(3600*2,0),'row':(None,None)},
        {'name':'12h','range':(3600*12,0),'row':(None,None)},
        {'name':'1d','range':(3600*24,0),'row':(None,None)},
        {'name':'2d','range':(3600*24*2,0),'row':(None,None)},
        {'name':'5d','range':(3600*24*5,0),'row':(None,None)},
        ]

    windows_long = [{'name':'1mo','range':(3600*24*30,0),'row':(None,None)}]

    timeWindow_str_functions = ["ifexist","existcount","countdistinct","entropy"]
    timeWindow_num_functions = ['fsum','fmax','ifexist','existcount','countdistinct','entropy']
    timeWindow_long_functions = ["ifexist","existcount","countdistinct"]
    timeWindow_functions = ['fsum','fmax','ifexist','existcount','countdistinct','entropy']    

    order_col = "trx_time_std"
    partition_cols = ['cardno','umid']

    target_str_cols = ['cardno','umid','responsecode','amt_bin','isweekend','weekday','hour','trx_time_diff']#,'x1','x2','x3','x4','x5','x6','x7','x8','x9','x10','x11','x12','x13','x14','x15','x16','x17','x18','x19','x20']
    target_num_cols = ['amt']

    t1 = window_aggregate(t1, windows_short, _generate_functions(timeWindow_str_functions), partition_cols, target_str_cols, order_col, NUM_PARTITIONS, HOT_SPOT_THRESHOLD, NUM_SUBSETS_SHORT, sqlContext)
    t1 = window_aggregate(t1, windows_short, _generate_functions(timeWindow_num_functions), partition_cols, target_num_cols, order_col, NUM_PARTITIONS, HOT_SPOT_THRESHOLD, NUM_SUBSETS_SHORT, sqlContext)
    t1 = window_aggregate(t1, windows_long, _generate_functions(timeWindow_long_functions), partition_cols, target_str_cols, order_col, NUM_PARTITIONS, HOT_SPOT_THRESHOLD, NUM_SUBSETS_LONG, sqlContext)

    return [t1]

