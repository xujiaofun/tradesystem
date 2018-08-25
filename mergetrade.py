import sys,os,msvcrt

def join(in_filenames, out_filename):  
    out_file = open(out_filename, 'w+')  

    err_files = []  
    for file in in_filenames:  
        try:  
            in_file = open(file, 'r')  
            out_file.write(in_file.read())  
            out_file.write('\n\n')  
            in_file.close()  
        except IOError:  
            print 'error joining', file  
            err_files.append(file)  
    out_file.close()  

    print 'joining completed. %d file(s) missed.' % len(err_files)  

    print 'output file:', out_filename

join([   
        'trade/main.py', 
        'trade/baserule.py', 
        'trade/adjust_condition_rules.py', 
        'trade/adjust_position_rules.py', 
        'trade/filter_query_rules.py', 
        'trade/filter_stock_rules.py', 
        'trade/position_control_var.py',
        'trade/tradelib.py'], 
    'trade_all.py')