#!/usr/bin/python

"""
Runstring:
%(scriptName)s --commands \"threadname,timeout_in_seconds,command_to_run%%threadname,timeout_in_seconds,command_to_run%%...\"

Examples:

%(scriptName)s --commands "Google_thread,5,wget http://www.google.com%%Yahoo_thread,5,wget http://www.yahoo.com" ')
%(scriptName)s --commands "Google_thread,5,wget http://www.google.com%%search_for_import,10,find / -name "*.py" -exec grep -n import {} /dev/null \; "')

Module:

You can also launch threads from our launch_threads function.  Here is an example:

%(scriptName)s --func_example

"""

import sys
import os,re
from logging_wrappers import reportError, debug_option, srcLineNum
import logging

import threading
# from Queue import Queue
from multiprocessing import Process, Queue
import time

import getopt


scriptDir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(scriptDir)

scriptName = os.path.basename(os.path.realpath(__file__)).replace('.pyc', '.py')


#=========================================

global q
q = Queue()

def thread_task(threadname, q, func, func_args=None):
    logging.debug("Entered")
    logging.debug("thread_task running.  os.getpid() = " + str(os.getpid()))
    logging.debug("thread_task: Before func()")
    if func_args == None:
        rc, results = func(threadname)
    else:
        rc, results = func(threadname, func_args)
    logging.debug("thread_task: Returned from func()")
    if rc != 0:
        results_formatted = reportError("In thread_task() for func.  Results: " + results, mode="return_msg_only")
    else:
        results_formatted = results
    q.put([threadname, rc, results_formatted])
    logging.debug("Adding result to queue.  From function " + str(func) + ".  rc = " + str(rc) + ".  New q.qsize = " + str(q.qsize()) + ".  results = " + results_formatted)
    logging.debug("Exited")
    return

#=========================================

global thread_dict
thread_dict = {}

def launch_threads(func_dict=None):
    logging.debug("46, here")
    # global debug
    debug = debug_option(__file__ + "," + srcLineNum())
    if debug:
        # logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s',)
        logging.basicConfig(level=logging.DEBUG, format=('%(asctime)s %(levelname)s (%(threadName)-10s) %(funcName)s %(message)s'))

    logging.debug("Entered")

    '''
    if commands_string != '':
       func_dict = {}
       for entry in commands_string.split('%'):
          threadname, timeout, command = entry.split(',',2)
          func_dict[threadname] = {'func':run_command(command), 'timeout':timeout}
    elif func_dict != None and len(func_dict) > 0:
       pass
    else:
    '''

    if func_dict == None:
        logging.error("ERROR: No commands found to run with launched threads.")
        return 1, ""

    logging.debug("main task running.  os.getpid() = " + str(os.getpid()))

    # results_list = []
    # results_list_index = -1

    for key,value in dict.items(func_dict):
        logging.debug("threads entry: " + key)
        threadname = key
        # results_list.append("placeholder")  # Placeholder
        # results_list_index += 1
        # resp_all += "threadname:" + threadname + "\n"
        # Queses don't work with multiprocessing module.
        # tid = Process(name=threadname, target=thread_task, args=(results_list_index, str(command), timeout))
        # tid = Process(name=threadname, target=thread_task, args=(results_list, str(command), timeout))

        # print 83, threadname
        # global threadname
        logging.debug("86 " + threadname)
        func_args = func_dict[threadname].get('args', None)
        tid = Process(name=threadname, target=thread_task, args=(threadname, q, func_dict[threadname]['func'], func_args))
        # tid = threading.Thread(name=threadname, target=thread_task, args=(q, threadname,  str(command), timeout))
        logging.debug(scriptName + ":" + threadname + ": tid = " + str(tid))
        thread_dict[threadname] = tid
        logging.debug(scriptName + ":" + threadname + ": Creating new thread_dict index " + str(len(thread_dict) - 1) + " for new thread " + str(tid))
        tid.start()
        logging.debug(scriptName + ":" + threadname + ": Thread " + str(tid) + " started")

        # logging.debug(scriptName + ":" + threadname + ": Number of results " + str(len(results_list)))
        # for row in results_list:
        #    logging.debug(scriptName + ":" + threadname + ": row = " + row)

        # if retcode != 0:
        #    resp_all += "ERROR: ssh.py error."
        #    resp_all += ("%d, %s" % (retcode, resp))
        #    continue
        # if resp == '':
        #    continue
        # print resp
        '''
        for line in resp.splitlines():
           # print "line: " + line
           # print resp
           if re.search('retval:', line) != None:
              continue
           # print '[' + user + '@' + threadname
           if re.search('\\[' + user + '@' + threadname, line) != None:
              continue
           # print line
        '''
        # resp_all += resp + "\n"

    overall_rc = 0
    threads_killed = 0
    max_retries = 10
    attempts = {}
    return_output = ''
    finished = False
    while finished == False:
        if len(attempts) > 0:
            finished = True
            for key, value in dict.items(attempts):
                if attempts[key] <= max_retries :
                    finished = False
                    break
                else:
                    logging.info("Thread " + key + ": Max retries reached.")
            if finished == True:
                logging.info("Max retries for all threads reached.  Aborting.")
                break

        logging.debug("Check the status of each thread")
        finished = True
        for key, value in dict.items(thread_dict):
            if not key in attempts:
                attempts[key] = 0
            attempts[key] += 1
            logging.debug(key + ": Attempt " + str(attempts[key]) + " of " + str(max_retries ))
            logging.debug("Is thread alive?  Join it: " + str(value))
            value.join(5)
            logging.debug("After join")
            if value.is_alive():
                logging.debug("Thread " + key + " still alive.  Status = " + str(value))
                finished = False
                continue
            else:
                logging.debug("Thread stopped = " + key)
                # logging.debug(scriptName + ":" + threadname + ": Number of results " + str(len(results_list)))
                logging.debug("Terminate/kill thread = " + key)
                value.terminate()
                if q.empty():
                    logging.debug("Thread " + key + " stopped but no result yet : " + str(value))
                    time.sleep(3)
                    finished = False
                    continue

        logging.debug("q.qsize = " + str(q.qsize()))
        logging.debug("Check the queue for user command output from the thread")
        while q.qsize() > 0:
            try:
                (threadname, rc, q_obj_content) = q.get_nowait()
            except q.empty():
                logging.debug("Thread " + threadname + " stopped, not q.empty(), but q.get_nowait() returns Empty, try a few more times" )
                time.sleep(3)
                finished = False
                continue

            logging.debug("Obtained threadname from queued results: " + threadname )
            if rc != 0:
                overall_rc = rc
            logging.debug("thread_result_from_queue: threadname:" + threadname + '.  rc = ' + str(rc) + '.  results = ' + q_obj_content)
            return_output += "\nthreadname:" + threadname + '\n' + q_obj_content

            logging.debug("Successfully retrieved queued messsage from thread so reset retries counter to 0 .")
            attempts[threadname] = 0

            if re.search('^sleep', q_obj_content):
                continue

            logging.debug("Remove " + threadname + " from thread_dict" )
            thread_dict.pop(threadname, None)
            threads_killed += 1

    # logging.debug("resp_all = " + resp_all)

    # logging.debug(scriptName + ":" + threadname + ": Before collecting results: Number of results " + str(len(results_list)))
    # return_output = ''
    # for row in results_list:
    #    return_output += row

    logging.debug("Final report:" )
    for key, value in dict.items(thread_dict):
        logging.error("Thread " + key + " still in thread_dict" )
        if value.is_alive():
            logging.debug("Thread " + key + " still alive.  Status = " + str(value))
        logging.debug("Terminate/kill thread = " + key)
        value.terminate()
        if value.is_alive():
            logging.debug("ERROR: Thread not killed = " + key)

    logging.debug("Threads killed = " + str(threads_killed))

    logging.debug("return_output = vvvvvvvvvv\n" + return_output + "\nend of return_output ^^^^^^^^^^^^^\n")
    logging.debug("Exited")
    if overall_rc != 0:
        return overall_rc, reportError("In launch_threads().  Results: " + return_output, mode="return_msg_only")
    else:
        return 0, return_output

#=========================================

def func_example_test(threadname, args):
    # print "219 " + threadname
    # logging.debug("219 " + threadname + ": " + str(args))
    return 0, "219 " + threadname + ": " + str(args)

def func_example():
    # Create and launch
    func_dict = {}
    func_dict['example_thread1'] = {'func':func_example_test, 'args':'string_value', 'timeout':5}
    func_dict['example_thread2'] = {'func':func_example_test, 'args':12, 'timeout':5}
    func_dict['example_thread3'] = {'func':func_example_test, 'args':[1, 2, 3], 'timeout':5}
    rc, all_results = launch_threads(func_dict=func_dict)
    return rc, all_results

#=========================================

def usage():
    print(__doc__ % {'scriptName' : scriptName, })


#=========================================


if __name__ == '__main__':
    # global results_list

    if len(sys.argv)< 2:
        usage()
        sys.exit(-1)

    commands_string = sys.argv[1]

    # global debug
    debug = debug_option(__file__ + "," + srcLineNum())
    if debug:
        # logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s',)
        logging.basicConfig(level=logging.DEBUG, format=('%(asctime)s %(levelname)s (%(threadName)-10s) %(funcName)s %(message)s'))

    try:
        opts, args = getopt.getopt(sys.argv[1:], "", ["strings=", "func_example"])
    except getopt.GetoptError as err:
        reportError("ERROR: Unrecognized runstring " + str(err))
        usage()

    commands_string = ''
    functions_string = ''
    func_dict = {}

    for opt, arg in opts:
        if opt == "--strings":
            commands_string = arg
        elif opt == "--func_example":
            rc, all_results = func_example()
        else:
            print("ERROR: Unrecognized runstring option: " + opt)
            usage()


    if commands_string != '':
        # global all_results
        all_results = ''
        try:
            if commands_string != '':
                rc, all_results = launch_threads(commands_string=commands_string)
            else:
                rc, all_results = func_example()
        except (KeyboardInterrupt, SystemExit):
            print("Signal trapped")
            logging.debug("Signal caught")
            for key, value in dict.items(thread_dict):
                logging.debug("Kill thread = " + key)
                value.terminate()
                if value.is_alive():
                    logging.debug("ERROR: Thread not killed = " + key)
                    continue
                else:
                    logging.debug("Thread killed = " + key)
                    thread_dict.pop(key, None)
            # sys.exit(1)

        print("Runstring: " + commands_string)

    # print all_results
    print("======================================================================")
    logging.debug("all_results = " + all_results)
    for line in all_results.splitlines():
        logging.debug("output = " + line)
        if re.search('\\[[^@]*@', line) != None:
            continue
        if re.search('threadname:', line) != None:
            print("--------------------------------------------")
        print(line)
