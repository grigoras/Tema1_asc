#!/usr/bin/python

"""
    Testing infrastructure - generates and executes tests, 
    simulates a cluster's FEPs (front-end processors)

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2014
"""

import random
import sys
import os
import re
import getopt
from threading import *

from supervisor import Supervisor
from node import Node
from datastore import Datastore
from util import *

#DEBUG = True
DEBUG = False


class Tester:   
    """ Generates test scenarios and simulates the users and front-end-processors
        of a cluster, which send jobs to the computational and storage nodes.
    """
    def __init__(self, output_filename):
        """
            Constructor.
            @type output_filename: String
            @param output_filename: the file in which the tester logs results
        """
        self.output_filename = output_filename
        
        self.test_generator = TestGenerator()    
        
        self.result_lock = Lock()
        self.result = {}
        
        self.passed_tests = 0
        self.bonus = None
        
        self.finished_output_lock = Lock()
        self.finished_output = False       
 
    def run_test(self, testfile = None, times = 1):
        """
            Performs a test generated from a given file or randomly.
            To better check for synchronization errors the test is run several
            times, as given by the 'times' parameter. 
            @type testfile: String
            @param testfile: the path+name of the file containing the test
                            parameters
            @type times: Integer
            @param times: number of time to run the test
        """
        if not testfile:
            test = self.test_generator.generate_random_test()
        else:
            test = self.test_generator.load_test(testfile)

        t = Timer(interval=test[TIMEOUT_PERIOD], 
                    function=Tester.timer_fn, 
                    args=[self, test, times])
        t.start()

        print start_test_msg % test[TEST_NAME]

        for i in range(times):
            self.start_test(test, i + 1, times, t)

        print end_test_msg % test[TEST_NAME]

        t.cancel()

        with self.finished_output_lock:
            if not self.finished_output:
                msg = test_finished_msg % (test[TEST_NAME], 
                                            100.0*self.passed_tests/times)
                if test[BONUS] and self.bonus:
                    msg = msg + test_bonus_msg % 5
                out_file = open(self.output_filename, "a")
                out_file.write(msg + "\n")
                out_file.close()
                self.finished_output = True 

    def timer_fn(self, test, num_times):
        with self.finished_output_lock:
            if not self.finished_output:
                if self.passed_tests == num_times:
                    msg = test_finished_msg % (test[TEST_NAME], 
                                                100.0*self.passed_tests/times)
                    if test[BONUS] and self.bonus:
                        msg = msg + test_bonus_msg % 5
                else:
                    msg = timout_msg % (test[TEST_NAME], 
                                        100.0*self.passed_tests/num_times)
                
                out_file = open(self.output_filename, "a")
                out_file.write(msg + "\n")
                out_file.close()
                self.finished_output = True 
                os._exit(0)

    def start_test(self, test, iteration, total, timer):
        """
            Starts the execution of a given test. It generates the nodes and
            creates threads that send tasks to the cluster's nodes and wait for
            the result. We have only 1 type of tasks, for obtaining the result.

            @type test: a dictionary, its keys are defined in util.py
            @param test: a Test object containing the test's parameters
            @type iteration: Integer
            @param iteration: the index of the current iteration
            @type total: Integer
            @param total: the total number of iterations
            @type timer: Timer
            @param timer: the timer used for timeout detection
        """
       
        supervisor = Supervisor()
        supervisor.register_banned_thread(timer)
        supervisor.register_banned_thread(current_thread())

        if test[MAT_SIZE] != test[NUM_NODES]:
            print "Wrong test format, matrix size must be the same as the\
                    number of nodes"
            os._exit(0)

        # contains all the Node objects, ordered by index (ascending) 
        nodes = []
        
        datastores = [] 
        
        n = test[MAT_SIZE] # NUM_NODES = MAT_SIZE !
        for i in range(n):
            requests = 0
            if test[MAX_PENDING_REQUESTS] != 0:
                requests = self.test_generator.rand_gen.randint(1, test[MAX_PENDING_REQUESTS])
            datastores.append(Datastore(test[A][i][:],     # matrix row
                                        test[B][i],     # vector element
                                        requests,
                                        test[MIN_DATASTORE_DELAY],
                                        test[MAX_DATASTORE_DELAY],
                                        test[SEED_DS],
                                        supervisor))
            nodes.append(Node(i, n))
            supervisor.register_node(datastores[-1], nodes[-1])

        for node in nodes:
            # set the node's datastore
            node.set_datastore(datastores[nodes.index(node)])
            # the nodes need to know about each other
            node.set_nodes(nodes[:])

        self.result = {}

        # sends tasks to nodes 
        # we need threads because get_x() is blocking

        threads = []
        for node in nodes:
            threads.append(Thread(target=Tester.start_node, args=(self, node)))
            supervisor.register_banned_thread(threads[-1])
            threads[-1].start()
            
        for t in threads:
            t.join()

        for node in nodes:
            node.shutdown()

        supervisor.check_termination()

        if self.bonus is None:
            self.bonus = supervisor.check_bonus()
        else:
            self.bonus = self.bonus and supervisor.check_bonus()
       
        errors = supervisor.status()
        errors.extend(self.check_result(test))

        if len(errors) == 0:
            self.passed_tests += 1
        else:
            self.bonus = False
            print test_errors_msg % (iteration, total)
            for error in errors:
                self.print_error(error)

                        
    def start_node(self, node):
        """
            Sends a task to a cluster's node and checks the result.

            @type node: Node
            @param node: the node to which to send the task
        """
        result = node.get_x()
        with self.result_lock:
            self.result[node] = result


    def check_result(self, test):
        """
            Checks if a given matrix block is the correct result.
            We consider the values only up to their 3rd decimal.

            @type test: a dictionary, its keys are defined in util.py
            @param test: the test to check

            @type Boolean
            @return True if the result is valid, False otherwise
        """
        test[X] = [round(x,3) for x in test[X]]
        result = [None for x in test[X]]
        for node, ans in self.result.items():
            try:
                (x, index) = ans
                result[index] = round(x, 3)
            except Exception, err:
                return ["exception '%s' while checking result returned by \
node '%s'" % (str(err), str(node))]
        if result != test[X]:
            return ["returned: %s" % str(result), "expected: %s" % str(test[X])]
        return []
 
    def print_error(self, error_msg):
        print "ERROR: %s" % error_msg  


class TestGenerator:
    """
        Randomly generates tests, tests are composed of a matrix, a number of
        nodes and a number of tasks.

        For loading a test's parameters from a file with the following format:
              
        param_name1 = value
       
        param_name2 = value
        
        [...]

        Blank lines or lines starting with # (comments) are ignored

        Parameter names are defined in this class. Parameters can be declared in
        any order in the file.
    """
    

    def __init__(self):
       
        self.params_names = [NUM_NODES, MAT_SIZE, TEST_NAME, TIMEOUT_PERIOD, 
                            MAT_FILE, SEED_DS, SEED_MAT, MIN_DATASTORE_DELAY,  
                            MAX_DATASTORE_DELAY, MAX_PENDING_REQUESTS, BONUS, A, X, B]
        
        # the seed will be set when loading a test file
        self.rand_gen = random.Random() 

 
    def generate_matrix(self, dim):
        """
            Randomly generates the elemens of a square matrix.
            @type dim: integer
            @param dim: the dimension of the matrix
            @param seed: the seed for the pseudorandom generator
            @return: a list of lists containing the matrix
        """
        matrix = []
        for i in range(dim):
            matrix.append([self.rand_gen.uniform(-1000,1000) for i in range(dim)])
        
        return matrix
       
    def generate_vectors(self, matrix):
        """
            Generate the X and B vectors of A*X=B equation. X is randomly 
            generated and than B is the result of A*X.
            @return 
        """
        dim = len(matrix)
        x = [self.rand_gen.uniform(-1000,1000) for i in range(dim)]

        b = []

        for i in range(dim):
            b.append(0)
            for j in range(dim):
                b[i] += matrix[i][j] * x[j]

        return (x, b)
              

    def load_matrix(self, filename, mat_size):
        """
            Loads a matrix from a given file. Checks if it has the expected 
            size, if not, it exists the program.

            @type filename: string
            @param filename: the file containing the matrix
            @type mat_size: integer
            @param mat_size: the expected size for the matrix
        """
        mat = []
        try:
            f = open(filename, "r")
            for line in f:
                line = line.strip()
                if len(line) == 0 or line.startswith('#'):
                    continue
                parts = line.split()
                mat.append([float(x) for x in parts])

            if mat_size != len(mat):
                print "Incorrect size %d for given matrix, expected %d " %\
                                                        (len(mat), mat_size)
                os._exit(0)

        except Exception, err: #IOError or cast errors
            print err
            os._exit(0)
            
        return mat

    def load_test(self, filename):
        """
            Loads test parameters from a file.
            @return: a Test object
        """
        test_params = dict.fromkeys(self.params_names, 0)
        try:
            f = open(filename, "r")
            for line in f:
                line = line.strip()
                if len(line) == 0 or line.startswith('#'):
                    continue
           
                parts = [i.strip() for i in re.split("=", line)]
                if len(parts) != 2:
                    raise Exception("Wrong test file format")
                
                if DEBUG: print parts
                
                if parts[0] not in test_params:
                    raise Exception("Wrong parameter name: %s" % parts[0])

                if parts[0] != TEST_NAME and parts[0] != MAT_FILE:
                    if parts[1].isdigit():
                        test_params[parts[0]] = int(parts[1])
                    else:
                        try:
                            test_params[parts[0]] = float(parts[1])
                        except ValueError:
                            raise Exception("Wrong parameter %s type, expected \
int or float, not %s" % (parts[0], type(parts[1])))
                else:
                    test_params[parts[0]] = parts[1]

                
        except IOError, err:
            print err
            os._exit(0)
        except Exception, err:
            print err
            f.close()
            os._exit(0)
        else: 
            f.close()
        
        test_params[MAT_SIZE] = test_params[NUM_NODES]
        # set seed of the Random object used to generate the matrix and vector
        self.rand_gen.seed(test_params[SEED_MAT])
               
        if test_params[MAT_FILE] == 0:
            test_params[A] = self.generate_matrix(test_params[MAT_SIZE])
        else:
            test_params[A] = self.load_matrix(test_params[MAT_FILE],
                test_params[MAT_SIZE])

        test_params[X], test_params[B] = self.generate_vectors(test_params[A])

        return test_params       
            

    def generate_random_test(self, testname, max_mat_size, timeout, bonus = 0):
        """
            Randomly generates test parameters.
            @return: a Test object
        """

        test_params[NUM_NODES] = random.randint(2, 150) 
        test_params[TEST_NAME] = testname
        test_params[SEED_DS] = random.random()
        test_params[SEED_MAT] = random.random()
        test_params[MAT_SIZE] = test_params[NUM_NODES]
        test_params[TIMEOUT] = timeout 
        test_params[A] = self.generate_matrix(test_params[MAT_SIZE],
                                                     test_params[SEED_MAT])
   
        max_delay = random.randint(5)
        min_delay = random.randint(5)
        while min_delay > max_delay:
            min_delay = random.randint(5)
        test_params[MIN_DATASTORE_DELAY] = min_delay
        test_params[MAX_DATASTORE_DELAY] = max_delay
        test_params[MAX_PENDING_REQUESTS] = random.randint(10)
        test_params[BONUS] = bonus

        return test_params

        
def usage(argv):
    print "Usage: python %s [OPTIONS]"%argv[0]
    print "options:"
    print "\t-f,   --testfile\ttest file, if not specified it generates a random test"
    print "\t-o,   --out\t\toutput file"
    print "\t-t,   --times\t\tthe number of times the test is run, defaults to 2"
    print "\t-h,   --help\t\tprint this help screen"

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:f:o:t:", ["help",
            "testfile=", "out=", "times="])
    except getopt.GetoptError, err:
        print str(err)
        usage(sys.argv)
        sys.exit(2)

    test_file = ""
    times = 2
    output_file = "tester.out"

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(0)
        elif o in ("-f", "--testfile"):
            test_file = a
        elif o in ("-o", "--out"):
            output_file = a 
        elif o in ("-t", "--times"):
            try:
                times = int(a)
            except TypeError, err:
                print str(err)
                sys.exit(2)
        else:
            assert False, "unhandled option"
    
    res =  TestGenerator().load_test(test_file)
    t = Tester(output_file)
    t.run_test(test_file, times)
