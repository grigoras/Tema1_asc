# Test parameters, the same string as in the test file format
NUM_NODES = "num_nodes" 
MAT_SIZE = "mat_size" 
TEST_NAME = "test_name" 
MAT_FILE = "matrix_file" 

TIMEOUT_PERIOD = "timeout" 

SEED_DS = "seed_datastore"
SEED_MAT = "seed_elem"

MIN_DATASTORE_DELAY = "min_datastore_delay"
MAX_DATASTORE_DELAY = "max_datastore_delay"
MAX_PENDING_REQUESTS = "max_pending_requests"

BONUS = "bonus"

# the variables for the A*X=B equation
A = "A"
X = "X"
B = "B"


# Tester messages

start_test_msg      = "**************** Start Test %s *****************"
end_test_msg        = "***************** End Test %s ******************"
test_errors_msg     = "Errors in iteration %d of %d:"
test_finished_msg   = "Test %-10s Finished...............%d%% completed"
test_bonus_msg      = " + %d%% bonus"
timout_msg          = "Test %-10s Timeout................%d%% completed"


def mprint(matrix):
    n = len(matrix)
    for i in range(n):
        for j in range(len(matrix[i])):
            print matrix[i][j]," ",
        print 
