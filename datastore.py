"""
    This module provides the storage part of the cluster's nodes.    

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2014
"""


from random import Random
from threading import current_thread, Semaphore
from time import sleep


class Datastore:
    """
        Class that represents the storage functionalities of a node.
    """

    def __init__(self, A_row, b_elem, max_pending_requests, delay_min, delay_max,
        delay_seed, supervisor):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Instantiates a new datastore with the given stored data and parameters.

            @type A_row: List of Float
            @param A_row: the row from matrix A
            @type b_elem: Float
            @param b_elem: the element from vector b
            @type max_pending_requests: Integer
            @param max_pending_requests: number of request to allow in-flight
            @type delay_min: Float
            @param delay_min: the minimum time taken by a request
            @type delay_max: Float
            @param delay_max: the maximum time taken by a request
            @type delay_seed: Integer
            @param delay_seed: seed used to randomly generate delays
            @type supervisor: Supervisor
            @param supervisor: supervisor to use for checking accesses
        """
        self.A_row = A_row
        self.b_elem = b_elem
        self.max_pending_requests = max_pending_requests
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.supervisor = supervisor
        self.remaining_requests = Semaphore(max_pending_requests)
        self.random = Random(delay_seed)


    def __check_request(self, node):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Internal datastore method used to check if a request is valid.

            @type node: Node
            @param node: the node wanting to access the datastore
        """
        self.supervisor.check_access(self, node, current_thread())

        if self.max_pending_requests != 0:
            if not self.remaining_requests.acquire(False):
                #ERROR: no more remaining requests
                self.supervisor.report("maximum pending datastore requests \
    exceeded on node " + str(node))
                return

        delay = self.delay_min + (self.delay_max - self.delay_min) * self.random.random()
        if delay > 0:
            sleep(delay)

        if self.max_pending_requests != 0:
            self.remaining_requests.release()


    def register_thread(self, node, thread):
        """
            Registers the given thread as belonging to the given node. The node
            must register threads with its own datastore. The threads will then
            be able to query the datastore. Multiple threads (logically
            owned by 'node') can be registred to the datastore.

            @type node: Node
            @param node: the node owning the datastore
            @type thread: Thread
            @param thread: the thread to register
        """
        self.supervisor.register_thread(self, node, thread)


    def get_max_pending_requests(self):
        """
            Returns the maximum number of in-flight requests supported by this
            datastore.

            @rtype: Integer
            @return: the maximum number of in-flight requests supported by this
                datastore; 0 if unlimited
        """

        return self.max_pending_requests


    def get_A(self, node, column):
        """
            Returns an element from the row of the A matrix that is stored in
            this datastore. This is a blocking operation. The maximum number of
            in-flight requests is limited, see get_max_pending_requests().

            @type node: Node
            @param node: the node accessing the datastore; must be the node that
                owns the datastore
            @type column: Integer
            @param column: the column of the element

            @rtype: Float
            @return: the element of matrix A at the requested position
        """
        self.__check_request(node)

        return self.A_row[column]


    def put_A(self, node, column, A):
        """
            Updates an element from the row of the A matrix that is stored in
            this datastore. This is a blocking operation. The maximum number of
            in-flight requests is limited, see get_max_pending_requests().

            @type node: Node
            @param node: the node accessing the datastore; must be the node that
                owns the datastore
            @type column: Integer
            @param column: the column of the element
            @type A: Float
            @param A: the new element value
        """
        self.__check_request(node)

        self.A_row[column] = A


    def get_b(self, node):
        """
            Returns the element of b stored in this datastore. This is a
            blocking operation. The maximum number of in-flight requests is 
            limited, see get_max_pending_requests().

            @type node: Node
            @param node: the node accessing the datastore; must be the node that
                owns the datastore

            @rtype: Float
            @return: the element of b stored in this datastore
        """
        self.__check_request(node)

        return self.b_elem

    def put_b(self, node, b):
        """
            Updates the element of b stored in this datastore. This is a
            blocking operation. The maximum number of in-flight requests is 
            limited, see get_max_pending_requests().

            @type node: Node
            @param node: the node accessing the datastore; must be the node that
                owns the datastore
            @type b: Float
            @param b: the new value of b
        """
        self.__check_request(node)

        self.b_elem = b
