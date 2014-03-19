"""
    This module is used to enforce the access restrictions imposed by the
    cluster architecture: nodes can only access their datastore from threads
    belonging to them. It offers a method for checking an access against the
    list of all threads in the application: node threads and tester threads.
    Access violation errors are stored in a list to be reported at the end of
    a test, togheter with errors reported by other modules.

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2014
"""


from threading import current_thread, enumerate, Lock


class Supervisor:
    """
        Class used to globaly check all datastore accesses from node threads.
    """
    def __init__(self):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Creates a new empty supervisor.
        """
        self.datastore_node = {}
        self.datastore_node_lock = Lock()
        self.banned_threads = set()
        self.banned_threads_lock = Lock()
        self.node_threads = {}
        self.node_threads_lock = Lock()
        self.messages = []
        self.messages_lock = Lock()


    def register_node(self, datastore, node):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Registers a new node and the associated datastore.

            @type datastore: Datastore
            @param datastore: the datastore
            @type node: Node
            @param node: the node to register
        """
        with self.datastore_node_lock:
            self.datastore_node[datastore] = node

        with self.node_threads_lock:
            self.node_threads[node] = set()


    def register_thread(self, datastore, node, thread):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Registers a new thread with a datastore.

            @type datastore: Datastore
            @param datastore: the datastore
            @type node: Node
            @param node: the node which owns the thread
            @type thread: Thread
            @param thread: the thread
        """
        with self.node_threads_lock:
            if node not in self.node_threads:
                #ERROR: called by unregistered node
                self.report("unregistered node '%s' is trying to register \
thread '%s' with datastore owned by node '%s'" % (str(node), str(thread),
                    str(self.datastore_node[datastore])))
                return

        with self.datastore_node_lock:
            if node != self.datastore_node[datastore]:
                #ERROR: called by wrong node
                self.report("node '%s' is trying to register thread '%s' with \
datastore owned by node '%s'" % (str(node), str(thread),
                    str(self.datastore_node[datastore])))
                return

        with self.banned_threads_lock:
            if thread in self.banned_threads:
                #ERROR: called with wrong thread
                self.report("node '%s' is trying to register tester thread \
'%s' with datastore owned by node '%s'" % (str(node), str(thread),
                    str(self.datastore_node[datastore])))
                return

        with self.node_threads_lock:
            for n, threads in self.node_threads.items():
                if n != node and thread in threads:
                    #ERROR thread registered with multiple nodes
                    self.report("node '%s' is trying to register thread '%s' \
which is already registered by node '%s'" % (str(node), str(thread), str(n)))
                    return

            self.node_threads[node].add(thread)


    def register_banned_thread(self, thread):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Registers a tester thread. This thread must not be used by nodes for
            any datastore access.

            @type thread: Thread
            @param thread: the thread
        """
        with self.banned_threads_lock:
            self.banned_threads.add(thread)


    def check_access(self, datastore, node, thread):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Checks a datastore access.

            @type datastore: Datastore
            @param datastore: the accessed datastore
            @type node: Node
            @param node: the node trying to access
            @type thread: Thread
            @param thread: the thread from which the access is attempted
        """
        with self.node_threads_lock:
            if node not in self.node_threads:
                #ERROR: called by unregistered node
                self.report("unregistered node '%s' is trying to access \
datastore of node '%s' from thread '%s'" % (str(node),
                    str(self.datastore_node[datastore]), str(thread)))
                return

        with self.datastore_node_lock:
            if node != self.datastore_node[datastore]:
                #ERROR: called by wrong node
                self.report("node '%s' is trying to access datastore registered by \
node '%s'" % (str(node), str(self.datastore_node[datastore])))
                return

        with self.node_threads_lock:
            if thread not in self.node_threads[node]:
                #ERROR: called from wrong thread
                self.report("node '%s' is trying to access datastore owned by \
node '%s' from unregistered thread '%s'" % (str(node),
                    str(self.datastore_node[datastore]), str(thread)))
                return

    def check_termination(self):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Checks for correct node shutdown. There must not be any active
            node threads.
        """
        for thread in enumerate():
            with self.banned_threads_lock:
                if thread in self.banned_threads:
                    continue
            with self.node_threads_lock:
                for node, threads in self.node_threads.items():
                    if thread in threads:
                        #ERROR: registered thread did not terminate
                        self.report("thread '%s' registered with datastore \
of node '%s' did not terminate" % (str(thread), str(node)))
                        break
                else:
                    self.report("thread '%s' not registered with any datastore \
did not terminate" % str(thread))
                continue

    def check_bonus(self):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Checks for bonus condition. Number of created threads per node must
            be at most the number of pending requests accepted by the node's
            datastore plus one.

            @rtype: Boolean
            @return: True, if the solution qualifies for a bonus; False otherwise
        """
        with self.datastore_node_lock:
            with self.node_threads_lock:
                for datastore, node in self.datastore_node.items():
                    if datastore.get_max_pending_requests() == 0:
                        continue
                    if len(self.node_threads[node]) > datastore.get_max_pending_requests() + 1:
                        return False
        return True


    def report(self, message):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Reports an error message. All messages are stored in a list for
            retrieval at the end of the test.

            @type message: String
            @param message: the error message to log
        """
        with self.messages_lock:
            self.messages.append(message)


    def status(self):
        """
            !!! This is not part of the assignment API, do not call it !!!

            Returns the list of logged error messages.

            @rtype: List of String
            @return: the list of encountered errors
        """
        return self.messages
