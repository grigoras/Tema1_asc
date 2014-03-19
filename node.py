"""
    This module represents a cluster's computational node.

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2014
"""
from threading import Semaphore
import Queue

class Node:
    """
        Class that represents a cluster node with computation and storage
        functionalities.
    """

	def __init__(self, node_id, matrix_size):
		"""
			Constructor.

			@type node_id: Integer
			@param node_id: an integer less than 'matrix_size' uniquely
				identifying the node
			@type matrix_size: Integer
			@param matrix_size: the size of the matrix A
		"""
		self.node_id = node_id
		self.matrix_size = matrix_size
		self.datastore = None
		self.nodes = None
		# TODO other code
		self.sem = Semaphore(value = 0)
		self.sema = None
		self.pivot = 0
		self.my_thread = None
		self.queue = Queue.Queue()

	def get_A(self, column):
		self.sema.acquire()
		el = self.datastore.get_A(self, column)
		self.sema.release()
		return el

	def put_A(self, column, A):
		self.sema.acquire()
		self.datastore.put_A(self, column, A)
		self.sema.release()

	def get_b(self):
		self.sema.acquire()
		el = self.datastore.get_b(self)
		self.sema.release()
		return el	

	def put_b(self, b):
		self.sema.acquire()
		self.datastore.put_b(self, b)
		self.sema.release()


	def comp(self):
		self.datastore.register_thread(self, my_thread)
		num = self.node_id;
		while (num >= 1):
			el = self.queue.get()
			if (el.tip == 0):
				#cer linia si o trimit
				# astept raspuns, daca 2 nu am aleso daca 3 aia e
			else if (el.tip == 1):
				#cer linia si fac zero pe node_id - num
				# o trimit la datastore
			num--
		if (self.datastore.get_A(self, node_id) == 0):
			# trimit la toti mesaj cu tip 0
			msg = Message(0, node_id)
			for nod in self.nodes
				nod.queue.put(msg)
			trim = 0;
			for i in range (self.nodes.size())
				linie = self.queue.get()
				if (linie.line[0] != 0 && trim == 0):
					trim = trim + 1
					msg = Message(3, self.node_id)
					for j in range (self.node_id, self.matrix_size):
						l[j-node_id] = self.datastore.get_A(self, j)
					msg.add_line(l)
					self.nodes[linie.sign].queue.put(msg)
				else:
					msg = Message(2, node_id)
					self.nodes[linie.sign].queue.put(msg)
		# cer toata linia si o trimit la toti
		# daca node_id != size
			# astept mesaj de la node_id + 1
		# calculez x
		# daca node_id != 0
			# trimit mesaj cu x lu' node_id - 1

	def __str__(self):
		"""
			Pretty prints this node.

			@rtype: String
			@return: a string containing this node's id
		"""
        	return "Node %d" % self.node_id


	def set_datastore(self, datastore):
		"""
			Gives the node a reference to its datastore. Guaranteed to be called
			before the first call to 'get_x'.

			@type datastore: Datastore
			@param datastore: the datastore associated with this node
		"""
		self.datastore = datastore
		# TODO other code


	def set_nodes(self, nodes):
		"""
			Informs the current node of the other nodes in the cluster. 
			Guaranteed to be called before the first call to 'get_x'.

			@type nodes: List of Node
			@param nodes: a list containing all the nodes in the cluster
		"""
		self.nodes = nodes
		# TODO other code
		self.sema = Semaphore(value = self.datastore.get_max_pending_requests())
		self.my_thread = Thread(target = comp)


	def get_x(self):
        	"""
			Computes the x value corresponding to this node. This method is
			invoked by the tester. This method must block until the result is
			available.

			@rtype: (Float, Integer)
			@return: the x value and the index of this variable in the solution
				vector
		"""
		pass
		# TODO other code


	def shutdown(self):
        	"""
			Instructs the node to shutdown (terminate all threads). This method
            		is invoked by the tester. This method must block until all the
            		threads started by this node terminate.
        	"""
		pass
		# TODO other code

class Message:

	def __init__(self, tip, sign):
		"""
			Constructor.
		"""
		self.tip = tip
		self.sign = sign

	def add_line(self, line):
		self.line = line
