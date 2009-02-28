import threading

class Future:
	def __init__(self, method, *args):
		self.value = None
		self.done = False
		self.valueReady = threading.Condition()

		# Now call the method
		method(self._callback, *args)

	def get(self, block):
		if not self.done and block:
			self.valueReady.acquire()
			while not self.done:
				self.valueReady.wait()
			self.valueReady.release()
		return (self.done, self.value)
	
	def _callback(self, value):
		self.valueReady.acquire()
		self.done = True
		self.value = value
		self.valueReady.notify()
		self.valueReady.release()

