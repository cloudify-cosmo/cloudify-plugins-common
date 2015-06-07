from cloudify.workflows.events import Monitor


# Prevents from capturing any events from AMQP
class MockMonitor(Monitor):
    # Disabling the monitor.
    def capture(self):
        pass

    # Stopping it should do nothing really. since it never really started.
    def stop(self):
        pass
