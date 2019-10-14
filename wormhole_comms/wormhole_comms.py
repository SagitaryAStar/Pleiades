import json
import os
import random
import time
from multiprocessing import Process

import zmq

IPC_PATH = '/tmp/django_processes'
MONITOR_PATH = os.path.join(IPC_PATH, 'monitor')
WORKER_PATH = os.path.join(IPC_PATH, 'worker')
ERRORS_PATH = os.path.join(WORKER_PATH, 'errors')
SERVER_PATH = os.path.join(WORKER_PATH, 'server')

MONITOR_CHANNEL_ROOT = 'ipc://' + os.path.join(MONITOR_PATH, '{}')
WORKER_CHANNEL_ROOT = 'ipc://' + os.path.join(WORKER_PATH, '{}')
ERROR_CHANNEL_ROOT = 'ipc://' + os.path.join(ERRORS_PATH, '{}')
SERVER_CHANNEL_ROOT = 'ipc://' + os.path.join(SERVER_PATH, '{}')

EXIT_MESSAGE = u'000__000'
DEFAUL_NUM_WORKERS = 4

_process_monitors = {}


def register_monitor_socket(monitor_socket):
    _process_monitors[os.getpid()] = monitor_socket
    # print("Registering socket in {}".format(_process_monitors))


def unregister_monitor_socket():
    if os.getpid() in _process_monitors:
        _process_monitors.pop(os.getpid())
    else:
        print('Error, not registered socket in {} {}'.format(os.getpid(), _process_monitors))


def _get_process_monitor_socket():
    return _process_monitors.get(os.getpid())


def initialize_ipc_file_structure():
    paths = [IPC_PATH, MONITOR_PATH, WORKER_PATH, ERRORS_PATH, SERVER_PATH]
    for path in paths:
        if not os.path.exists(path):
            os.mkdir(path)


def monitored(func):
    def wrapper(*args):
        monitor_socket = _get_process_monitor_socket()
        start_time = time.time()
        func(*args)
        end_time = time.time()
        instrument_id = func.__name__
        call_time = end_time - start_time
        if monitor_socket:
            # print("Sending data through Socket")
            instrument_data = InstrumentData(instrument_id, call_time)
            monitor_socket.send_instrumentation(instrument_data.serialize())
        else:
            print("{} called, call time: {}".format(instrument_id, call_time))

    return wrapper


class InstrumentData(object):
    FIELDS = ['call_time', ]
    SEPARATOR = '****-***'
    DATA_SEPARATOR = '---*---'

    def __init__(self, instrument_id, *args, **kwargs):
        self.instrument_id = instrument_id
        for index, value in enumerate(args):
            setattr(self, self.FIELDS[index], value)
        for field, value in kwargs.items():
            setattr(self, field, value)
        not_set = [f for f in self.FIELDS if f not in dir(self)]
        if not_set:
            raise ValueError("You need to set a value for {} fields".format(','.join(not_set)))

    def serialize(self):
        data = self.SEPARATOR.join(["{}-{}".format(field, getattr(self, field)) for field in self.FIELDS])
        return self.DATA_SEPARATOR.join([self.instrument_id, data])

    @classmethod
    def deserialize(cls, msg):
        instrument_id, data = msg.split(cls.DATA_SEPARATOR)
        return {

            instrument_id: {
                field: value for field, value in [pair.split('-') for pair in data.split(cls.SEPARATOR)]
            }
        }


class MonitorSocket(object):
    INSTRUMENTATION = 'inst_xxx '
    TYPE_INSTRUMENTATION = 'instrumentation'
    TYPE_MESSAGE = 'message'

    def __init__(self, context, execution_id, socket_type):
        self.socket = context.socket(socket_type)
        self.socket_direction = MONITOR_CHANNEL_ROOT.format(execution_id)
        if socket_type == zmq.PUSH:
            self.socket.connect(self.socket_direction)
        elif socket_type == zmq.PULL:
            # print("Binding {}".format(self.socket_direction))
            self.socket.bind(self.socket_direction)
        else:
            error_msg = "socket_type should be either zmq.PULL or zmq.PUSH {}-{}-{}"
            error_msg = error_msg.format(context, execution_id, socket_type)
            raise ValueError(error_msg)

    def format(self, incoming_data):
        if hasattr(incoming_data, 'startswith') and incoming_data.startswith(self.INSTRUMENTATION):
            type_data = self.TYPE_INSTRUMENTATION
            data = InstrumentData.deserialize(incoming_data[len(self.INSTRUMENTATION):])
        else:
            type_data = self.TYPE_MESSAGE
            data = incoming_data
        return type_data, data

    def recv(self):
        data = self.socket.recv_json()
        # print("Receiving data Monitor")
        return self.format(json.loads(data))

    def send(self, data):
        # print("Sending data Monitor")
        msg = json.dumps(data)
        self.socket.send_json(msg)

    def send_instrumentation(self, data):
        msg = "{}{}".format(self.INSTRUMENTATION, data)
        self.send(msg)


class WorkerSocket(object):
    def __init__(self, context, execution_id, socket_type):
        self.socket = context.socket(socket_type)
        self.socket_direction = WORKER_CHANNEL_ROOT.format(execution_id)
        if socket_type == zmq.PUSH:
            # print("Binding {}".format(self.socket_direction))
            self.socket.bind(self.socket_direction)
        elif socket_type == zmq.PULL:
            self.socket.connect(self.socket_direction)
        else:
            error_msg = "socket_type should be either zmq.PULL or zmq.PUSH {}-{}-{}"
            error_msg = error_msg.format(context, execution_id, socket_type)
            raise ValueError(error_msg)

    def recv(self):
        data = self.socket.recv_json()
        # print(u'Worker receiving {}'.format(data))
        return json.loads(data)

    def send(self, data):
        msg = json.dumps(data)
        self.socket.send_json(msg)

    def send_worker_data(self, data):
        self.send(data)


class ErrorSocket(object):
    ERROR_TOPIC = 'errors_xxx'

    def __init__(self, context, execution_id, socket_type):
        self.socket = context.socket(socket_type)
        self.socket_direction = ERROR_CHANNEL_ROOT.format(execution_id)
        if socket_type == zmq.PUB:
            # print("Binding {}".format(self.socket_direction))
            self.socket.connect(self.socket_direction)
        elif socket_type == zmq.SUB:
            self.socket.bind(self.socket_direction)
            self.socket.setsockopt(zmq.SUBSCRIBE, self.ERROR_TOPIC)
        else:
            error_msg = "socket_type should be either zmq.PUB or zmq.SUB {}-{}-{}".format(
                context, execution_id, socket_type)
            error_msg = error_msg.format(context, execution_id, socket_type)
            raise ValueError(error_msg)

    def recv(self):
        data = self.socket.recv_string()
        data = data[len(self.ERROR_TOPIC):]
        return json.loads(data)

    def send(self, data):
        msg = json.dumps(data)
        self.socket.send_string(self.ERROR_TOPIC + msg)


def master_process(execution_id, worker_func, get_data_func, data, batch_size):
    initialize_ipc_file_structure()
    context = zmq.Context()

    # Socket to send messages on
    sender = WorkerSocket(context, execution_id, zmq.PUSH)

    monitor = Process(target=monitor_process, args=(execution_id,))
    monitor.start()
    # Socket with direct access to the monitor: used to clean up the process after all workers finish
    monitor_socket = MonitorSocket(context, execution_id, zmq.PUSH)

    # Initialize random number generator
    random.seed()

    # Send 100 tasks.
    workers = []
    try:
        for i in range(DEFAUL_NUM_WORKERS):
            # worker_construction_start = time.time()
            worker = Process(target=worker_process, args=(execution_id, worker_func, get_data_func))
            worker.start()
            # print("Worker construction cost: %s msec" % ((time.time() - worker_construction_start) * 1000))
            workers.append(worker)

        for task_batch in [data[i:i + batch_size] for i in range(0, len(data), batch_size)]:
            sender.send_worker_data(task_batch)
        print("All data sent")
    finally:
        for i in range(10):
            sender.send(EXIT_MESSAGE)
        while(any([w.is_alive() for w in workers])):
            sender.send(EXIT_MESSAGE)
            time.sleep(5)
        print("All workers killed")
        monitor_socket.send(EXIT_MESSAGE)
        monitor.join()
    # Give 0MQ time to deliver
    time.sleep(1)


def worker_process(execution_id, worker_func, get_data_func):
    def worker(data):
        context = zmq.Context()
        # Socket to send messages to
        sender = MonitorSocket(context, execution_id, zmq.PUSH)
        register_monitor_socket(sender)

        # Error socket
        errors = ErrorSocket(context, execution_id, zmq.PUB)

        # Do the work
        try:
            result = worker_func(get_data_func(data))

            # Send results to sink
            sender.send(result)
        except Exception as e:
            msg = u"Exception in worker Process {}".format(e)
            print(msg)
            errors.send(msg)
        unregister_monitor_socket()

    context = zmq.Context()

    # Socket to receive messages on
    receiver = WorkerSocket(context, execution_id, zmq.PULL)
    errors = ErrorSocket(context, execution_id, zmq.PUB)
    # Process tasks forever
    receive_time = []
    worker_time = []
    processed = 0
    while True:
        receive_start = time.time()
        try:
            data = receiver.recv()
            receive_time.append(time.time() - receive_start)
            if data == EXIT_MESSAGE:
                # print('Process killed by master')
                break
            processed += 1
            worker_start_time = time.time()
            worker_process = Process(target=worker, args=(data,))
            worker_process.start()
            worker_process.join()
            worker_time.append(time.time() - worker_start_time)
        except Exception as e:
            msg = "Exception in the worker {}".format(e)
            errors.send(msg)
            print(msg)
    unregister_monitor_socket()
    print("Processed elements {}".format(processed))
    if receive_time:
        print("Medium time receive {}".format(sum(receive_time) * 1000.0 / float(len(receive_time))))
    if worker_time:
        print("Medium time worker {}".format(sum(worker_time) * 1000.0 / float(len(worker_time))))
    else:
        print("Not used worker")


def _construct_info_msg(results, errors, instrumented_data, tstart):
    tend = time.time()
    data = {
        'data': results,
        'errors': errors,
        'instrumentation': instrumented_data,
        'time': "Total elapsed time: {} msec".format((tend-tstart)*1000),
    }

    return json.dumps(data)


def monitor_process(execution_id):
    context = zmq.Context()

    # Socket to receive messages on

    receiver = MonitorSocket(context, execution_id, zmq.PULL)

    error = ErrorSocket(context, execution_id, zmq.SUB)

    info_server = context.socket(zmq.REP)
    info_server.bind(SERVER_CHANNEL_ROOT.format(execution_id))

    poller = zmq.Poller()
    poller.register(receiver.socket, zmq.POLLIN)
    poller.register(error.socket, zmq.POLLIN)
    poller.register(info_server, zmq.POLLIN)
    # Wait for start of batch

    # Start our clock now
    tstart = time.time()

    # Process confirmations
    task_nbr = 0
    receive_time = []
    errors = []
    instrumented_data = {}
    results = []
    try:
        while True:
            reception = dict(poller.poll())
            # print('Reception {}'.format(reception))
            if receiver.socket in reception:
                task_nbr += 1
                receive_start = time.time()
                type_data, data = receiver.recv()
                # print("monitor data received {}".format(data))
                receive_time.append(time.time() - receive_start)
                if data == EXIT_MESSAGE:
                    break
                if type_data == MonitorSocket.TYPE_INSTRUMENTATION:
                    [(instrumented_key, instrumented_info)] = [(key, value) for key, value in data.items()]
                    instrumented_value = instrumented_data.get(instrumented_key, {
                        'num_calls': 0,
                        'data_points': []
                    })
                    instrumented_value['num_calls'] += 1
                    instrumented_value['data_points'].append(instrumented_info)
                    instrumented_data[instrumented_key] = instrumented_value
                else:
                    results.append(data)

            if error.socket in reception:
                error_msg = error.recv()
                errors.append(error_msg)

            if info_server in reception:
                msg = _construct_info_msg(results, errors, instrumented_data)
                info_server.send(msg)
    finally:
        with open('/tmp/result_{}_monitoring.json'.format(execution_id), 'w') as f:
            f.write(_construct_info_msg(results, errors, instrumented_data, tstart))
    # Calculate and report duration of batch
    print("Medium time receive monitor {}".format(sum(receive_time) * 1000.0 / float(len(receive_time))))

    tend = time.time()
    print("Total elapsed time: %d msec" % ((tend-tstart)*1000))
