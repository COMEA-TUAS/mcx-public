__package__ = "modelconductor"
import threading
import sys
import logging
import yaml
import os
import asyncio
import concurrent.futures
import json
import time
from datetime import datetime as dt

from hbmqtt.broker import Broker
from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.errors import MQTTException
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2

from modelconductor.modelhandler import SklearnModelHandler, HistoricalModelHandler
from .utils import Measurement
from modelconductor.experiment import Experiment, ModelStatus
from modelconductor.config import config_dir

logger = logging.getLogger(__name__)
formatter = "[%(asctime)s] :: %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=formatter)

class MqttExperiment(Experiment):

    def init_broker(self, loop):
        if sys.version_info[:2] < (3, 4):
            logger.fatal("Error: Python 3.4+ is required")
            sys.exit(-1)

        config_file = os.path.join(config_dir, 'default_broker.yaml')
        try:
            with open(config_file, 'r') as stream:
                config = yaml.full_load(stream) if hasattr(yaml, 'full_load') else yaml.load(stream)
        except yaml.YAMLError as exc:
            logger.error("Invalid config_file %s: %s" % (config_file, exc))
    
        broker = Broker(config, loop=loop)
        return broker

    def init_subscriber(self, loop):
        config_file = os.path.join(config_dir, 'default_client.yaml')
        try:
            with open(config_file, 'r') as stream:
                config = yaml.full_load(stream) if hasattr(yaml, 'full_load') else yaml.load(stream)
        except yaml.YAMLError as exc:
            logger.error("Invalid config_file %s: %s" % (config_file, exc))
        client = MQTTClient(client_id="mqtt_subscriber_exp", config=config, loop=loop)
        return client
    
    @Experiment._run
    def execute_model_loop(self):
        while True:
            print("waiting for buffer_equal_window")            
            while not (self.model.status == ModelStatus.READY):
                time.sleep(0.1)
            print("continue from buffer_equal_window")
            result = self.model.step()
            print("result: ")
            print(result)
            if result is not None:
                result['TIMING_model_response_timestamp'] = time.time()
                self.log_row(result, self.model)
            
            

    @asyncio.coroutine
    def do_sub_historical(self, client, url):
        try:
            qos = QOS_1
            
            yield from client.connect(uri=url)
            
            filters = [("topic_1", qos)]
            yield from client.subscribe(filters)
            self.model.spawn()
            self.model.buffer_equal_window = asyncio.Event()
            count = 0
            with open(self.log_path + "_inputs", mode='w') as input_logger:
                while True:
                    try:
                        message = yield from client.deliver_message()
                        count += 1                    
                        item = message.publish_packet.data
                        data = json.loads(item.decode('utf-8'))
                        data = Measurement(data)    
                        print(data)     
                        print(",".join(map(str, list(data.values()))), file=input_logger)
                        input_logger.flush()
                        data['TIMING_model_response_timestamp'] = 0

                        self.model.store(data)                       

                        # res = self.model.step(data)
                        # results.append(result)
                        
                    except MQTTException:
                        logger.debug("Error reading packet")
            yield from client.disconnect()
        except KeyboardInterrupt:
            yield from client.disconnect()
        except ConnectException as ce:
            logger.fatal("connection to '%s' failed: %r" % (url, ce))
        except asyncio.CancelledError as cae:
            logger.fatal("Publish canceled due to prvious error: %r" % cae)
    @asyncio.coroutine
    def do_sub(self, client, url):
        try:
            yield from client.connect(uri=url)
            qos = QOS_1
            filters = [("topic_1", qos)]
            yield from client.subscribe(filters)
            self.model.spawn()

            count = 0
            while True:
                try:
                    message = yield from client.deliver_message()
                    count += 1                    
                    item = message.publish_packet.data
                    data = json.loads(item.decode('utf-8'))
                    data = Measurement(data)    
                    print(data)     
                    
                    data['TIMING_model_response_timestamp'] = 0

                    res = self.model.step(data)

                    res['TIMING_model_response_timestamp'] = time.time()
                    print("result: ")
                    print(res)
                    self.results.append(res)
                    self.log_row(res, self.model)
                except MQTTException:
                    logger.debug("Error reading packet")
            yield from client.disconnect()
        except KeyboardInterrupt:
            yield from client.disconnect()
        except ConnectException as ce:
            logger.fatal("connection to '%s' failed: %r" % (url, ce))
        except asyncio.CancelledError as cae:
            logger.fatal("Publish canceled due to prvious error: %r" % cae)

    def init_model(self, idx_path, model_path):
        import pickle
        # load input variable names for the pretrained sklearn model
        with open(idx_path, 'rb') as f:
            idx = pickle.load(f)
            
        input_keys = idx
        target_keys = ["Left_NOx_pred"]
        control_keys = ["Left_NOx", 
                        "TIMING_client_request_timestamp", 
                        "TIMING_model_response_timestamp"]

        model = SklearnModelHandler(model_filename=model_path,
                           input_keys=input_keys,
                           target_keys=target_keys,
                           control_keys=control_keys)
        return model
    
    def run(self, loop = None):
        if loop is None:
            loop = asyncio.get_event_loop()
        
        # broker = self.init_broker(loop=loop)
        client = self.init_subscriber(loop)

        try:
            # loop.run_until_complete(broker.start())
            print("running execute_model_loop!")
            # loop = asyncio.get_running_loop()
            # with concurrent.futures.ThreadPoolExecutor() as pool:
            #    loop.run_in_executor(pool, self.execute_model_loop)
            print(type(self.model))
            # import pdb; pdb.set_trace()
            if isinstance(self.model, HistoricalModelHandler):
                model_runner = threading.Thread(target=self.execute_model_loop, daemon=True)
                model_runner.start()
                loop.run_until_complete(self.do_sub_historical(client, "mqtt://localhost"))
            else:
                loop.run_until_complete(self.do_sub(client, "mqtt://localhost"))
            loop.run_forever()
        except KeyboardInterrupt:
            pass
            # loop.run_until_complete(broker.shutdown())


if __name__ == "__main__":
    ex = MqttExperiment(logging=True)
    
    ex.model = ex.init_model(os.path.join("customexamples", "sklearnovertcp_nox_demo", "nox_idx.pickle"), \
                                os.path.join("customexamples", "sklearnovertcp_nox_demo", "nox_rfregressor.pickle"))
    headers = ["timestamp"]
    if ex.model.input_keys:
        headers += ex.model.input_keys
    if ex.model.target_keys:
        headers += ex.model.target_keys
    if ex.model.control_keys:
        headers += ex.model.control_keys
    ex.logger = ex.initiate_logging(headers=headers)
    ex.run()
    