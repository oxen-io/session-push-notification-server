from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from requests_futures.sessions import FuturesSession
import random, json, os
from const import *


class LokiAPITarget:
    def __init__(self, address, port, id_key, encryption_key):
        self.address = 'https://' + address
        self.port = str(port)
        self.id_key = id_key
        self.encryption_key = encryption_key

    def __str__(self):
        return self.address + ':' + self.port


class LokiRequestManager:
    def __init__(self, target, api, session_id):
        self.session = api.session
        self.target = target
        self.random_snode_pool = api.random_snode_pool
        self.session_id = session_id
        self.api = api

    def make_request(self, url, parameters):
        request = self.session.post(url=url,
                                    json=parameters,
                                    verify=False,
                                    timeout=defaultTimeout,
                                    hooks={'response': self.parse_response})
        request.add_done_callback(self.request_failed)
        return request

    def parse_response(self, res, **kwargs):
        result = None
        if res:
            try:
                result = json.loads(res.content.decode())
            except Exception as e:
                print('parse error')
        res.result = result
        res.target = self.target
        res.session_id = self.session_id
        return res

    def request_failed(self, future):
        try:
            if future.exception():
                self.api.swarm_cache[self.session_id].remove(self.target)
        except:
            pass


class LokiAPI:
    def __init__(self, logger):
        self.swarm_cache = {}
        self.seed_node_pool = ["https://storage.seed1.loki.network",
                               "https://storage.seed3.loki.network",
                               "https://public.loki.foundation"]
        self.random_snode_pool = []
        self.logger = logger
        self.is_ready = False
        self.session = FuturesSession(executor=ThreadPoolExecutor(max_workers=os.cpu_count()*330))
        self.get_random_snode()

    def init_for_swarms(self, session_ids):
        pubkeys = list(session_ids)
        while len(pubkeys) > 10:
            for pubkey, swarm in self.swarm_cache.items():
                if len(swarm) > 0 and pubkey in pubkeys:
                    pubkeys.remove(pubkey)
            self.get_swarms(pubkeys)
            self.logger.info("get swarms finished, the length is " + str(len(session_ids) - len(pubkeys)))
        self.is_ready = True

    def get_swarms(self, pubkeys):
        self.logger.info("get swarms for " + str(len(pubkeys)) + " session_ids")
        if len(self.random_snode_pool) == 0:
            self.get_random_snode()
        requests = []
        for pubkey in pubkeys:
            if pubkey not in self.swarm_cache.keys():
                self.swarm_cache[pubkey] = []
            random_snode = random.choice(self.random_snode_pool)
            url = random_snode.address + ':' + random_snode.port + '/storage_rpc/' + apiVersion
            parameters = {'method': 'get_snodes_for_pubkey',
                          'params': {
                              'pubKey': pubkey
                          }}
            request_manager = LokiRequestManager(random_snode, self, pubkey)
            requests.append(request_manager.make_request(url, parameters))
        for future in as_completed(requests):
            try:
                response = future.result()
                if response is None:
                    continue
                result = response.result
                session_id = response.session_id
                self.handle_swarm_response(result, session_id)
            except:
                pass

    def handle_swarm_response(self, result, session_id):
        if result and result['snodes']:
            snodes = result['snodes']
            for snode in snodes:
                address = snode['ip']
                if address == '0.0.0.0':
                    continue
                target = LokiAPITarget(address,
                                       snode['port'],
                                       snode['pubkey_ed25519'],
                                       snode['pubkey_x25519'])
                if target not in self.swarm_cache[session_id]:
                    self.swarm_cache[session_id].append(target)
            random.shuffle(self.swarm_cache[session_id])
        else:
            self.logger.warn("error when get snodes for " + session_id)

    def get_random_snode(self):
        print("get random snode")
        target = random.choice(self.seed_node_pool)
        url = target + '/json_rpc'
        parameters = {'method': 'get_n_service_nodes',
                      'params': {
                          'active_only': True,
                          'limit': maxRandomSnodePoolSize,
                          'fields': {
                              'public_ip': True,
                              'storage_port': True,
                              'pubkey_ed25519': True,
                              'pubkey_x25519': True
                          }
                      }}
        try:
            res = self.session.post(url, json=parameters).result()
            result = json.loads(res.content.decode())['result']
            snodes = result['service_node_states']
            for snode in snodes:
                address = snode['public_ip']
                if address == '0.0.0.0':
                    continue
                target = LokiAPITarget(address,
                                       snode['storage_port'],
                                       snode['pubkey_ed25519'],
                                       snode['pubkey_x25519'])
                self.random_snode_pool.append(target)
        except Exception:
            self.logger.warn("Getting random snode failed")

    def get_target_snodes(self, pubkey, index):
        if pubkey not in self.swarm_cache.keys():
            return []
        # if index == 0:
        #     random.shuffle(self.swarm_cache[pubkey])
        return self.swarm_cache[pubkey][:3]

    def get_raw_messages(self, pubkey, last_hash, index):
        target_snodes = self.get_target_snodes(pubkey, index)
        if index >= len(target_snodes):
            return None
        target_snode = target_snodes[index]
        url = target_snode.address + ':' + target_snode.port + '/storage_rpc/' + apiVersion
        parameters = {'method': 'retrieve',
                      'params': {
                          'pubKey': pubkey,
                          'lastHash': last_hash
                      }}
        request_manager = LokiRequestManager(target_snode, self, pubkey)
        return request_manager.make_request(url, parameters)

    def fetch_raw_messages(self, pubkey_list, last_hash):
        swarm_needed_ids = list(pubkey_list)
        for pubkey, swarm in self.swarm_cache.items():
            if len(swarm) > 0 and pubkey in swarm_needed_ids:
                swarm_needed_ids.remove(pubkey)
        self.get_swarms(swarm_needed_ids)
        requests = []
        messages_dict = {}
        for pubkey in pubkey_list:
            if pubkey not in messages_dict.keys():
                messages_dict[pubkey] = []
            hash_value = ""
            if pubkey in last_hash:
                hash_value = last_hash[pubkey][LASTHASH]
            req = self.get_raw_messages(pubkey, hash_value, 0)
            if req is not None:
                requests.append(req)
        num = 0
        try:
            for future in as_completed(requests):
                num += 1
                try:
                    response = future.result(timeout=defaultTimeout)
                    if response is None:
                        continue
                    data = response.result
                    session_id = response.session_id
                    if data is None:
                        self.swarm_cache[session_id].remove(response.target)
                        continue
                    message_json = data
                    if not message_json or 'messages' not in dict(message_json).keys():
                        self.logger.warn(session_id + " swarm mapping changed")
                        self.swarm_cache[session_id].remove(response.target)
                        self.handle_swarm_response(data, session_id)
                        continue
                    messages = list(message_json['messages'])
                    old_length = len(messages_dict[session_id])
                    new_length = len(messages)
                    if old_length == 0:
                        messages_dict[session_id] = messages
                    elif new_length > 0:
                        old_expiration = int(messages_dict[session_id][old_length - 1]['expiration'])
                        new_expiration = int(messages[new_length - 1]['expiration'])
                        if new_expiration > old_expiration:
                            messages_dict[session_id] = messages
                except:
                    pass
        except:
            self.logger.info(str(num) + " requests has been done in 60s")
        return messages_dict
