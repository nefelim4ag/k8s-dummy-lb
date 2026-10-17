#!/usr/bin/env python3
import os
import re
import ssl
import time
import boto3
import logging
import queue
import threading
import urllib3

from collections import namedtuple
from botocore.exceptions import BotoCoreError, ClientError
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

logging.basicConfig(
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}',
    level=logging.INFO,
    force=True,
    datefmt='%Y-%m-%d %H:%M:%S',
)

try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
        logging.info("Using kubeconfig")
    except config.ConfigException:
        # trunk-ignore(ruff/B904)
        raise Exception("Could not configure Kubernetes Python client")


def make_api_client():
    configuration = client.Configuration.get_default_copy()

    context = ssl.create_default_context(cafile=configuration.ssl_ca_cert)
    # Python 3.13 enables VERIFY_X509_STRICT, which rejects CA certificates
    # without an Authority Key Identifier (the EKS cluster CA has none).
    # The chain is still fully verified.
    context.verify_flags &= ~ssl.VERIFY_X509_STRICT

    if configuration.cert_file:
        context.load_cert_chain(configuration.cert_file, configuration.key_file)

    api_client = client.ApiClient(configuration)
    api_client.rest_client.pool_manager = urllib3.PoolManager(
        ssl_context=context,
        maxsize=configuration.connection_pool_maxsize or 8,
    )
    return api_client


api_client = make_api_client()
# status patch must be sent as merge-patch, so the ingress list gets replaced
api_client.set_default_header('Content-Type', 'application/merge-patch+json')
v1 = client.CoreV1Api(api_client)
discovery_v1 = client.DiscoveryV1Api(api_client)

RECONCILE_INTERVAL = int(os.getenv('RECONCILE_INTERVAL', '15'))
IPV6_CACHE_TTL = int(os.getenv('IPV6_CACHE_TTL', '300'))
DRY_RUN = os.getenv('DRY_RUN', 'false') == 'true'

WATCH_TIMEOUT = 600  # 10 minutes, then relist + rewatch
SERVICE_NAME_LABEL = 'kubernetes.io/service-name'
AWS_PROVIDER_ID = re.compile(r'^aws:///(?P<zone>[a-z0-9-]+)/(?P<instance_id>i-[0-9a-f]+)$')

# What a watcher puts into the queue.
# kind: services | nodes | endpointslices
# type: RESYNC (obj is a list with all items) | ADDED | MODIFIED | DELETED
Event = namedtuple('Event', ['kind', 'type', 'obj'])

NodeInfo = namedtuple('NodeInfo', ['name', 'ready', 'provider_id', 'external_ips'])
ServiceInfo = namedtuple('ServiceInfo', ['namespace', 'name', 'ips'])


def ip_sort_key(ip):
    # IPv4 first, then IPv6
    return (':' in ip, ip)


# ---------------------------------------------------------------------------
# State objects. They are only ever touched by the main thread:
# watchers just put events into the queue, main loop feeds them to consume().
# ---------------------------------------------------------------------------
class Consumer:
    def __init__(self):
        self._state = {}
        self.synced = False

    def consume(self, event):
        if event.type == 'RESYNC':
            # Throw everything away and rebuild. This also drops objects
            # which were deleted while the watch was down.
            self._state = {}
            for item in event.obj:
                self.apply('ADDED', item)
            self.synced = True
            return

        self.apply(event.type, event.obj)

    def apply(self, event_type, obj):
        raise NotImplementedError


class NodesConsumer(Consumer):
    def get_node(self, name):
        return self._state.get(name)

    def apply(self, event_type, node):
        name = node.metadata.name

        if event_type == 'DELETED':
            self._state.pop(name, None)
            return

        ready = False
        for condition in node.status.conditions or []:
            if condition.type == 'Ready':
                ready = condition.status == 'True'

        external_ips = []
        for address in node.status.addresses or []:
            if address.type == 'ExternalIP':
                external_ips.append(address.address)

        self._state[name] = NodeInfo(
            name,
            ready,
            node.spec.provider_id,
            tuple(external_ips))


class EndpointSlicesConsumer(Consumer):
    # state: (namespace, service name) -> {slice name: ((node name, ready), ...)}
    def iter_endpoints(self, namespace, name):
        by_slice = self._state.get((namespace, name), {})
        for entries in by_slice.values():
            for node_name, ready in entries:
                yield node_name, ready

    def apply(self, event_type, endpoint_slice):
        labels = endpoint_slice.metadata.labels or {}
        service_name = labels.get(SERVICE_NAME_LABEL)
        if service_name is None:
            return

        key = (endpoint_slice.metadata.namespace, service_name)
        slice_name = endpoint_slice.metadata.name

        if event_type == 'DELETED':
            by_slice = self._state.get(key)
            if by_slice is not None:
                by_slice.pop(slice_name, None)
                if not by_slice:
                    del self._state[key]
            return

        entries = []
        for endpoint in endpoint_slice.endpoints or []:
            if not endpoint.node_name:
                continue

            # ready == None means "unknown", API docs say consumers
            # must interpret that as ready
            ready = True
            if endpoint.conditions is not None and endpoint.conditions.ready is False:
                ready = False

            entries.append((endpoint.node_name, ready))

        self._state.setdefault(key, {})[slice_name] = tuple(entries)


class ServiceConsumer(Consumer):
    def __init__(self, lb_class):
        super().__init__()
        self._lb_class = lb_class

    def iter_services(self):
        for service in self._state.values():
            yield service

    def apply(self, event_type, service):
        key = (service.metadata.namespace, service.metadata.name)

        if event_type == 'DELETED':
            self._state.pop(key, None)
            return

        if service.spec.type != 'LoadBalancer':
            self._state.pop(key, None)
            return

        if getattr(service.spec, 'load_balancer_class', None) != self._lb_class:
            self._state.pop(key, None)
            return

        ips = []
        load_balancer = service.status.load_balancer if service.status else None
        if load_balancer is not None and load_balancer.ingress:
            for ingress in load_balancer.ingress:
                if ingress.ip:
                    ips.append(ingress.ip)

        self._state[key] = ServiceInfo(
            service.metadata.namespace,
            service.metadata.name,
            tuple(sorted(ips, key=ip_sort_key)))


# ---------------------------------------------------------------------------
# AWS: IPv6 addresses by instance id. Resolved lazily during reconcile,
# in batches, with a TTL cache.
# ---------------------------------------------------------------------------
class Ipv6Resolver:
    def __init__(self, source='auto', ttl=300):
        self._source = source
        self._ttl = ttl
        self._ec2_clients = {}
        self._cache = {}  # instance id -> (expires at, (ipv6, ...))

    def get_aws_instance(self, node):
        # providerID is aws:///eu-west-1a/i-0123456789abcdef0
        # Fargate nodes and non-AWS clusters do not match
        if self._source == 'none':
            return None, None

        match = AWS_PROVIDER_ID.match(node.provider_id or '')
        if match is None:
            return None, None

        region = match.group('zone')[:-1]
        return region, match.group('instance_id')

    def get_ec2_client(self, region):
        if region not in self._ec2_clients:
            self._ec2_clients[region] = boto3.client('ec2', region_name=region)
        return self._ec2_clients[region]

    def get_instance_ipv6(self, instance):
        # IPv6 addresses of the primary network interface
        result = []
        for interface in instance.get('NetworkInterfaces', []):
            if interface.get('Attachment', {}).get('DeviceIndex') != 0:
                continue
            for address in interface.get('Ipv6Addresses', []):
                result.append(address['Ipv6Address'])
        return tuple(result)

    def refresh(self, nodes):
        now = time.monotonic()

        missing = {}  # region -> [instance id, ...]
        for node in nodes:
            region, instance_id = self.get_aws_instance(node)
            if instance_id is None:
                continue

            cached = self._cache.get(instance_id)
            if cached is not None and cached[0] > now:
                continue

            missing.setdefault(region, []).append(instance_id)

        for region, instance_ids in missing.items():
            for instance_id in instance_ids:
                try:
                    response = self.get_ec2_client(region).describe_instances(InstanceIds=[instance_id])
                except (ClientError, BotoCoreError) as e:
                    self._cache[instance_id] = (now + self._ttl, tuple())
                    logging.error(f"Can't describe instances in {region}: {e}")
                    continue

                found = set()
                for reservation in response['Reservations']:
                    for instance in reservation['Instances']:
                        found.add(instance['InstanceId'])
                        self._cache[instance['InstanceId']] = (
                            now + self._ttl, self.get_instance_ipv6(instance))

                if instance_id not in found:
                    self._cache[instance_id] = (now + self._ttl, tuple())

    def iter_ipv6(self, node):
        region, instance_id = self.get_aws_instance(node)
        if instance_id is None:
            return

        cached = self._cache.get(instance_id)
        if cached is None:
            return

        for ip in cached[1]:
            yield ip


def get_list_func(kind, namespace):
    if kind == 'nodes':
        return v1.list_node, {}

    if kind == 'services':
        if namespace:
            return v1.list_namespaced_service, {'namespace': namespace}
        return v1.list_service_for_all_namespaces, {}

    if kind == 'endpointslices':
        # only slices which belong to a service
        scope = {'label_selector': SERVICE_NAME_LABEL}
        if namespace:
            scope['namespace'] = namespace
            return discovery_v1.list_namespaced_endpoint_slice, scope
        return discovery_v1.list_endpoint_slice_for_all_namespaces, scope

    raise ValueError(f"Unknown kind {kind}")


class Watcher:
    def __init__(self, kind, events_bus, namespace=None):
        self.kind = kind
        self.events_bus = events_bus
        self.list_func, self.scope = get_list_func(kind, namespace)
        self._run = True
        self.thread = threading.Thread(target=self.watch_kind, daemon=True)

    def start(self):
        self.thread.start()

    def stop(self):
        self._run = False

    def watch_kind(self):
        while self._run:
            try:
                response = self.list_func(**self.scope)
                self.events_bus.put(Event(self.kind, 'RESYNC', response.items))

                w = watch.Watch()
                for event in w.stream(
                        self.list_func,
                        resource_version=response.metadata.resource_version,
                        timeout_seconds=WATCH_TIMEOUT,
                        **self.scope):
                    if event['type'] not in ('ADDED', 'MODIFIED', 'DELETED'):
                        break
                    self.events_bus.put(Event(self.kind, event['type'], event['object']))
            except ApiException as e:
                logging.error(f"Watch {self.kind} failed: {e.status} {e.reason}. Restarting...")
                time.sleep(5)
            except Exception as e:
                logging.error(f"Watch {self.kind} failed: {e}. Restarting...")
                time.sleep(5)


# ---------------------------------------------------------------------------
# Reconcile: build the target state from the in-memory state, patch services
# ---------------------------------------------------------------------------
class Reconciler:
    def __init__(self, services, endpoints, nodes, ipv6, dry_run=False):
        self.services = services
        self.endpoints = endpoints
        self.nodes = nodes
        self.ipv6 = ipv6
        self.dry_run = dry_run

    def select_nodes(self, service):
        ready_nodes = set()
        all_nodes = set()

        for node_name, endpoint_ready in self.endpoints.iter_endpoints(service.namespace, service.name):
            node = self.nodes.get_node(node_name)
            if node is None:
                continue

            all_nodes.add(node_name)
            if endpoint_ready and node.ready:
                ready_nodes.add(node_name)

        if ready_nodes:
            return ready_nodes

        if all_nodes:
            logging.warning(f"{service.namespace}/{service.name}: Panic mode: no ready endpoints found, returning all")
            return all_nodes

        return set()

    def iter_node_ips(self, node):
        for ip in node.external_ips:
            yield ip

        for ip in self.ipv6.iter_ipv6(node):
            yield ip

    def publish(self, service, ips):
        logging.info(f"{service.namespace}/{service.name}: {list(service.ips)} -> {ips}")
        if self.dry_run:
            return

        body = {'status': {'loadBalancer': {'ingress': []}}}
        for ip in ips:
            body['status']['loadBalancer']['ingress'].append({'ip': ip})

        try:
            v1.patch_namespaced_service_status(service.name, service.namespace, body)
        except ApiException as e:
            logging.error(f"{service.namespace}/{service.name}: Can't patch status: {e.status} {e.reason}")

    def reconcile_service(self, service):
        node_names = self.select_nodes(service)
        if not node_names:
            logging.warning(f"{service.namespace}/{service.name}: no endpoints found, leaving status as is")
            return

        selected_nodes = []
        for node_name in sorted(node_names):
            selected_nodes.append(self.nodes.get_node(node_name))

        self.ipv6.refresh(selected_nodes)

        unique_ips = set()
        for node in selected_nodes:
            for ip in self.iter_node_ips(node):
                unique_ips.add(ip)

        if not unique_ips:
            logging.warning(f"{service.namespace}/{service.name}: no IP addresses found, leaving status as is")
            return

        ips = tuple(sorted(unique_ips, key=ip_sort_key))
        if ips == service.ips:
            return

        self.publish(service, ips)

    def reconcile(self):
        for service in self.services.iter_services():
            try:
                self.reconcile_service(service)
            except Exception as e:
                logging.error(f"{service.namespace}/{service.name}: reconcile failed: {e}")


def all_synced(consumers):
    for consumer in consumers.values():
        if not consumer.synced:
            return False
    return True


def main():
    lb_class = os.getenv('LB_CLASS', 'controller.io/node-ip')
    namespace = os.getenv('NAMESPACE')
    ipv6_source = os.getenv('NODE_IPV6_SOURCE', 'auto')  # auto | none

    logging.info(f"Starting: class={lb_class} namespace={namespace or '<all>'} ipv6={ipv6_source} dry_run={DRY_RUN}")

    events_bus = queue.Queue()
    consumers = {
        "nodes": NodesConsumer(),
        "services": ServiceConsumer(lb_class),
        "endpointslices": EndpointSlicesConsumer(),
    }
    watchers = [
        Watcher("nodes", events_bus),
        Watcher("services", events_bus, namespace),
        Watcher("endpointslices", events_bus, namespace),
    ]
    reconciler = Reconciler(
        consumers["services"],
        consumers["endpointslices"],
        consumers["nodes"],
        Ipv6Resolver(ipv6_source, IPV6_CACHE_TTL),
        DRY_RUN)

    for watcher in watchers:
        watcher.start()

    next_reconcile = time.monotonic() + RECONCILE_INTERVAL
    try:
        while True:
            timeout = max(0.0, next_reconcile - time.monotonic())
            try:
                event = events_bus.get(timeout=timeout)
            except queue.Empty:
                event = None

            if event is not None:
                try:
                    consumers[event.kind].consume(event)
                except Exception as e:
                    logging.error(f"Can't apply {event.kind} {event.type}: {e}")

            if time.monotonic() < next_reconcile:
                continue

            next_reconcile = time.monotonic() + RECONCILE_INTERVAL
            if not all_synced(consumers):
                logging.info("Waiting for initial sync of all watchers")
                continue

            reconciler.reconcile()
    finally:
        for watcher in watchers:
            watcher.stop()


if __name__ == "__main__":
    main()
