import logging

import grpc
from grpc_reflection.v1alpha import reflection
from marketplane.apiserver.v1 import apiserver_pb2, apiserver_pb2_grpc

from .service import Service
from .servicer import ApiserverServicer

logger = logging.getLogger(__name__)


async def serve(service: Service, *, address: str, max_message_bytes: int) -> None:
    server = grpc.aio.server(options=[
        ("grpc.max_send_message_length", max_message_bytes),
        ("grpc.max_receive_message_length", max_message_bytes),
    ])
    apiserver_pb2_grpc.add_ApiserverServiceServicer_to_server(ApiserverServicer(service), server)
    reflection.enable_server_reflection(
        (apiserver_pb2.DESCRIPTOR.services_by_name["ApiserverService"].full_name, reflection.SERVICE_NAME),
        server,
    )
    server.add_insecure_port(address)
    await server.start()
    logger.info("apiserver listening on %s", address)
    await server.wait_for_termination()
