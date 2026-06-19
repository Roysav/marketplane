import grpc
from grpc_reflection.v1alpha import reflection
from marketplane.apiserver.v1 import apiserver_pb2, apiserver_pb2_grpc

from .service import Service
from .servicer import ApiserverServicer

_PORT = 50051


async def serve(service: Service) -> None:
    server = grpc.aio.server()
    apiserver_pb2_grpc.add_ApiserverServiceServicer_to_server(ApiserverServicer(service), server)
    reflection.enable_server_reflection(
        (apiserver_pb2.DESCRIPTOR.services_by_name["ApiserverService"].full_name, reflection.SERVICE_NAME),
        server,
    )
    server.add_insecure_port(f"[::]:{_PORT}")
    await server.start()
    await server.wait_for_termination()
