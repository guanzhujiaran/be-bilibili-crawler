from httpx import Response
from google.protobuf.json_format import MessageToDict  # 这是第三方包，不用管
import gzip
from google.protobuf.message import DecodeError
from log.base_log import BiliGrpcApi_logger


def raw_resp_content_2_dict(*, raw_resp: Response, protobuf_msg, is_gzip: bool = True) -> dict:
    log_func = None
    if is_gzip:
        grpc_msg_decompress: bytes = gzip.decompress(raw_resp.content[5:])
    else:
        grpc_msg_decompress: bytes = raw_resp.content[5:]
    try:
        protobuf_msg.ParseFromString(grpc_msg_decompress)
    except Exception as e:
        if is_gzip:
            raw_content_desc = f'Content hex (gzip, 5-byte prefix stripped): {grpc_msg_decompress.hex()}'
        else:
            raw_content_desc = f'Content hex (5-byte prefix stripped): {grpc_msg_decompress.hex()}'
        resp_dict = MessageToDict(protobuf_msg)
        if resp_dict:
            BiliGrpcApi_logger.warning(
                f'Partial parsing success for URL: {raw_resp.url}. Error: {e}. '
                f'Partially parsed message: {resp_dict}. {raw_content_desc}'
            )
            return resp_dict
        else:
            BiliGrpcApi_logger.error(
                f'Failed to parse gRPC message for URL: {raw_resp.url}. Error: {e}. '
                f'Headers: {raw_resp.headers}. Request body: {raw_resp.request.content!r}. '
                f'Response text: {raw_resp.text}. '
                f'{raw_content_desc}. Full response content hex: {raw_resp.content.hex()}'
            )
            raise DecodeError(f'Failed to parse gRPC message: {e}')
    resp_dict = MessageToDict(protobuf_msg)
    return resp_dict
