from py_clob_client_v2 import ClobClient, SignatureTypeV2

from .config import SignerConfig

_SIGNATURE_TYPES = {
    "EOA": SignatureTypeV2.EOA,
    "POLY_PROXY": SignatureTypeV2.POLY_PROXY,
    "POLY_GNOSIS_SAFE": SignatureTypeV2.POLY_GNOSIS_SAFE,
}


def build_clob_client(clob_api_url: str, chain_id: int, signer: SignerConfig) -> ClobClient:
    creds = ClobClient(clob_api_url, chain_id, key=signer.private_key).create_or_derive_api_key()
    return ClobClient(
        clob_api_url,
        chain_id,
        key=signer.private_key,
        creds=creds,
        signature_type=_SIGNATURE_TYPES[signer.signature_type],
        funder=signer.funder,
    )
