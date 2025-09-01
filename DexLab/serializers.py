from borsh_construct import U8, U64, Bool, I64, String, CStruct
from base58 import b58encode
import base64
import sys

class Interpreters:
    def __init__(self):
        self.pumpStructs = self.pumpStructs()

    class pumpStructs:
        def __init__(self):
            self.TransactionData = CStruct(
                "discriminator" / U8[8],
                "mint" / U8[32],
                "sol_amount" / U64,
                "token_amount" / U64,
                "is_buy" / Bool,
                "user" / U8[32],
                "timestamp" / I64,
                "virtual_sol_reserves" / U64,
                "virtual_token_reserves" / U64,
            )
            
            self.CreationData = CStruct(
                "discriminator" / U8[8],
                "name" / String,
                "symbol" / String,
                "uri" / String,
                "mint" / U8[32],
                "bonding_curve" / U8[32],
                "user" / U8[32],
            )

    def parse_pumpfun_transaction(self, b64_data):
        try:
            data = base64.b64decode(b64_data)
            tx_parsed = self.parse_data(data, self.pumpStructs.TransactionData)
            decoded_data = {
                "mint": self.decode_base58_field(tx_parsed.mint),
                "user": self.decode_base58_field(tx_parsed.user),
                "sol_amount": tx_parsed.sol_amount,
                "token_amount": tx_parsed.token_amount,
                "is_buy": tx_parsed.is_buy,
                "timestamp": tx_parsed.timestamp,
                "virtual_sol_reserves": tx_parsed.virtual_sol_reserves,
                "virtual_token_reserves": tx_parsed.virtual_token_reserves,
            }
            return decoded_data
        except Exception as e:
            sys.stdout.write(f"\nError parsing transaction data ({b64_data}): {str(e)}")
            sys.stdout.flush()
    
    def parse_pumpfun_creation(self, b64_data):
        try:
            data = base64.b64decode(b64_data)
            creation_parsed = self.parse_data(data, self.pumpStructs.CreationData)
            decoded_data = {
                "name": creation_parsed.name,
                "symbol": creation_parsed.symbol,
                "uri": creation_parsed.uri,
                "mint": self.decode_base58_field(creation_parsed.mint),
                "bonding_curve": self.decode_base58_field(creation_parsed.bonding_curve),
                "user": self.decode_base58_field(creation_parsed.user),
            }
            return decoded_data
        except Exception as e:
            sys.stdout.write("\nError parsing transaction data: " + str(e))
            sys.stdout.flush()
        
    @staticmethod
    def decode_base58_field(field_bytes):
        return b58encode(bytes(field_bytes)).decode("utf-8")

    @staticmethod
    def parse_data(data_bytes, schema):
        parsed_data = schema.parse(data_bytes)
        return parsed_data