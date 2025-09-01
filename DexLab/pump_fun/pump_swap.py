from solders.transaction import VersionedTransaction # type: ignore
from solders.keypair import Keypair # type: ignore
from solders.pubkey import Pubkey as PublicKey # type: ignore
from solders import message
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solders.transaction import Transaction # type: ignore
from solders.instruction import AccountMeta, Instruction # type: ignore
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import get_associated_token_address, create_associated_token_account
import base58
from borsh_construct import CStruct, U64
import logging
import asyncio, json
from solders.compute_budget import set_compute_unit_price # type: ignore
from solders.system_program import transfer, TransferParams # type: ignore
from aiohttp import ClientSession
import time, requests
try: from .pump_bond import check_has_migrated
except: from .pump_bond import check_has_migrated

from solana.rpc.commitment import Processed
from solders.message    import MessageV0 # type: ignore

PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
GLOBAL_VOLUME_ACCUMULATOR = "Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y"
FEE_CONFIG = "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt"
FEE_PROGRAM = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"

BUY_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "max_sol_cost" / U64
)

SELL_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "min_sol_output" / U64
)

BUY_DISCRIMINATOR = bytes([102, 6, 61, 18, 1, 218, 235, 234])
SELL_DISCRIMINATOR = bytes([51, 230, 133, 164, 1, 127, 131, 173])

suppress_logs = [
    "socks",
    "requests",
    "httpx",
    "trio.async_generator_errors",
    "trio",
    "trio.abc.Instrument",
    "trio.abc",
    "trio.serve_listeners",
    "httpcore.http11",
    "httpcore",
    "httpcore.connection",
    "httpcore.proxy",
]

# Set all of them to CRITICAL (no logs)
for log_name in suppress_logs:
    logging.getLogger(log_name).setLevel(logging.CRITICAL)
    logging.getLogger(log_name).handlers.clear()
    logging.getLogger(log_name).propagate = False

def get_solana_price_usd():
    try:
        response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd')
        data = response.json()
        price = data['solana']['usd']
        logging.info(f"Solana price: {price}")
        return str(price)
    except Exception:
        logging.info(f"Failed to get Solana price from Coingecko")
        time.sleep(5)
        return get_solana_price_usd()

class PumpFun:
    def __init__(self, session: ClientSession, priv_key: str, async_client: AsyncClient):
        self.session = session
        self.priv_key = Keypair.from_bytes(
                base58.b58decode(str(priv_key))
            )
        self.async_client = async_client

    def _derive_uva_pda(self, payer: PublicKey):
        user_acc, _ = PublicKey.find_program_address(
            [b"user_volume_accumulator", bytes(payer)], PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
        )
        return user_acc

    async def build_buy_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        token_amount: int,      # how many tokens to buy
        lamports_budget: int,    # how many lamports to spend
        vault: PublicKey
    ) -> Instruction:
        instruction_data = BUY_DISCRIMINATOR + BUY_INSTRUCTION_SCHEMA.build({
            "amount": token_amount,
            "max_sol_cost": lamports_budget
        })

        buyer = self.priv_key.pubkey()

        accounts = [
            AccountMeta(pubkey=PublicKey.from_string("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"), is_signer=False, is_writable=False), # global
            AccountMeta(pubkey=fee_recipient, is_signer=False, is_writable=True),  # feeRecipient
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),         # mint
            AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True), # bondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(bonding_curve, mint, TOKEN_PROGRAM_ID),
                is_signer=False,
                is_writable=True
            ),                                                                    # associatedBondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(buyer, mint, TOKEN_PROGRAM_ID),
                is_signer=False,
                is_writable=True
            ),                                                                    # associatedUser
            AccountMeta(pubkey=buyer, is_signer=True, is_writable=True),         # user
            AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False), # systemProgram
            AccountMeta(pubkey=PublicKey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"), is_signer=False, is_writable=False), # tokenProgram
            AccountMeta(pubkey=PublicKey.from_string(str(vault)), is_signer=False, is_writable=True), # vault
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False), # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False),   # program
            AccountMeta(pubkey=PublicKey.from_string(GLOBAL_VOLUME_ACCUMULATOR), is_signer=False, is_writable=True), # globalVolumeAccumulator
            AccountMeta(pubkey=self._derive_uva_pda(buyer), is_signer=False, is_writable=True), # userVolumeAccumulator
            AccountMeta(pubkey=PublicKey.from_string(FEE_CONFIG), is_signer=False, is_writable=False), # feeConfig
            AccountMeta(pubkey=PublicKey.from_string(FEE_PROGRAM), is_signer=False, is_writable=False), # feeProgram
        ]

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def build_sell_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        token_amount: int,       # how many tokens to sell
        lamports_min_output: int, # minimum lamports you want to receive
        vault: PublicKey
    ) -> Instruction:
        instruction_data = SELL_DISCRIMINATOR + SELL_INSTRUCTION_SCHEMA.build({
            "amount": token_amount,
            "min_sol_output": lamports_min_output
        })

        user = self.priv_key.pubkey()

        # The IDL's account list for sell:
        accounts = [
            AccountMeta(pubkey=PublicKey.from_string("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"), is_signer=False, is_writable=False),  # global
            AccountMeta(pubkey=fee_recipient, is_signer=False, is_writable=True),  # feeRecipient
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),          # mint
            AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),  # bondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(bonding_curve, mint, TOKEN_PROGRAM_ID),
                is_signer=False,
                is_writable=True
            ),                                                                     # associatedBondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(user, mint, TOKEN_PROGRAM_ID),
                is_signer=False,
                is_writable=True
            ),                                                                     # associatedUser
            AccountMeta(pubkey=user, is_signer=True, is_writable=True),           # user
            AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False), # systemProgram
            AccountMeta(pubkey=PublicKey.from_string(str(vault)), is_signer=False, is_writable=True),  # vault
            AccountMeta(pubkey=PublicKey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"), is_signer=False, is_writable=False), # tokenProgram
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False),  # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False),    # program
            AccountMeta(pubkey=PublicKey.from_string(FEE_CONFIG), is_signer=False, is_writable=False), # feeConfig
            AccountMeta(pubkey=PublicKey.from_string(FEE_PROGRAM), is_signer=False, is_writable=False), # feeProgram
        ]

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def check_ata_exists(self, owner: PublicKey, mint: PublicKey) -> bool:
        """
        Check if the associated token account (ATA) exists on-chain.
        """
        ata_address = get_associated_token_address(owner, mint, TOKEN_PROGRAM_ID)

        try:
            response = await self.async_client.get_account_info(ata_address)
            if response.value:
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Error checking ATA existence: {e}")
            return False

    async def make_check_ata(self, instructions: list, mint_address: PublicKey):
        """
        Check if the Associated Token Account (ATA) exists.
        If it doesn't, add an instruction to create it.
        """
        owner = self.priv_key.pubkey()
        
        # Check if the ATA exists
        ata_exists = await self.check_ata_exists(owner, mint_address)
        
        if not ata_exists:
            instructions.append(
                create_associated_token_account(
                    payer=owner,
                    owner=owner,
                    mint=mint_address
                )
            )

        return instructions

    def get_creator_vault(self, creator):
        creator_vault_pda, _ = PublicKey.find_program_address(
            [b"creator-vault", bytes(PublicKey.from_string(creator))],
            PublicKey.from_string(PUMP_FUN)
        )
        return creator_vault_pda

    async def pump_buy(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            sol_amount: int,
            creator: str,
            token_amount: int = 0,
            sim: bool = False,
            priority_micro_lamports: int = 0,
            slippage: float = 1.3, # MAX: 1.99
            skip_ata_check: bool = False,
        ):

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        has_migrated = await check_has_migrated(
            self.async_client,
            bonding_curve_pda
        )
        if has_migrated:
            return "migrated"

        if priority_micro_lamports > 0:
            instructions.append(
                set_compute_unit_price(
                    priority_micro_lamports
                )
            )

        if not skip_ata_check:
            instructions = await self.make_check_ata(instructions, mint_address)

        fee_recipient = PublicKey.from_string("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV")
        vault = self.get_creator_vault(creator)
        buy_ix = await self.build_buy_instruction(
            mint_address,
            bonding_curve_pda,
            fee_recipient,
            token_amount,
            # slippage, 1.99x
            int(sol_amount * slippage),
            vault
        )
        instructions.append(buy_ix)

        try:
            latest_blockhash = (await self.async_client.get_latest_blockhash(commitment=Processed)).value.blockhash
            msg = MessageV0.try_compile(
                payer = self.priv_key.pubkey(),
                instructions = instructions,
                address_lookup_table_accounts = [],
                recent_blockhash = latest_blockhash
            )
            tx = VersionedTransaction(msg, [self.priv_key])
        except Exception as e:
            logging.error(f"Failed to fetch latest blockhash: {e}")
            raise

        try:
            if sim:
                simulate_resp = await self.async_client.simulate_transaction(tx)
                logging.info(f"Simulation result: {simulate_resp}")
            
            opts = TxOpts(skip_preflight=True, max_retries=0, skip_confirmation=True)
            result = await self.async_client.send_transaction(tx, opts=opts)
            result_json = result.to_json()
            transaction_id = json.loads(result_json).get('result')
            return transaction_id
        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            raise

    async def pump_sell(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            token_amount: int,
            lamports_min_output: int,
            creator: str,
            sim: bool = False,
            priority_micro_lamports: int = 0
        ):

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)
        
        has_migrated = await check_has_migrated(
            self.async_client,
            bonding_curve_pda
        )
        if has_migrated:
            return "migrated"
        
        if priority_micro_lamports > 0:
            instructions.append(
                set_compute_unit_price(
                    priority_micro_lamports
                )
            )

        fee_recipient = PublicKey.from_string("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV")
        sell_ix = await self.build_sell_instruction(
            mint=mint_address,
            bonding_curve=bonding_curve_pda,
            fee_recipient=fee_recipient,
            token_amount=token_amount,
            lamports_min_output=lamports_min_output,
            vault=self.get_creator_vault(creator)
        )
        instructions.append(sell_ix)

        try:
            latest_blockhash = (await self.async_client.get_latest_blockhash(commitment=Processed)).value.blockhash
            msg = MessageV0.try_compile(
                payer = self.priv_key.pubkey(),
                instructions = instructions,
                address_lookup_table_accounts = [],
                recent_blockhash = latest_blockhash
            )
            tx = VersionedTransaction(msg, [self.priv_key])
        except Exception as e:
            logging.error(f"Failed to fetch latest blockhash: {e}")
            raise

        try:
            if sim:
                simulate_resp = await self.async_client.simulate_transaction(tx)
                logging.info(f"Simulation result: {simulate_resp}")

            opts = TxOpts(skip_preflight=True, max_retries=0, skip_confirmation=True)
            result = await self.async_client.send_transaction(tx, opts=opts)
            result_json = result.to_json()
            transaction_id = json.loads(result_json).get('result')
            return transaction_id
        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            raise

    async def getTransaction(self, tx_id: str, session: ClientSession):
        start_time = time.time()
        attempt = 1
        try:
            while attempt < 25:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        tx_id,
                        {
                            "commitment": "confirmed",
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]
                }
                headers = {
                    "Content-Type": "application/json"
                }

                async with session.post(self.rpc_endpoint, json=payload, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        logging.error(f"HTTP Error {response.status}: {await response.text()}")
                        raise Exception(f"HTTP Error {response.status}")

                    data = await response.json()
                    logging.info(f"Attempt {attempt}")

                    if data and data.get('result') is not None:
                        logging.info(f"Elapsed: {time.time() - start_time:.2f}s")
                        result = data['result']
                        return result

                await asyncio.sleep(0.5)
                attempt += 1
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    async def close(self):
        await self.async_client.close()
        await self.session.close()