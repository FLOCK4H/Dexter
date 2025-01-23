from solders.transaction import VersionedTransaction
from solders.keypair import Keypair
from solders.pubkey import Pubkey as PublicKey
from solders import message
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solana.transaction import Transaction, AccountMeta, Instruction
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import get_associated_token_address, create_associated_token_account
import base58
from borsh_construct import CStruct, U64
from decimal import Decimal
import logging
import asyncio, json
from solders.compute_budget import set_compute_unit_price
from aiohttp import ClientSession
import time, requests

try:
    from .colors import *
    from .common_ import *
    from .utils import usd_to_lamports, lamports_to_tokens, usd_to_microlamports
except ImportError:
    from colors import *
    from common_ import *
    from utils import usd_to_lamports, lamports_to_tokens, usd_to_microlamports

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BUY_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "max_sol_cost" / U64
)

SELL_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "min_sol_output" / U64
)

# Instruction discriminator
BUY_DISCRIMINATOR = bytes([102, 6, 61, 18, 1, 218, 235, 234])
SELL_DISCRIMINATOR = bytes([51, 230, 133, 164, 1, 127, 131, 173])

def get_solana_price_usd():
    try:
        response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd')
        data = response.json()
        price = data['solana']['usd']
        print(f"{cc.YELLOW}Solana price: {price}")
        return str(price)
    except Exception:
        logging.info(f"{cc.RED}Failed to get Solana price from Coingecko{cc.RESET}")
        return '247.61'  # Fallback price

class PumpSwap:
    def __init__(self, session: ClientSession, priv_key: str, rpc_endpoint: str, parent = None):
        self.parent = parent
        self.session = session
        self.priv_key = Keypair.from_bytes(
                base58.b58decode(str(priv_key))
            )
        self.rpc_endpoint = rpc_endpoint
        self.async_client = AsyncClient(endpoint=rpc_endpoint)

    async def fetch_wallet_balance_sol(self):
        headers = {"Content-Type": "application/json"}
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getBalance",
            "params": [
                f"{WALLET}",
            ]
        }
        async with self.session.post(RPC_URL, json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                result = data.get('result')
                value = result.get('value')
                logging.info(f"{cc.BRIGHT}{cc.LIGHT_GREEN}| Wallet balance: {Decimal(value) / Decimal('1e9')} SOL")
                return value
            else:
                raise Exception(f"HTTP {resp.status}: {await resp.text()}")

    async def build_buy_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        token_amount: int,      # how many tokens to buy
        lamports_budget: int    # how many lamports to spend
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
            AccountMeta(pubkey=PublicKey.from_string("SysvarRent111111111111111111111111111111111"), is_signer=False, is_writable=False), # rent
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False), # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False)   # program
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
        lamports_min_output: int # minimum lamports you want to receive
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
            AccountMeta(pubkey=PublicKey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"), is_signer=False, is_writable=False),  # associatedTokenProgram
            AccountMeta(pubkey=PublicKey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"), is_signer=False, is_writable=False), # tokenProgram
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False),  # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False)    # program
        ]

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def make_check_ata(self, transaction_obj: Transaction, mint_address: PublicKey):
        """
        Check if the Associated Token Account (ATA) exists.
        If it doesn't, add an instruction to create it.
        """
        transaction_obj.add(
            create_associated_token_account(
                payer=self.priv_key.pubkey(),
                owner=self.priv_key.pubkey(),
                mint=mint_address
            )
        )

        return transaction_obj

    async def pump_buy(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            sol_amount: int,
            token_amount: int = 0,
            sim: bool = False,
            priority_micro_lamports: int = 0,
        ):

        transaction = Transaction()

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        # 1) (Optional) Add Compute Budget instructions for priority fee
        # ---------------------------------------------------------------------
        if priority_micro_lamports > 0:
            transaction.add(
                set_compute_unit_price(
                    priority_micro_lamports
                )
            )
            logging.info(f"Added priority fee instructions with {priority_micro_lamports} micro-lamports per CU.")

        # ---------------------------------------------------------------------
        # 2) Check if associated token account exists. If not, create it.
        # ---------------------------------------------------------------------
    
        transaction = await self.make_check_ata(transaction, mint_address)

        # ---------------------------------------------------------------------
        # 3) Add buy instruction
        #    'token_amount' => how many tokens you want to buy
        #    'sol_amount' => lamports you can spend
        # ---------------------------------------------------------------------
        fee_recipient = PublicKey.from_string("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM")
        buy_ix = await self.build_buy_instruction(
            mint_address,
            bonding_curve_pda,
            fee_recipient,
            token_amount,
            # slippage, 1.99x
            int(sol_amount * 1.52)
        )
        transaction.add(buy_ix)

        # ---------------------------------------------------------------------
        # 4) Recent blockhash and fee payer
        # ---------------------------------------------------------------------
        try:
            latest_blockhash_resp = await self.async_client.get_latest_blockhash(commitment="processed")
            transaction.recent_blockhash = latest_blockhash_resp.value.blockhash
            transaction.fee_payer = self.priv_key.pubkey()
        except Exception as e:
            logging.error(f"Failed to fetch latest blockhash: {e}")
            raise

        # ---------------------------------------------------------------------
        # 5) Sign, optionally simulate, and send the transaction
        # ---------------------------------------------------------------------
        try:
            compiled_message = transaction.compile_message()
            signed_txn = VersionedTransaction.populate(
                compiled_message,
                [self.priv_key.sign_message(message.to_bytes_versioned(compiled_message))]
            )

            if sim:
                simulate_resp = await self.async_client.simulate_transaction(signed_txn)
                logging.info(f"Simulation result: {simulate_resp}")
            
            opts = TxOpts(skip_preflight=True, max_retries=0, skip_confirmation=True)
            result = await self.async_client.send_raw_transaction(bytes(signed_txn), opts=opts)
            logging.info(f"Transaction result: {result}")
            return result
        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            raise

    async def pump_sell(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            token_amount: int,
            lamports_min_output: int,
            sim: bool = False,
            priority_micro_lamports: int = 0
        ):

        logging.info("Preparing sell transaction...")

        transaction = Transaction()

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        if priority_micro_lamports > 0:
            transaction.add(
                set_compute_unit_price(
                    priority_micro_lamports
                )
            )
            logging.info(f"Added priority fee instructions with {priority_micro_lamports} micro-lamports per CU.")

        # Add the sell instruction
        fee_recipient = PublicKey.from_string("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicfhtW4xC9iM")
        sell_ix = await self.build_sell_instruction(
            mint=mint_address,
            bonding_curve=bonding_curve_pda,
            fee_recipient=fee_recipient,
            token_amount=token_amount,
            lamports_min_output=lamports_min_output
        )
        transaction.add(sell_ix)

        # Fetch recent blockhash and set fee payer
        try:
            latest_blockhash_resp = await self.async_client.get_latest_blockhash(commitment="processed")
            transaction.recent_blockhash = latest_blockhash_resp.value.blockhash
            transaction.fee_payer = self.priv_key.pubkey()
        except Exception as e:
            logging.error(f"Failed to fetch latest blockhash: {e}")
            raise

        # Sign, (optionally simulate), and send
        try:
            compiled_message = transaction.compile_message()
            signed_txn = VersionedTransaction.populate(
                compiled_message,
                [self.priv_key.sign_message(message.to_bytes_versioned(compiled_message))]
            )

            if sim:
                simulate_resp = await self.async_client.simulate_transaction(signed_txn)
                logging.info(f"Simulation result: {simulate_resp}")

            opts = TxOpts(skip_preflight=True, max_retries=0, skip_confirmation=True)
            result = await self.async_client.send_raw_transaction(bytes(signed_txn), opts=opts)
            logging.info(f"Sell transaction result: {result}")
            return result
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
        """
        Gracefully close the permanent clients.
        """
        await self.async_client.close()
        await self.session.close()
        logging.info("PumpSwap clients successfully closed.")
