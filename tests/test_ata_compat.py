from __future__ import annotations

from unittest import TestCase

from solders.pubkey import Pubkey as PublicKey
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import TOKEN_2022_PROGRAM_ID, create_idempotent_associated_token_account

from DexLab.utils import create_compatible_idempotent_associated_token_account


class AtaCompatTests(TestCase):
    def test_legacy_program_matches_installed_helper_shape(self) -> None:
        payer = PublicKey.new_unique()
        owner = PublicKey.new_unique()
        mint = PublicKey.new_unique()

        expected = create_idempotent_associated_token_account(
            payer=payer,
            owner=owner,
            mint=mint,
        )
        actual = create_compatible_idempotent_associated_token_account(
            payer=payer,
            owner=owner,
            mint=mint,
            token_program_id=PublicKey.from_string(str(TOKEN_PROGRAM_ID)),
        )

        self.assertEqual(actual.program_id, expected.program_id)
        self.assertEqual(actual.data, expected.data)
        self.assertEqual(actual.accounts, expected.accounts)

    def test_token_2022_uses_requested_token_program(self) -> None:
        payer = PublicKey.new_unique()
        owner = PublicKey.new_unique()
        mint = PublicKey.new_unique()

        actual = create_compatible_idempotent_associated_token_account(
            payer=payer,
            owner=owner,
            mint=mint,
            token_program_id=PublicKey.from_string(str(TOKEN_2022_PROGRAM_ID)),
        )

        self.assertEqual(actual.data, bytes([1]))
        self.assertEqual(actual.accounts[-1].pubkey, PublicKey.from_string(str(TOKEN_2022_PROGRAM_ID)))
