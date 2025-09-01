# Description: This file contains all the settings for the Dexter.
from decimal import Decimal
# =================
# TRUST FACTOR PART
# =================

# !IMPORTANT: Tweak these values to adjust the trust-factor algorithm
# Minimum total swaps on all tokens for creators with more than 2 mints
# Scale: 500 HIGHRISK, 1000 MEDIUMRISK, 2500 LOWRISK
TOTAL_SWAPS_ABOVE_2_MINTS = 1 

# Minimum total swaps on all tokens for creators with 1 mint
# Scale: 500 HIGHRISK, 1000 MEDIUMRISK, 2000 LOWRISK
TOTAL_SWAPS_1_MINT = 1 

# Median peak market cap for creators with more than 2 mints
# Scale: 5000 HIGHRISK, 10000 MEDIUMRISK, 35000 LOWRISK
MEDIAN_PEAK_MC_ABOVE_2_MINTS = 1

# Median peak market cap for creators with 1 mint
# Scale: 20000 HIGHRISK, 50000 MEDIUMRISK, 70000 LOWRISK
MEDIAN_PEAK_MC_1_MINT = 1

# Minimum swaps before the highest price is reached
# Scale: 50 HIGHRISK, 100 MEDIUMRISK, 250 LOWRISK
HIGHEST_PRICE_MIN_SWAPS = 1 

# Snipe price to peak price ratio
# Scale: 2.5 HIGHRISK, 3.0 MEDIUMRISK, 3.5 LOWRISK
SNIPE_PRICE_TO_PEAK_PRICE_RATIO = 1.0

# Minimum trust factor to consider a creator successful
# Scale: 0.5 HIGHRISK, 0.6 MEDIUMRISK, 0.8 LOWRISK
TRUST_FACTOR_RATIO = 0.0

# Price closest to X second after the first transaction.
# Example: 1.5 means the price closest to 1.5 seconds after the first transaction.
SNIPING_PRICE_TIME = 0.0

# Refer to set_trust_level method in Dexter.py to understand how trust levels are determined
# Dexter automatically handles USD to SOL conversion, no need to worry about it
AMOUNT_BUY_TL_2 = 0.1 # 10 USD for creators with Trust Level 2
AMOUNT_BUY_TL_1 = 0.05 # 10 USD for creators with Trust Level 1
BUY_FEE = 0.006 # 0.1 USD
SELL_FEE = 0.006 # 0.1 USD
SLIPPAGE_AMOUNT = 1.30 # 90%, 30% is safer
PRICE_STEP_UNITS = 40 # 40% price step

# If you want to just swap on pump.fun until the bonding curve is reached:
PROFIT_MARGIN = Decimal('0.8') # 80% - This will clamp the maximum profit range of the creator to 40% of his median profit range
# Example: If the creator's median profit range is 720%, the maximum profit range for sell will be 576%
# If you want to sell on raydium you must do it manually through your wallet or other services, Dexter is a Pump.fun bot.
# For selling on Raydium set this to 1.0

# Algorithm parameters
# !WARNING DO NOT CHANGE IF YOU DO NOT KNOW WHAT YOU ARE DOING
# Price trend weight and transaction momentum weight affect the composite score of a token
# E.g. if price trend weight is 0.4 and transaction momentum weight is 0.6, the composite score is:
# 0.4 * price trend score + 0.6 * transaction momentum score
# !default values are 0.4 and 0.6
PRICE_TREND_WEIGHT = Decimal('0.4')
TX_MOMENTUM_WEIGHT = Decimal('0.6')

# Session algorithm parameters
# If composite score is above INCREMENT_THRESHOLD, bot will increase current step at half of a jump
# Example: If current step is 40% and composite score is above 25, bot will increase step to 80%
# !default values are 25 and 20
INCREMENT_THRESHOLD = Decimal('25')
INCREMENT_COOLDOWN = 15.0 # Do not increment step for X seconds after incrementing the first time, or we may never sell until max profit range is reached.

# Example: If current step is 80% but there are more sells than buys, bot will sell
# !default value is 20
# This is a heuristic, rarely being triggered
DECREMENT_THRESHOLD = Decimal('20')

# Sell if there was no buy in the last X seconds
DROP_TIME = 30

# If a token is under 0.0000000300 price for X seconds, it is considered stagnant and malicious
STAGNANT_UNDER_PRICE = 13 

LEADERBOARD_UPDATE_INTERVAL = 10 # Update leaderboard every X minutes