import asyncio
import aiohttp
from discord import Webhook, Embed
import datetime

WEBHOOK_URL = 'https://discord.com/api/webhooks/1339999744923140157/GaKfNLiAdzk1IqHR1c_FsnmdznuZ4pPyvx5NFVCPdY6G4R3ZNy2zy4vM07ZOdTSyNEka'

async def send_test_embed():
    async with aiohttp.ClientSession() as session:
        webhook = Webhook.from_url(WEBHOOK_URL, session=session)
        
        embed = Embed(color=0x808080)  # Gray color
        
        # Add fields to match the embed structure
        embed.add_field(name="Type", value="SWAP", inline=True)
        embed.add_field(name="Source", value="RAYDIUM", inline=True)
        embed.add_field(name="Date", value="2025-03-03 21:47:45 +0000 UTC", inline=False)
        embed.add_field(name="Description", 
                       value="6YPC5vKDiFAyG7b2J5NrjqJVnAsjNf1ynVcd92UZeK2S swapped 663245.705671 EKBZDhaSiAmUQNeJbkAkhJTEZPAN8WC5fShnUTyxpump for 0.991 SOL", 
                       inline=False)
        embed.add_field(name="Explorer", 
                       value="https://xray.helius.xyz/tx/5ck8uFBfTcK2keDSY6n64i34UumeLe8EGKPKsCF2We3ARLjyXZ6oYQWvAgvzzUbYJ6UfmpYXqrBwnUrwUcRxmVRL", 
                       inline=False)

        await webhook.send(embed=embed)
        print("Test embed sent!")

if __name__ == "__main__":
    asyncio.run(send_test_embed())