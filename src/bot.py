import os

import discord
from discord import app_commands

from query_db import Database

from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

db_conn = Database()

intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

@tree.command(
    name="query",
    description="enter a valid sql query"
)
async def first_command(interaction: discord.Interaction, message: str):
    resp = db_conn.query(message)
    await interaction.response.send_message(resp)

@client.event
async def on_ready():
    await tree.sync()
    print("Ready!")

client.run(TOKEN)